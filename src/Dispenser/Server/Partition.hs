{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}

module Dispenser.Server.Partition
     ( module Exports
     , PGConnection
     , connectedPartition
     , create
     , drop
     , pool
     , pgConnect
     , currentEventNumber
     , recreate
     , partitionNameToChannelName
     ) where

import           Dispenser.Server.Prelude              hiding ( drop )
import qualified Streaming.Prelude          as S

import qualified Data.List                  as List
import           Data.String                                  ( fromString )
import           Data.Text                                    ( unpack )
import           Database.PostgreSQL.Simple                   ( query_ )
import           Dispenser.Server.Db                          ( poolFromUrl
                                                              , runSQL
                                                              )
import           Dispenser.Server.Orphans                     ()
import           Dispenser.Types            as Exports
import           Streaming

data PGConnection = PGConnection
  { _connectedPartition :: Partition
  , _pool               :: Pool Connection
  } deriving (Generic)

makeClassy ''PGConnection

instance HasPartition PGConnection where
  partition = connectedPartition

-- TODO: `Foldable a` instead of `[a]`?
instance PartitionConnection PGConnection where
  appendEvents :: (EventData a, MonadIO m)
               => PGConnection -> [StreamName] -> NonEmptyBatch a
               -> m (Async EventNumber)
  appendEvents conn streamNames (NonEmptyBatch b) =
    liftIO . async . withResource (conn ^. pool) $ \dbConn ->
      List.last <$> returning dbConn q (fmap f $ toJSON <$> toList b)
    where
      f :: Value -> (Value, [StreamName])
      f v = (v, streamNames)

      q :: Query
      q = fromString . unpack
            $ "INSERT INTO " <> unPartitionName (conn ^. partitionName)
           <> " (event_data, stream_names)"
           <> " VALUES (?, ?)"
           <> " RETURNING event_number"

  fromNow :: (EventData a, MonadIO m)
          => PGConnection -> [StreamName]
          -> m (Stream (Of (Event a)) m r)
  fromNow _conn _streamNames = panic "disco 2"

  rangeStream :: (EventData a, MonadIO m)
              => PGConnection
              -> BatchSize
              -> [StreamName]
              -> (EventNumber, EventNumber)
              -> m (Stream (Of (Event a)) m ())
  rangeStream conn batchSize streamNames (minNum, maxNum) = do
    -- TODO: filter by stream names
    batch :: Batch (Event a) <- liftIO $ wait =<< pgReadBatchFrom minNum batchSize conn
    let events      = unBatch batch
        batchStream = S.each events
    if any ((>= maxNum) . view eventNumber) events
      then return $ S.takeWhile ((<= maxNum) . view eventNumber) batchStream
      else do
        let minNum' = succ . fromMaybe (EventNumber (-1))
              . maximumMay . map (view eventNumber) $ events
        nextStream <- rangeStream conn batchSize streamNames (minNum', maxNum)
        return $ batchStream >>= const nextStream

pgConnect :: Partition -> PoolSize -> IO PGConnection
pgConnect part (PoolSize size) =
  PGConnection part <$> poolFromUrl (part ^. dbUrl) (fromIntegral size)

create :: PGConnection -> IO ()
create conn = withResource (conn ^. pool) $ \dbConn -> do
  -- TODO: tx
  createTable           dbConn
  createStreamTriggerFn dbConn
  createStreamTrigger   dbConn
  createIndexes         dbConn
  where
    table = unPartitionName $ conn ^. partitionName

    createTable :: Connection -> IO ()
    createTable dbConn = runSQL dbConn $
      "CREATE TABLE " <> table
      <> " ( event_number BIGSERIAL PRIMARY KEY"
      <> " , stream_names TEXT[]"
      <> " , event_data   JSONB NOT NULL"
      <> " , created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()"
      <> " )"

    createIndexes :: Connection -> IO ()
    createIndexes _conn = return () -- TODO: createIndexes

    createStreamTriggerFn :: Connection -> IO ()
    createStreamTriggerFn dbConn = runSQL dbConn $
      "CREATE FUNCTION stream_" <> table <> "_events() RETURNS trigger AS $$\n"
      <> "BEGIN\n"
      <> "  PERFORM pg_notify('" <> channel <> "', (row_to_json(NEW) :: TEXT));\n"
      <> "  RETURN NEW;\n"
      <> "END\n"
      <> "$$ LANGUAGE plpgsql"
      where
        channel = partitionNameToChannelName table

    createStreamTrigger :: Connection -> IO ()
    createStreamTrigger dbConn = runSQL dbConn $
      "CREATE TRIGGER " <> triggerName <> " AFTER INSERT ON " <> table
      <> " FOR EACH ROW EXECUTE PROCEDURE stream_" <> table <> "_events()"
      where
        triggerName = table <> "_stream_trig"

currentEventNumber :: PGConnection -> IO EventNumber
currentEventNumber conn = withResource (conn ^. pool) $ \dbConn -> do
  [Only n] <- query_ dbConn q
  return $ EventNumber n
  where
    q = fromString . unpack $ "SELECT COALESCE(MAX(event_number), -1) FROM " <> tn
    PartitionName tn = conn ^. partitionName

drop :: PGConnection -> IO ()
drop partConn = withResource (partConn ^. pool) $ \conn -> do
  runSQL conn $ "DROP TABLE IF EXISTS " <> table
  runSQL conn $ "DROP FUNCTION IF EXISTS stream_" <> table <> "_events() CASCADE"
  where
    table = unPartitionName $ partConn ^. partitionName

recreate :: PGConnection -> IO ()
recreate conn = do
  drop conn
  create conn

partitionNameToChannelName :: Text -> Text
partitionNameToChannelName = (<> "_stream")

-- Right now there is no limit on batch size... so obviously we should uh... do
-- something about that.
pgReadBatchFrom :: EventData a
                => EventNumber -> BatchSize -> PGConnection -> IO (Async (Batch (Event a)))
pgReadBatchFrom (EventNumber n) (BatchSize sz) conn
  | sz <= 0   = async (return $ Batch [])
  | otherwise = async $ withResource (conn ^. pool) $ \dbConn -> do
      batchValue :: Batch (Event Value) <- Batch <$> query dbConn q params
      return $ f batchValue
  where
    f :: EventData a => Batch (Event Value) -> Batch (Event a)
    f = fmap (g . fromJSON <$>)

    g :: Result a -> a
    g = \case
      Error e   -> panic $ "unexpected error deserializing result: " <> show e
      Success x -> x

    q :: Query
    q = fromString . unpack
          $ "SELECT event_number, stream_names, event_data, created_at"
         <> " FROM " <> unPartitionName (conn ^. partitionName)
         <> " WHERE event_number >= ?"
         <> " ORDER BY event_number"
         <> " LIMIT ?"

    params :: (Int, Int)
    params = bimap fromIntegral fromIntegral (n, sz)

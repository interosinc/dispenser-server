{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE InstanceSigs           #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE NoImplicitPrelude      #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TemplateHaskell        #-}

module Dispenser.Server.Partition
     ( module Exports
     , PGConnection
     , connectedPartition
     , create
     , drop
     , pool
     , pgConnect
     , recreate
     , partitionNameToChannelName
     ) where

import           Dispenser.Server.Prelude                           hiding ( drop )
import qualified Streaming.Prelude                       as S

import           Control.Monad.Fail                                        ( fail )
import           Control.Monad.Trans.Resource                              ( allocate )
import           Data.Aeson
import qualified Data.ByteString.Lazy                    as Lazy
import qualified Data.HashMap.Strict                     as HM
import qualified Data.List                               as List
import           Data.String                                               ( fromString )
import           Data.Text                                                 ( pack
                                                                           , unpack
                                                                           )
import           Database.PostgreSQL.Simple                                ( query_ )
import           Database.PostgreSQL.Simple.Notification
import           Dispenser                                                 ( genericFromEventNumber )
import           Dispenser.Server.Db                                       ( poolFromUrl
                                                                           , runSQL
                                                                           )
import           Dispenser.Server.Orphans                                  ()
import           Dispenser.Types                         as Exports
import           Streaming

data PGConnection a = PGConnection
  { _connectedPartition :: Partition
  , _pool               :: Pool Connection
  } deriving (Generic)

makeClassy ''PGConnection

instance HasPartition (PGConnection e) where
  partition = connectedPartition

newtype PushEvent a = PushEvent { unEvent :: Event a }
  deriving (Eq, Ord, Read, Show)

-- TODO: `Foldable a` instead of `[a]`?
instance CanAppendEvents PGConnection a where
  appendEvents :: (EventData e, MonadResource m)
               => PGConnection e -> [StreamName] -> NonEmptyBatch e
               -> m EventNumber
  appendEvents conn streamNames (NonEmptyBatch b) =
    liftIO . withResource (conn ^. pool) $ \dbConn ->
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

instance CanRangeStream PGConnection e where
  rangeStream :: (EventData e, MonadIO m, MonadResource m)
              => PGConnection e -> BatchSize -> [StreamName] -> (EventNumber, EventNumber)
              -> m (Stream (Of (Event e)) m ())
  rangeStream conn batchSize streamNames (minNum, maxNum)
    | maxNum < minNum        = do
        debugRangeStream streamNames (minNum, maxNum) "maxNum < minNum"
        return mempty
    | maxNum < EventNumber 0 = do
        debugRangeStream streamNames (minNum, maxNum) "max EventNumber < 0"
        return mempty
    | otherwise = do
        debugRangeStream streamNames (minNum, maxNum) "otherwise"
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
    where
      debugRangeStream :: MonadIO m
                       => [StreamName] -> (EventNumber, EventNumber) -> String -> m ()
      debugRangeStream streamNames' range msg = debug $ "debugRangeStream: streamNames="
        <> show streamNames'
        <> " "
        <> show range
        <> " -- "
        <> show msg

instance CanFromNow PGConnection e where
  fromNow :: ( EventData e
             , MonadResource m
             )
          => PGConnection e -> BatchSize -> [StreamName]
          -> m (Stream (Of (Event e)) m r)
  fromNow conn batchSize _streamNames = do
    -- TODO: filter by streamNames

    debug $ "Dispenser.Server.Partition: fromNow, batchSize=" <> show batchSize
    -- TODO: This will leak connections if an exception occurs.
    --       conn should be destroyed or returned

    dbConn <- takeConnection conn

    debug $ "Listening for notifications on: " <> show channelText
    void . liftIO . execute_ dbConn . fromString . unpack $ "LISTEN " <> channelText
    debug "pushStream acquired connection"
    return . forever $ do
      debug "pushStream attempting to acquire notification"
      n <- liftIO $ getNotification dbConn
      debug $ "got notification: " <> show n
      when (notificationChannel n == channelBytes) $ do
        debug "notification was for the right channel!"
        case deserializeNotificationEvent . notificationData $ n of
          Left err -> panic $ "pushStream ERROR: " <> show err
          Right e  -> do
            debug $ "yielding event: " <> show e
            S.yield e
    where
      channelBytes = encodeUtf8 channelText
      channelText  = partitionNameToChannelName . unPartitionName
        $ conn ^. partitionName

      deserializeNotificationEvent :: EventData a => ByteString -> Either Text (Event a)
      deserializeNotificationEvent = bimap pack unEvent . eitherDecode . Lazy.fromStrict

instance CanFromEventNumber PGConnection e where
  fromEventNumber = genericFromEventNumber

pgConnect :: Partition -> PoolSize -> IO (PGConnection a)
pgConnect part (PoolSize size) =
  PGConnection part <$> poolFromUrl (part ^. dbUrl) (fromIntegral size)

create :: PGConnection a -> IO ()
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

instance CanCurrentEventNumber PGConnection e where
  currentEventNumber conn = liftIO . withResource (conn ^. pool) $ \dbConn -> do
    [Only n] <- query_ dbConn q
    return $ EventNumber n
    where
      q = fromString . unpack $ "SELECT COALESCE(MAX(event_number), -1) FROM " <> tn
      PartitionName tn = conn ^. partitionName

drop :: PGConnection a -> IO ()
drop partConn = withResource (partConn ^. pool) $ \conn -> do
  runSQL conn $ "DROP TABLE IF EXISTS " <> table
  runSQL conn $ "DROP FUNCTION IF EXISTS stream_" <> table <> "_events() CASCADE"
  where
    table = unPartitionName $ partConn ^. partitionName

recreate :: PGConnection a -> IO ()
recreate conn = do
  drop conn
  create conn

partitionNameToChannelName :: Text -> Text
partitionNameToChannelName = (<> "_stream")

-- Right now there is no limit on batch size... so obviously we should uh... do
-- something about that.
pgReadBatchFrom :: EventData a
                => EventNumber -> BatchSize -> PGConnection a
                -> IO (Async (Batch (Event a)))
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

instance FromJSON a => FromJSON (PushEvent a) where
  parseJSON = withObject "notificationEvent" $ \obj -> do
    n   <- parseField "event_number" obj
    sns <- parseField "stream_names" obj
    val <- parseField "event_data"   obj
    at  <- parseField "created_at"   obj
    return . PushEvent $ Event (EventNumber n) (map StreamName sns) val (Timestamp at)
    where
      parseField s obj = case HM.lookup s obj of
        Just x  -> parseJSON x
        Nothing -> fail $ "no '" <> unpack s <> "' field"

takeConnection :: MonadResource m
               => PGConnection e -> m Connection
takeConnection conn = do
  (dbConn, _localPool) <- takeConnectionLP conn
  return dbConn

takeConnectionLP :: MonadResource m
                 => PGConnection e -> m (Connection, LocalPool Connection)
takeConnectionLP conn = do
  (_key, x) <- allocate take' release'
  return x
  where
    take' :: IO (Connection, LocalPool Connection)
    take' = takeResource (conn ^. pool)

    release' :: (Connection, LocalPool Connection) -> IO ()
    release' (conn', localPool) = putResource localPool conn'

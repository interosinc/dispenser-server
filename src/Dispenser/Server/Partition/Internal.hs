{-# LANGUAGE DeriveGeneric                 #-}
{-# LANGUAGE FlexibleContexts              #-}
{-# LANGUAGE FlexibleInstances             #-}
{-# LANGUAGE FunctionalDependencies        #-}
{-# LANGUAGE InstanceSigs                  #-}
{-# LANGUAGE LambdaCase                    #-}
{-# LANGUAGE MonoLocalBinds                #-}
{-# LANGUAGE MultiParamTypeClasses         #-}
{-# LANGUAGE NoImplicitPrelude             #-}
{-# LANGUAGE OverloadedStrings             #-}
{-# LANGUAGE ScopedTypeVariables           #-}
{-# LANGUAGE TemplateHaskell               #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module Dispenser.Server.Partition.Internal where

import           Dispenser.Server.Prelude                           hiding ( drop
                                                                           , intercalate
                                                                           )
import qualified Streaming.Prelude                       as S

import           Control.Monad.Fail                                        ( fail )
import           Control.Monad.Trans.Resource                              ( allocate )
import           Data.Aeson
import qualified Data.ByteString.Lazy                    as Lazy
import qualified Data.HashMap.Strict                     as HM
import qualified Data.List                               as List
import qualified Data.Set                                as Set
import           Data.String                                               ( String
                                                                           , fromString
                                                                           )
import           Data.Text                                                 ( intercalate
                                                                           , pack
                                                                           , unpack
                                                                           )
import           Database.PostgreSQL.Simple                                ( Connection
                                                                           , query_
                                                                           )
import           Database.PostgreSQL.Simple.Notification
import           Dispenser                                                 ( StreamSource
                                                                           , connect
                                                                           , genericFromEventNumber
                                                                           )
import           Dispenser.Server.Db                                       ( poolFromUrl
                                                                           , runSQL
                                                                           )
import           Dispenser.Server.Orphans                                  ()
import           Dispenser.Types                         as Exports
import           Streaming

data PgClient a = PgClient
  { _pgClientMaxPoolSize :: Word
  , _pgClientUrl         :: Text
  }

data PgConnection a = PgConnection
  { _pgConnectionConnectedPartition :: Partition
  , _pgConnectionPool               :: Pool Connection
  } deriving (Generic)

data WrappedConnection a = WrappedConnection
  { _wrappedConnectionConnectedPartition :: Partition
  , _wrappedConnectionConnection         :: Connection
  }

makeFields ''PgClient
makeFields ''PgConnection

new :: Word -> Text -> IO (PgClient a)
new = (return .) . PgClient

instance (EventData e, MonadResource m) => Client (PgClient e) PgConnection m e where
  connect :: PartitionName -> PgClient e -> m (PgConnection e)
  connect partName client = do
    let part = Partition (DatabaseURL $ client ^. url) partName
    PgConnection part <$> liftIO
      (poolFromUrl (part ^. dbUrl) (fromIntegral $ client ^. maxPoolSize))

_proof :: PartitionConnection PgConnection m e => Proxy (m e)
_proof = Proxy

instance HasPartition (PgConnection e) where
  partition = connectedPartition

newtype PushEvent a = PushEvent { unEvent :: Event a }
  deriving (Eq, Ord, Read, Show)

-- -- TODO: `Foldable a` instead of `[a]`?

instance (EventData a, MonadResource m)
  => CanAppendEvents PgConnection m a where
  appendEvents :: (EventData e, MonadResource m)
               => PgConnection e -> Set StreamName -> NonEmptyBatch e
               -> m EventNumber
  appendEvents conn streamNames batch =
    liftIO . withResource (conn ^. pool) $ \dbConn -> do
      let wrappedConn = WrappedConnection (conn ^. partition) dbConn
      runResourceT $ appendEvents wrappedConn streamNames batch

instance (EventData a, MonadResource m)
  => CanAppendEvents WrappedConnection m a where
  appendEvents :: (EventData e, MonadIO m)
               => WrappedConnection e
               -> Set StreamName
               -> NonEmptyBatch e
               -> m EventNumber
  appendEvents (WrappedConnection part dbConn) streamNames (NonEmptyBatch b) =
    liftIO $ List.last <$> returning dbConn q (fmap f $ toJSON <$> toList b)
    where
      f :: Value -> (Value, Set StreamName)
      f v = (v, streamNames)

      q :: Query
      q = fromString . unpack
            $ "INSERT INTO " <> unPartitionName (part ^. partitionName)
           <> " (event_data, stream_names)"
           <> " VALUES (?, ?)"
           <> " RETURNING event_number"

instance CanRangeStream PgConnection m e where
  rangeStream :: (EventData e, MonadIO m, MonadResource m)
              => PgConnection e
              -> BatchSize
              -> StreamSource
              -> (EventNumber, EventNumber)
              -> m (Stream (Of (Event e)) m ())
  rangeStream conn batchSize source (minNum, maxNum)
    | maxNum < minNum        = do
        debugRangeStream source (minNum, maxNum) "maxNum < minNum"
        return mempty
    | maxNum < EventNumber 0 = do
        debugRangeStream source (minNum, maxNum) "max EventNumber < 0"
        return mempty
    | otherwise = do
        debugRangeStream source (minNum, maxNum) "otherwise"
        batch :: Batch (Event a) <- liftIO $ wait
          =<< pgReadBatchFrom minNum batchSize source conn
        let events      = unBatch batch
            batchStream = S.each events
        if any ((>= maxNum) . view eventNumber) events
            || (length events < (fromIntegral . unBatchSize) batchSize)
            -- TODO: is this right?  I added it to prevent tests hanging that
            --       started when I actually implemented filtering based on
            --       StreamSource but I'm not sure if philosophically
            --       rangeStream should block until the event stream catches up
            --       to the end of the range or not.  maybe it should or maybe
            --       there should be both blocking and non-blocking interfaces.
            --       either way for now I'm taking the coward's way out since
            --       this fixes the tests.

            -- TODO: If nothing else it seems like it could miss an event if it
            --       reads short but new events come in before it switches over
            --       to LISTEN
          then return $ S.takeWhile ((<= maxNum) . view eventNumber) batchStream
          else do
            let minNum' = succ . fromMaybe (EventNumber (-1))
                  . maximumMay . map (view eventNumber) $ events
            nextStream <- rangeStream conn batchSize source (minNum', maxNum)
            return $ batchStream >>= const nextStream
    where
      debugRangeStream :: StreamSource -> (EventNumber, EventNumber) -> String -> m ()
      debugRangeStream source' range msg = debug $ "debugRangeStream: source="
        <> show source'
        <> " "
        <> show range
        <> " -- "
        <> show msg

instance CanFromNow PgConnection m e where
  fromNow :: ( EventData e
             , MonadResource m
             )
          => PgConnection e -> BatchSize -> StreamSource
          -> m (Stream (Of (Event e)) m r)
  fromNow conn batchSize _source = do
    -- TODO: filter by source

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

instance CanFromEventNumber PgConnection m e where
  fromEventNumber = genericFromEventNumber

exists :: PgConnection a -> IO Bool
exists conn = withResource (conn ^. pool) $ \dbConn-> do
  [Only b] <- query_ dbConn . fromString . List.unlines $
                [ "SELECT EXISTS ("
                , "   SELECT 1"
                , "   FROM   information_schema.tables"
                , "   WHERE  table_schema = current_schema()"
                , "   AND    table_name = '" ++ unpack table ++ "'"
                , "   );"
                ]
  return b
  where
    table = unPartitionName $ conn ^. partitionName

ensureExists :: PgConnection a -> IO ()
ensureExists conn = exists conn >>= \b -> unless b $ create conn

create :: PgConnection a -> IO ()
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
      <> " , recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()"
      <> " )"

    createIndexes :: Connection -> IO ()
    createIndexes dbConn = runSQL dbConn $
      "CREATE INDEX " <> idxName <> " ON " <> table <> " USING GIN(stream_names)"
      where
        idxName = table <> "_stream_names_idx"

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

instance CanCurrentEventNumber PgConnection m e where
  currentEventNumber conn = liftIO . withResource (conn ^. pool) $ \dbConn -> do
    [Only n] <- query_ dbConn q
    return $ EventNumber n
    where
      q = fromString . unpack $ "SELECT COALESCE(MAX(event_number), -1) FROM " <> tn
      PartitionName tn = conn ^. partitionName

drop :: PgConnection a -> IO ()
drop partConn = withResource (partConn ^. pool) $ \conn -> do
  runSQL conn $ "DROP TABLE IF EXISTS " <> table
  runSQL conn $ "DROP FUNCTION IF EXISTS stream_" <> table <> "_events() CASCADE"
  where
    table = unPartitionName $ partConn ^. partitionName

recreate :: PgConnection a -> IO ()
recreate conn = do
  drop conn
  create conn

partitionNameToChannelName :: Text -> Text
partitionNameToChannelName = (<> "_stream")

-- Right now there is no limit on batch size... so obviously we should uh... do
-- something about that.
pgReadBatchFrom :: EventData a
                => EventNumber
                -> BatchSize
                -> StreamSource
                -> PgConnection a
                -> IO (Async (Batch (Event a)))
pgReadBatchFrom (EventNumber n) (BatchSize sz) source conn
  | sz <= 0   = async (return $ Batch [])
  | otherwise = async $ withResource (conn ^. pool) $ \dbConn -> do
      -- debug $ "streamSourceClause: " <> show streamSourceClause
      -- debug $ "q: " <> show q
      -- debug $ "params: " <> show params
      batchValue :: Batch (Event Value) <- Batch <$> query dbConn q params
      -- debug $ "result: " <> show batchValue
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
          $ "SELECT event_number, stream_names, event_data, recorded_at"
         <> " FROM " <> unPartitionName (conn ^. partitionName)
         <> " WHERE event_number >= ?"
         <> streamSourceClause
         <> " ORDER BY event_number"
         <> " LIMIT ?"

    params :: (Int, Int)
    params = bimap fromIntegral fromIntegral (n, sz)

    streamSourceClause = maybe "" (" AND stream_names && " <>) (streamArray source)

    streamArray :: StreamSource -> Maybe Text
    streamArray AllStreams       = Nothing
    streamArray (SomeStreams ss) = Just
      . ("'{" <>) . (<> "}'")
      . intercalate ","
      . map (show . unStreamName)
      . toList
      $ ss

instance FromJSON a => FromJSON (PushEvent a) where
  parseJSON = withObject "notificationEvent" $ \obj -> do
    n   <- parseField "event_number" obj
    sns <- parseField "stream_names" obj
    val <- parseField "event_data"   obj
    at  <- parseField "recorded_at"  obj
    return . PushEvent $ Event (EventNumber n) (promote sns) val (Timestamp at)
    where
      promote :: Set Text -> Set StreamName
      promote = Set.fromList . map StreamName . toList

      parseField s obj = case HM.lookup s obj of
        Just x  -> parseJSON x
        Nothing -> fail $ "no '" <> unpack s <> "' field"

takeConnection :: MonadResource m
               => PgConnection e
               -> m Connection
takeConnection conn = do
  (dbConn, _localPool) <- takeConnectionLP conn
  return dbConn

takeConnectionLP :: MonadResource m
                 => PgConnection e
                 -> m (Connection, LocalPool Connection)
takeConnectionLP conn = do
  (_key, x) <- allocate take' release'
  return x
  where
    take' :: IO (Connection, LocalPool Connection)
    take' = takeResource (conn ^. pool)

    release' :: (Connection, LocalPool Connection) -> IO ()
    release' (conn', localPool) = putResource localPool conn'

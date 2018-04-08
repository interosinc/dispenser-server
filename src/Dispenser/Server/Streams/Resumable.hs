{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Dispenser.Server.Streams.Resumable
     ( OffsetName( OffsetName )
     , createOffsetsTable
     , resume
     ) where

import           Dispenser.Server.Prelude
import qualified Streaming.Prelude                as S

import           Data.String                           ( fromString )
import           Data.Text                             ( unlines
                                                       , unpack
                                                       )
import           Dispenser.Server.Partition
import           Dispenser.Server.Streams.Catchup
import           Streaming

newtype OffsetName = OffsetName { unOffsetName :: Text }
  deriving (Eq, Generic, Ord, Read, Show)

data PartitionOffset = PartitionOffset
  { _partition   :: Partition
  , _offsetName  :: OffsetName
  , _eventNumber :: EventNumber
  } deriving (Eq, Generic, Ord, Read, Show)

offsetTableName :: PGConnection -> Text
offsetTableName conn = unPartitionName (conn ^. partitionName) <> "_offsets"

createOffsetsTable :: PGConnection -> IO ()
createOffsetsTable conn = withResource (conn ^. pool) $ \dbConn ->
  void . execute_ dbConn . fromString . unpack . unlines $
    [ "CREATE TABLE " <> offsetTableName conn
    , "  ( offset_name TEXT PRIMARY KEY"
    , "  , event_number BIGINT NOT NULL"
    , "  )"
    ]

resume :: (EventData a, MonadIO m)
       => OffsetName -> BatchSize -> PGConnection -> m (Stream (Of (Event a)) m r)
resume name batchSize conn = do
  eventNum <- fromMaybe (EventNumber 0) <$> retrieveOffset conn name
  catchupStream <- fromEventNumber conn eventNum batchSize
  recordOffsets name conn catchupStream

recordOffsets :: (EventData a, MonadIO m)
              => OffsetName
              -> PGConnection
              -> Stream (Of (Event a)) m r
              -> m (Stream (Of (Event a)) m r)
recordOffsets (OffsetName offsetName') conn stream = do
  -- TODO: resource mgmt
  (dbConn, _) <- liftIO . takeResource $ conn ^. pool
  return $ S.mapM (f dbConn) stream
  where
    f :: (EventData a, MonadIO m) => Connection -> Event a -> m (Event a)
    f dbConn event' = do
      -- TODO: error handling, etc.
      void . liftIO $ execute dbConn q params
      return event'
      where
        q = fromString . unpack . unlines $
              [ "INSERT INTO " <> tname
              , " ( offset_name, event_number ) VALUES (?, ?)"
              , " ON CONFLICT (offset_name) DO "
              , "   UPDATE SET event_number = EXCLUDED.event_number"
              ]

        params = ( offsetName'
                 , event' ^. eventNumber
                 )

        tname = offsetTableName conn

retrieveOffset :: MonadIO m => PGConnection -> OffsetName -> m (Maybe EventNumber)
retrieveOffset conn name = liftIO . withResource (conn ^. pool) $ \dbConn ->
  query dbConn q params >>= \case
    [Only n] -> return $ Just n
    _        -> return Nothing
  where
    q = fromString . unpack . unlines $
          [ "SELECT event_number"
          , " FROM " <> offsetTableName conn
          , " WHERE offset_name = ?"
          ]
    params = Only . unOffsetName $ name

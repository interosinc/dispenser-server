{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Dispenser.Server.Partition
     ( connect
     , create
     , currentEventNumber
     , drop
     , recreate
     , tableNameToChannelName
     ) where

import Dispenser.Server.Prelude   hiding ( drop )

import Data.String                       ( fromString )
import Data.Text                         ( unpack )
import Database.PostgreSQL.Simple        ( query_ )
import Dispenser.Server.Db               ( poolFromUrl
                                         , runSQL
                                         )
import Dispenser.Server.Types

connect :: Partition -> PoolSize -> IO PartitionConnection
connect part (PoolSize size) =
  PartitionConnection part <$> poolFromUrl (part ^. dbUrl) (fromIntegral size)

create :: PartitionConnection -> IO ()
create partConn = withResource (partConn ^. pool) $ \conn -> do
  -- TODO: tx
  createTable conn
  createStreamTriggerFn conn
  createStreamTrigger conn
  createIndexes conn
  where
    table = unTableName $ partConn ^. tableName

    createTable :: Connection -> IO ()
    createTable conn = runSQL conn $
      "CREATE TABLE " <> table
      <> " ( event_number BIGSERIAL PRIMARY KEY"
      <> " , stream_names TEXT[]"
      <> " , event_data   JSONB NOT NULL"
      <> " , created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()"
      <> " )"

    createIndexes :: Connection -> IO ()
    createIndexes _conn = return () -- TODO: createIndexes

    createStreamTriggerFn :: Connection -> IO ()
    createStreamTriggerFn conn = runSQL conn $
      "CREATE FUNCTION stream_" <> table <> "_events() RETURNS trigger AS $$\n"
      <> "BEGIN\n"
      <> "  PERFORM pg_notify('" <> channel <> "', (row_to_json(NEW) :: TEXT));\n"
      <> "  RETURN NEW;\n"
      <> "END\n"
      <> "$$ LANGUAGE plpgsql"
      where
        channel = tableNameToChannelName table

    createStreamTrigger :: Connection -> IO ()
    createStreamTrigger conn = runSQL conn $
      "CREATE TRIGGER " <> triggerName <> " AFTER INSERT ON " <> table
      <> " FOR EACH ROW EXECUTE PROCEDURE stream_" <> table <> "_events()"
      where
        triggerName = table <> "_stream_trig"

currentEventNumber :: PartitionConnection -> IO EventNumber
currentEventNumber conn = withResource (conn ^. pool) $ \dbConn -> do
  [Only n] <- query_ dbConn q
  return $ EventNumber n
  where
    q = fromString . unpack $ "SELECT COALESCE(MAX(event_number), -1) FROM " <> tn
    TableName tn = conn ^. tableName

drop :: PartitionConnection -> IO ()
drop partConn = withResource (partConn ^. pool) $ \conn -> do
  runSQL conn $ "DROP TABLE IF EXISTS " <> table
  runSQL conn $ "DROP FUNCTION IF EXISTS stream_" <> table <> "_events() CASCADE"
  where
    table = unTableName $ partConn ^. tableName

recreate :: PartitionConnection -> IO ()
recreate conn = do
  drop conn
  create conn

tableNameToChannelName :: Text -> Text
tableNameToChannelName = (<> "_stream")

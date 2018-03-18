{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Dispenser.Server.Partition
     ( pgConnect
     , create
     , currentEventNumber
     , drop
     , recreate
     , partitionNameToChannelName
     ) where

import Dispenser.Server.Prelude   hiding ( drop )

import Data.String                       ( fromString )
import Data.Text                         ( unpack )
import Database.PostgreSQL.Simple        ( query_ )
import Dispenser.Server.Db               ( poolFromUrl
                                         , runSQL
                                         )
import Dispenser.Server.Types

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

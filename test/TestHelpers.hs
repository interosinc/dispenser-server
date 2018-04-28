{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE ScopedTypeVariables #-}

module TestHelpers where

import Dispenser.Server.Prelude

import Data.String                            ( fromString )
import Data.Text                              ( unpack )
import Database.PostgreSQL.Simple       as PG
import Database.PostgreSQL.Simple.SqlQQ
import Database.PostgreSQL.Simple.URL
import Dispenser.Server.Partition
import Streaming
import System.Random

newtype TestInt = TestInt Int
  deriving (Eq, Generic, Ord, Read, Show)

instance FromJSON  TestInt
instance ToJSON    TestInt
instance EventData TestInt

createTestPartition :: IO (PGConnection TestInt)
createTestPartition = do
  pname <- ("test_disp_" <>) . show <$> randomRIO (0, maxBound :: Int)
  conn <- pgConnect (Partition testDbUrl (PartitionName pname)) (PoolSize 5)
  recreate conn
  return conn

testDbUrl :: DatabaseURL
testDbUrl = DatabaseURL "postgres://dispenser@localhost:5432/dispenser"

deleteAllTestPartitions :: IO ()
deleteAllTestPartitions = case parseDatabaseUrl . unpack $ url of
  Nothing -> panic $ "invalid database URL: " <> show url
  Just connectInfo -> do
    conn <- PG.connect connectInfo
    tableNames :: [Text] <- map fromOnly <$> query_ conn q
    mapM_ (deleteTable conn) tableNames
    putLn $ "Removed " <> show (length tableNames) <> " partitions."
  where
    deleteTable :: Connection -> Text -> IO ()
    deleteTable conn tableName = do
      putLn $ "Deleting: " <> tableName
      void . execute_ conn . fromString . unpack $ "DROP TABLE " <> tableName

    DatabaseURL url = testDbUrl
    q = [sql| SELECT tablename
              FROM pg_tables
              WHERE tableowner = 'dispenser'
              AND tablename
              LIKE 'test_disp_%'
            |]

postTestEvent :: PGConnection TestInt -> Int -> IO ()
postTestEvent conn = (void . wait =<<)
  . runResourceT
  . postEvent conn [StreamName "test"]
  . TestInt

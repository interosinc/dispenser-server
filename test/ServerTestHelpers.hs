{-# LANGUAGE DeriveDataTypeable    #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude     #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE QuasiQuotes           #-}
{-# LANGUAGE ScopedTypeVariables   #-}

module ServerTestHelpers where

import Dispenser.Server.Prelude

import Data.Set                               ( fromList )
import Data.String                            ( fromString )
import Data.Text                              ( unpack )
import Database.PostgreSQL.Simple       as PG
import Database.PostgreSQL.Simple.SqlQQ
import Database.PostgreSQL.Simple.URL
import Dispenser                        as D
import Dispenser.Server.Partition
import System.Random

newtype TestInt = TestInt Int
  deriving (Data, Eq, Generic, Ord, Read, Show)

instance FromJSON  TestInt
instance ToJSON    TestInt
instance EventData TestInt

instance PartitionConnection PgConnection TestInt where

createTestPartition :: IO (PgConnection TestInt)
createTestPartition = do
  pname <- ("test_disp_" <>) . show <$> randomRIO (0, maxBound :: Int)
  client :: PgClient TestInt <- new poolMax url'
  conn <- D.connect (PartitionName pname) client
  recreate conn
  return conn
  where
    DatabaseURL url' = testDbUrl
    poolMax          = 5

testDbUrl :: DatabaseURL
testDbUrl = DatabaseURL "postgres://dispenser@localhost:5432/dispenser"

deleteAllTestPartitions :: IO ()
deleteAllTestPartitions = case parseDatabaseUrl . unpack $ url' of
  Nothing -> panic $ "invalid database URL: " <> show url'
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

    DatabaseURL url' = testDbUrl
    q = [sql| SELECT tablename
              FROM pg_tables
              WHERE tableowner = 'dispenser'
              AND tablename
              LIKE 'test_disp_%'
            |]

postTestEvent :: PgConnection TestInt -> Int -> IO ()
postTestEvent conn = void
  . runResourceT
  . postEvent conn (fromList [StreamName "test"])
  . TestInt

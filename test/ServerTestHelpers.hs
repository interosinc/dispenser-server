{-# LANGUAGE DeriveDataTypeable            #-}
{-# LANGUAGE DeriveGeneric                 #-}
{-# LANGUAGE FlexibleContexts              #-}
{-# LANGUAGE MonoLocalBinds                #-}
{-# LANGUAGE MultiParamTypeClasses         #-}
{-# LANGUAGE NoImplicitPrelude             #-}
{-# LANGUAGE OverloadedStrings             #-}
{-# LANGUAGE QuasiQuotes                   #-}
{-# LANGUAGE ScopedTypeVariables           #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

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

-- instance PartitionConnection PgConnection TestInt where

_proof :: PartitionConnection PgConnection m TestInt => Proxy (m TestInt)
_proof = Proxy

createTestPartition :: MonadIO m => m (PgConnection TestInt)
createTestPartition = liftIO $ do
  pname <- ("test_disp_" <>) . show <$> randomRIO (0, maxBound :: Int)
  client :: PgClient TestInt <- new poolMax url'
  conn <- liftIO . runResourceT $ D.connect (PartitionName pname) client
  recreate conn
  return conn
  where
    DatabaseURL url' = testDbUrl
    poolMax          = 5

testDbUrl :: DatabaseURL
testDbUrl = DatabaseURL "postgres://dispenser:dispenser@localhost:5432/dispenser"

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

postTestEvent :: MonadIO m => PgConnection TestInt -> Int -> m ()
postTestEvent conn = void
  . liftIO
  . runResourceT
  . postEvent conn (fromList [StreamName "test"])
  . TestInt

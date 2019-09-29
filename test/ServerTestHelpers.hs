{-# LANGUAGE DeriveDataTypeable            #-}
{-# LANGUAGE DeriveGeneric                 #-}
{-# LANGUAGE FlexibleContexts              #-}
{-# LANGUAGE NoImplicitPrelude             #-}
{-# LANGUAGE OverloadedStrings             #-}
{-# LANGUAGE QuasiQuotes                   #-}
{-# LANGUAGE ScopedTypeVariables           #-}
{-# LANGUAGE TypeFamilies                  #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module ServerTestHelpers where

import Dispenser.Server.Prelude

import Data.Set                         as Set
import Data.Set                                ( fromList )
import Data.String                             ( fromString )
import Data.Text                               ( unpack )
import Database.PostgreSQL.Simple       as PG
import Database.PostgreSQL.Simple.SqlQQ        ( sql )
import Database.PostgreSQL.Simple.URL          ( parseDatabaseUrl )
import Dispenser                        as D
import Dispenser.ResourceTOrphans              ()
import Dispenser.Server.Partition              ( PgClient
                                               , PgConnection
                                               , new
                                               , recreate
                                               )
import System.Random                           ( randomRIO )

newtype TestInt = TestInt Int
  deriving (Eq, Generic, Ord, Read, Show)

instance FromJSON TestInt
instance ToJSON   TestInt

_proof :: PartitionConnection PgConnection m TestInt => Proxy (m TestInt)
_proof = Proxy

createTestPartition :: MonadIO m => m (PgConnection TestInt)
createTestPartition = liftIO $ do
  pname <- ("test_disp_" <>) . show <$> randomRIO (0, maxBound :: Int)
  let _ = pname :: Text
  client :: PgClient TestInt <- new poolMax url'
  conn <- runResourceT $ D.connect (PartitionName pname) client
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
    tableNames :: [Text] <- fromOnly <<$>> query_ conn q
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

testStreamSource :: StreamSource
testStreamSource = SomeStreams . Set.fromList . return . StreamName $ "test"

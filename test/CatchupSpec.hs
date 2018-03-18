{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module CatchupSpec where

import           Dispenser.Server.Prelude
import qualified Streaming.Prelude         as S

import           Dispenser.Db
import           Dispenser.Server.Types
import           Dispenser.Streams.Catchup
import           Streaming
import           System.Random
import           Test.Hspec

main :: IO ()
main = hspec spec

data TestEvent = TestEvent Int
  deriving (Eq, Generic, Ord, Read, Show)

instance FromJSON TestEvent
instance ToJSON   TestEvent

instance EventData TestEvent

type TestStream a = Stream (Of (Event TestEvent)) IO a

spec :: Spec
spec = describe "Catchup" $ do
  it "should not drop any events" $ do
    conn <- createTestPartition
    s1 :: TestStream () <- S.take 5 <$> fromEventNumber (EventNumber 0) conn
    postTestEvent 1 conn
    postTestEvent 2 conn
    postTestEvent 3 conn
    Right (e1, s1')   <- S.next s1
    Right (e2, s1'')  <- S.next s1'
    Right (e3, s1''') <- S.next s1''
    let front1 :: [Event TestEvent] = [e1, e2, e3]
    back1 :: [Event TestEvent] <- S.fst' <$> S.toList s1'''
    let all1 = front1 ++ back1
    all1 `shouldBe` []
    s2 :: TestStream () <- S.take 5 <$> fromEventNumber (EventNumber 0) conn
    postTestEvent 4 conn
    postTestEvent 5 conn
    postTestEvent 6 conn
    s3 :: TestStream () <- S.take 5 <$> fromEventNumber (EventNumber 0) conn

    (1 :: Int) `shouldBe` 2


postTestEvent :: Int -> PartitionConnection -> IO ()
postTestEvent = panic "postTestEvent not impl"

createTestPartition :: IO PartitionConnection
createTestPartition = do
  partitionName :: Text <- ("test_disp_" <>) . show <$> randomRIO (0, maxBound :: Int)
  let partition = Partition dbUrl tableName
      tableName = TableName partitionName
  conn <- connectPartition partition (PoolSize 5)
  restartPartition conn
  panic "createTestPartition not impl"
  where
    dbUrl = DatabaseURL "postgres://dispenser@localhost:5432/dispenser"

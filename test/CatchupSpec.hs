{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module CatchupSpec where

import           Dispenser.Server.Prelude
import qualified Streaming.Prelude                as S

import           Dispenser.Server.Partition
import           Dispenser.Server.Streams.Catchup
import           Streaming
import           System.Random
import           Test.Hspec

main :: IO ()
main = hspec spec

newtype TestEvent = TestEvent Int
  deriving (Eq, Generic, Ord, Read, Show)

instance FromJSON TestEvent
instance ToJSON   TestEvent

instance EventData TestEvent

type TestStream a = Stream (Of (Event TestEvent)) IO a

spec :: Spec
spec = describe "Catchup" $ do
  let batchSize = 100
  it "should not drop any events" $ do
    conn <- createTestPartition
    s :: TestStream () <- fromEventNumber conn (EventNumber 0) batchSize
    let s1 :: TestStream () = S.take 3 s
    postTestEvent conn 1
    postTestEvent conn 2
    postTestEvent conn 3
    Right (e1, s1')   <- S.next s1
    Right (e2, s1'')  <- S.next s1'
    Right (e3, s1''') <- S.next s1''
    let front1 :: [Event TestEvent] = [e1, e2, e3]
    back1 :: [Event TestEvent] <- S.fst' <$> S.toList s1'''
    let both = front1 ++ back1
    map (unEventNumber . view eventNumber) both `shouldBe` [1..3]
    map (view eventStreams) both `shouldBe`
      [[StreamName "test"], [StreamName "test"], [StreamName "test"]]
    map (view eventData) both `shouldBe` map TestEvent [1..3]

    s2 :: TestStream () <- S.take 5 <$> fromEventNumber conn (EventNumber 0) batchSize
    postTestEvent conn 4
    postTestEvent conn 5
    postTestEvent conn 6

    events2 :: [Event TestEvent] <- S.fst' <$> S.toList s2
    map (unEventNumber . view eventNumber) events2 `shouldBe` [1..5]
    map (unEventNumber . view eventNumber) events2 `shouldBe` [1..5]

    s3 :: TestStream () <- S.take 6 <$> fromEventNumber conn (EventNumber 0) batchSize
    events3 <- S.fst' <$> S.toList s3
    map (unEventNumber . view eventNumber) events3 `shouldBe` [1..6]

postTestEvent :: PGConnection -> Int -> IO ()
postTestEvent conn = void . postEvent conn [StreamName "test"] . TestInt

newtype TestInt = TestInt Int
  deriving (Eq, Generic, Ord, Read, Show)

instance FromJSON  TestInt
instance ToJSON    TestInt
instance EventData TestInt

createTestPartition :: IO PGConnection
createTestPartition = do
  pname :: Text <- ("test_disp_" <>) . show <$> randomRIO (0, maxBound :: Int)
  conn <- pgConnect (Partition dbUrl' (PartitionName pname)) (PoolSize 5)
  recreate conn
  return conn
  where
    dbUrl' = DatabaseURL "postgres://dispenser@localhost:5432/dispenser"

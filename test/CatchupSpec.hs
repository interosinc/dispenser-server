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
    s1 :: TestStream () <- S.take 5 <$> fromEventNumber conn (EventNumber 0) batchSize
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
    _s2 :: TestStream () <- S.take 5 <$> fromEventNumber conn (EventNumber 0) batchSize
    postTestEvent 4 conn
    postTestEvent 5 conn
    postTestEvent 6 conn
    _s3 :: TestStream () <- S.take 5 <$> fromEventNumber conn (EventNumber 0) batchSize
    (1 :: Int) `shouldBe` 2

postTestEvent :: Int -> PGConnection -> IO ()
postTestEvent = panic "postTestEvent not impl"

createTestPartition :: IO PGConnection
createTestPartition = do
  pname :: Text <- ("test_disp_" <>) . show <$> randomRIO (0, maxBound :: Int)
  conn <- pgConnect (Partition dbUrl' (PartitionName pname)) (PoolSize 5)
  recreate conn
  panic "createTestPartition not impl"
  where
    dbUrl' = DatabaseURL "postgres://dispenser@localhost:5432/dispenser"

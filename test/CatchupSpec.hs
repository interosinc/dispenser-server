{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module CatchupSpec where

import           Dispenser.Server.Prelude
import qualified Streaming.Prelude                as S

import           Dispenser.Server.Partition
import           Dispenser.Server.Streams.Catchup
import           Test.Hspec
import           TestHelpers

main :: IO ()
main = hspec spec

spec :: Spec
spec = describe "Catchup" $ do
  let batchSize = 100
  it "should not drop any events" $ do
    conn <- createTestPartition
    putLn "a"
    s :: TestStream () <- fromEventNumber conn (EventNumber 0) batchSize
    let s1 :: TestStream () = S.take 3 s
    putLn "b"
    postTestEvent conn 1
    putLn "c"
    postTestEvent conn 2
    putLn "d"
    postTestEvent conn 3
    putLn "e"
    Right (e1, s1')   <- S.next s1
    putLn "f"
    Right (e2, s1'')  <- S.next s1'
    putLn "g"
    Right (e3, s1''') <- S.next s1''
    putLn "h"
    let front1 :: [Event TestEvent] = [e1, e2, e3]
    putLn "i"
    back1 :: [Event TestEvent] <- S.fst' <$> S.toList s1'''
    putLn "j"
    let both = front1 ++ back1
    putLn "k"
    map (unEventNumber . view eventNumber) both `shouldBe` [1..3]
    putLn "l"
    map (view eventStreams) both `shouldBe`
      [[StreamName "test"], [StreamName "test"], [StreamName "test"]]
    putLn "m"
    map (view eventData) both `shouldBe` map TestEvent [1..3]
    putLn "n"

    s2 :: TestStream () <- S.take 5 <$> fromEventNumber conn (EventNumber 0) batchSize
    putLn "o"
    postTestEvent conn 4
    putLn "p"
    postTestEvent conn 5
    putLn "q"
    postTestEvent conn 6
    putLn "r"

    events2 :: [Event TestEvent] <- S.fst' <$> S.toList s2
    putLn "s"
    map (unEventNumber . view eventNumber) events2 `shouldBe` [1..5]
    putLn "t"
    map (unEventNumber . view eventNumber) events2 `shouldBe` [1..5]
    putLn "u"

    s3 :: TestStream () <- S.take 6 <$> fromEventNumber conn (EventNumber 0) batchSize
    putLn "v"
    events3 <- S.fst' <$> S.toList s3
    putLn "w"
    map (unEventNumber . view eventNumber) events3 `shouldBe` [1..6]
    putLn "x"

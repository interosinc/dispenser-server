{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module EventStreamSpec
     ( main
     , spec
     ) where

import           Dispenser.Prelude
import qualified Streaming.Prelude              as S

import           Control.Monad.Trans.Resource
import           Dispenser.Server.Streams.Event
import           Dispenser.Types
import           Test.Hspec
import           TestHelpers

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  currentStreamFromSpec

currentStreamFromSpec :: Spec
currentStreamFromSpec = describe "currentStreamFrom" $ do
  context "given an empty partition" $ do
    it "should return ???" $ do
      conn <- createTestPartition
      stream <- runResourceT $ currentStreamFrom conn (EventNumber 0) (BatchSize 100) []
      xs :: [Event TestInt] <- runResourceT $ S.fst' <$> S.toList stream
      xs `shouldBe` []
  context "given a partition with events" $ do
    it "should return a stream of those events" $ do
      conn <- createTestPartition
      postTestEvent conn 1
      postTestEvent conn 2
      postTestEvent conn 3
      sleep 0.25
      stream <- runResourceT $ currentStreamFrom conn (EventNumber 0) (BatchSize 100) []
      xs :: [Event TestInt] <- runResourceT $ S.fst' <$> S.toList stream
      map (view eventData) xs `shouldBe` map TestInt [1..3]

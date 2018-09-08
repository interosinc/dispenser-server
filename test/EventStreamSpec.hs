{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module EventStreamSpec
     ( main
     , spec
     ) where

import           Dispenser.Prelude
import qualified Streaming.Prelude as S

import           Dispenser
import           ServerTestHelpers
import           Test.Hspec

main :: IO ()
main = hspec spec

spec :: Spec
spec = currentStreamFromSpec

currentStreamFromSpec :: Spec
currentStreamFromSpec = describe "currentStreamFrom" $ do
  let batchSize = BatchSize 100
      source    = singletonSource $ StreamName "test"

  context "given an empty partition" $
    it "should return ???" $ do
      conn <- createTestPartition
      stream <- runResourceT $ currentStreamFrom conn batchSize source (EventNumber 0)
      xs :: [Event TestInt] <- runResourceT $ S.fst' <$> S.toList stream
      xs `shouldBe` []

  context "given a partition with events" $
    it "should return a stream of those events" $ do
      conn <- createTestPartition
      postTestEvent conn 1
      postTestEvent conn 2
      postTestEvent conn 3
      sleep 0.25
      stream <- runResourceT $ currentStreamFrom conn batchSize source (EventNumber 0)
      xs :: [Event TestInt] <- runResourceT $ S.fst' <$> S.toList stream
      fmap (view eventData) xs `shouldBe` fmap TestInt [1..3]

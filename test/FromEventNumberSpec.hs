{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

module FromEventNumberSpec where

import           Dispenser.Server.Prelude
import qualified Streaming.Prelude          as S

import           Data.Set                   as Set
import           Dispenser
import           Dispenser.Server.Partition
import           ServerTestHelpers
import           Streaming
import           Test.Hspec

main :: IO ()
main = hspec spec

spec :: Spec
spec = describe "FromEventNumber" $ do

  context "isolating known issues" $
    it "should work for numEvents=4, batchSize=2, take=numEvents" $ do
      let numEvents  = 4
          batchSize  = BatchSize 2
          testStream = makeTestStream batchSize numEvents
      stream <- S.take numEvents . snd <$> runResourceT testStream
      xs <- runResourceT (S.fst' <$> S.toList stream)
      fmap (view eventData) xs `shouldBe` fmap TestInt [1..numEvents]

  forM_ (fmap BatchSize [1..10]) $ \batchSize ->
    context ("with a BatchSize of " <> show (unBatchSize batchSize)) $ do

      context "given a stream with 3 events in it" $ do
        let testStream = makeTestStream batchSize 3

        it "should be able to take the first 2 immediately" $ do
          stream <- S.take 2 . snd <$> runResourceT testStream
          xs <- runResourceT (S.fst' <$> S.toList stream)
          fmap (view eventData) xs `shouldBe` fmap TestInt [1..2]

        it "should be able to take 5 if two more are posted asynchronously" $ do
          (conn, stream) <- runResourceT testStream
          void . forkIO $ do
            sleep 0.05
            postTestEvent conn 4
            sleep 0.05
            postTestEvent conn 5
            sleep 0.05
            postTestEvent conn 6
          let stream' = S.take 5 stream
          xs <- runResourceT (S.fst' <$> S.toList stream')
          fmap (view eventData) xs `shouldBe` fmap TestInt [1..5]

      context "given a stream with 20 events in it" $ do
        let testStream = makeTestStream batchSize 20

        it "should be able to take all 20" $ do
          stream <- S.take 20 . snd <$> runResourceT testStream
          xs <- runResourceT (S.fst' <$> S.toList stream)
          fmap (view eventData) xs `shouldBe` fmap TestInt [1..20]

        it "should be able to take 25 if 5 are posted asynchronously" $ do
          (conn, stream) <- runResourceT testStream
          void . forkIO $ mapM_ ((sleep 0.05 >>) . postTestEvent conn) [21..25
                                                                                      ]
          let stream' = S.take 25 stream
          xs <- runResourceT (S.fst' <$> S.toList stream')
          fmap (view eventData) xs `shouldBe` fmap TestInt [1..25]

makeTestStream :: ( MonadIO m
                  , MonadResource m
                  , MonadBaseControl IO m
                  )
               => BatchSize -> Int
               -> m (PgConnection TestInt, Stream (Of (Event TestInt)) m r)
makeTestStream batchSize n = do
  conn <- createTestPartition
  mapM_ (postTestEvent conn) [1..n]
  (conn,) <$> fromOne conn batchSize testStreamSource

testStreamSource :: StreamSource
testStreamSource = SomeStreams . Set.fromList . return . StreamName $ "test"

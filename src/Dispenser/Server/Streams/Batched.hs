{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dispenser.Server.Streams.Batched
  ( rangeStream
  ) where

import           Dispenser.Server.Prelude
import qualified Streaming.Prelude           as S

import           Dispenser.Server.Primitives
import           Dispenser.Server.Types
import           Streaming

-- NB. semantics are that rangeStream will give you a stream from minNum to
-- maxNum if they all already exist.  if they don't you get whatever portion
-- does, including potentially just an empty stream if none of the range exists
-- yet.
rangeStream :: forall m a. (EventData a, MonadIO m)
            => (EventNumber, EventNumber) -> BatchSize -> PGConnection
            -> m (Stream (Of (Event a)) m ())
rangeStream (minNum, maxNum) batchSize conn = do
  batch :: Batch (Event a) <- liftIO $ wait =<< pgReadBatchFrom minNum batchSize conn
  let events      = unBatch batch
      batchStream = S.each events
  if any ((>= maxNum) . view eventNumber) events
    then return $ S.takeWhile ((<= maxNum) . view eventNumber) batchStream
    else do
      let minNum' = succ . maximum . map (view eventNumber) $ events
      nextStream <- rangeStream (minNum', maxNum) batchSize conn
      return $ batchStream >>= const nextStream

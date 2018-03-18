{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dispenser.Server.Streams.Catchup
     ( fromEventNumber
     , fromZero
     ) where

import           Dispenser.Server.Prelude
import qualified Streaming.Prelude                as S

import           Dispenser.Server.Partition
import           Dispenser.Server.Streams.Batched
import           Dispenser.Server.Streams.Event
import           Dispenser.Server.Streams.Push
import           Dispenser.Server.Types
import           Streaming

fromEventNumber :: forall m a r. (EventData a, MonadIO m)
                => EventNumber -> BatchSize -> PGConnection
                -> m (Stream (Of (Event a)) m r)
fromEventNumber eventNum batchSize conn = do
  clstream <- S.store S.last <$> currentStreamFrom eventNum batchSize conn
  return $ clstream >>= \case
    Nothing :> _ -> catchup (EventNumber 0)
    Just lastEvent :> _ -> do
      let lastEventNum = lastEvent ^. eventNumber
          nextEventNum = succ lastEventNum
      currentEventNum <- liftIO $ currentEventNumber conn
      if eventNumberDelta currentEventNum lastEventNum > maxHandOffDelta
        then join . lift $ fromEventNumber nextEventNum batchSize conn
        else catchup nextEventNum
    where
      maxHandOffDelta = 50 -- TODO

      catchup en = join . lift $ fromNow conn >>= chaseFrom en

      chaseFrom startNum stream = S.next stream >>= \case
        Left _ -> return stream
        Right (pivotEvent, stream') -> do
          missingStream <- rangeStream (startNum, endNum) batchSize conn
          return $ missingStream >>= const (S.yield pivotEvent) >>= const stream'
          where
            endNum = pred $ pivotEvent ^. eventNumber

fromZero :: (EventData a, MonadIO m)
         => BatchSize -> PGConnection
         -> m (Stream (Of (Event a)) m r)
fromZero = fromEventNumber (EventNumber 0)

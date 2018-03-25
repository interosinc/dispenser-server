{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dispenser.Server.Streams.Event
     ( currentStream
     , currentStreamFrom
     ) where

import Dispenser.Server.Prelude

import Dispenser.Server.Partition       ( PGConnection, currentEventNumber )
import Dispenser.Server.Types
import Streaming

currentStream :: forall m a. (EventData a, MonadIO m)
              => BatchSize -> [StreamName]
              -> PGConnection -> m (Stream (Of (Event a)) m ())
currentStream = currentStreamFrom (EventNumber 0)

currentStreamFrom :: forall m a. (EventData a, MonadIO m)
                  => EventNumber -> BatchSize -> [StreamName] -> PGConnection
                  -> m (Stream (Of (Event a)) m ())
currentStreamFrom minEvent batchSize streamNames conn = do
  endNum <- liftIO $ currentEventNumber conn
  rangeStream batchSize streamNames (minEvent, endNum) conn

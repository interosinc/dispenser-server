{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dispenser.Server.Streams.Event
     ( currentStream
     , currentStreamFrom
     ) where

import Dispenser.Server.Prelude

import Dispenser.Server.Partition       ( currentEventNumber )
import Dispenser.Server.Streams.Batched ( rangeStream )
import Dispenser.Server.Types
import Streaming

currentStream :: forall m a. (EventData a, MonadIO m)
              => BatchSize -> PartitionConnection -> m (Stream (Of (Event a)) m ())
currentStream = currentStreamFrom (EventNumber 0)

currentStreamFrom :: forall m a. (EventData a, MonadIO m)
                  => EventNumber -> BatchSize -> PartitionConnection
                  -> m (Stream (Of (Event a)) m ())
currentStreamFrom minEvent batchSize conn = do
  endNum <- liftIO (currentEventNumber conn)
  rangeStream (minEvent, endNum) batchSize conn

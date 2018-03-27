{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dispenser.Server.Streams.Catchup
     ( fromEventNumber
     , fromZero
     ) where

import           Dispenser.Server.Prelude

import           Dispenser.Server.Partition
import           Dispenser.Server.Streams.Event
import           Streaming
import qualified Dispenser.Catchup as Catchup

fromEventNumber :: forall m a r. (EventData a, MonadIO m)
                => PGConnection -> EventNumber -> BatchSize
                -> m (Stream (Of (Event a)) m r)
fromEventNumber conn = Catchup.make $ Catchup.Config
  (currentEventNumber conn)
  (currentStreamFrom conn)
  (fromEventNumber conn)
  (fromNow conn)
  (rangeStream conn)

-- TODO: make this generic over some class that fromEventNumber is in
fromZero :: (EventData a, MonadIO m)
         => PGConnection -> BatchSize
         -> m (Stream (Of (Event a)) m r)
fromZero conn = fromEventNumber conn (EventNumber 0)

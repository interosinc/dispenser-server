{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dispenser.Server.Streams.Event
     ( currentStream
     , currentStreamFrom
     ) where

import Dispenser.Server.Prelude

import Dispenser.Server.Partition ( PGConnection
                                  , currentEventNumber
                                  )
import Dispenser.Server.Types
import Streaming

currentStream :: forall m a. (EventData a, MonadIO m)
              => PGConnection -> BatchSize -> [StreamName]
              -> m (Stream (Of (Event a)) m ())
currentStream conn = currentStreamFrom conn (EventNumber 0)

currentStreamFrom :: forall m a. (EventData a, MonadIO m)
                  => PGConnection -> EventNumber -> BatchSize -> [StreamName]
                  -> m (Stream (Of (Event a)) m ())
currentStreamFrom conn minEvent batchSize streamNames = do
  putLn $ "currentStreamFrom " <> show minEvent <> ", streamNames=" <> show streamNames
  endNum <- liftIO $ currentEventNumber conn
  putLn $ "endNum=" <> show endNum
  rangeStream conn batchSize streamNames (minEvent, endNum)

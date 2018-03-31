{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE NoImplicitPrelude      #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TemplateHaskell        #-}

module Dispenser.Server.Aggregates
     ( Aggregate
     , currentSnapshot
     , findOrCreate
     -- TEMPORARY:
     , aggregateId
     , state
     , stream
     --
     , aggregateAggregateId
     , aggregateStream
     , snapshotEventNumber
     , snapshotState
     ) where

import Dispenser.Server.Prelude         hiding ( state )

import Dispenser.Server.Partition       hiding ( eventNumber )
import Dispenser.Server.Streams.Catchup
import Streaming

data Aggregate a x b r = Aggregate
  { aggregateAggregateId :: AggregateId
  , aggregateStream      :: Stream (Of (Event a)) IO r
  }

newtype AggregateId = AggregateId Text
  deriving (Eq, Ord, Read, Show)

data AggregateError = AggregateError
  deriving (Eq, Ord, Read, Show)

data Snapshot x = Snapshot
  { snapshotEventNumber :: EventNumber
  , snapshotState       :: x
  } deriving (Eq, Ord, Read, Show)

makeFields ''Aggregate
makeFields ''Snapshot

findOrCreate :: (EventData a, MonadIO m)
             => PGConnection -> AggregateId -> m (Aggregate a x b r)
findOrCreate conn id = liftIO $ Aggregate id <$> (latestSnapshot id >>= \case
  Just snapshot -> fromEventNumber conn (succ $ snapshot ^. eventNumber) batchSize
  Nothing       -> fromZero conn batchSize
                                                 )
  where
    batchSize = BatchSize 100 -- TODO

latestSnapshot :: AggregateId -> IO (Maybe (Snapshot x))
latestSnapshot _ = return Nothing -- TODO

currentSnapshot :: Aggregate a x b r -> IO b
currentSnapshot _ = undefined


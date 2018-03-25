{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude     #-}
{-# LANGUAGE ScopedTypeVariables   #-}

module Dispenser.Server.Aggregates.Partition
     ( PartitionAggregateSource
     , connectAgg
     ) where

import Dispenser.Server.Prelude

import Control.Concurrent.STM.TVar
import Dispenser.Server.Aggregates
import Dispenser.Server.Partition

newtype PartitionAggregateSource = PartitionAggregateSource PGConnection
newtype PartitionAggregate a = PartitionAggregate (TVar a)

instance MonadIO m => Aggregate m (PartitionAggregate a) a where
  currentValue (PartitionAggregate v) = liftIO . atomically $ readTVar v

instance MonadIO m =>
         AggregateSource m PartitionAggregateSource (PartitionAggregate a) e a
      where
  aggregate :: FoldM m e a
            -> PartitionAggregateSource
            -> m (Either AggregateError (PartitionAggregate a))
  aggregate _f@(FoldM _fm zm exm) (PartitionAggregateSource _conn) = do
    z <- zm
    exd <- exm z
    v <- liftIO . atomically . newTVar $ exd
    let agg :: PartitionAggregate a
        agg = PartitionAggregate v
    return $ Right agg

connectAgg :: PGConnection -> IO PartitionAggregateSource
connectAgg = return . PartitionAggregateSource

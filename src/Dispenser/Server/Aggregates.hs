{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude     #-}

module Dispenser.Server.Aggregates
       ( Aggregate
       , AggregateId( AggregateId )
       , AggregateError( AggregateError )
       , AggregateSource
       , aggregate
       , currentValue
       ) where

import Dispenser.Server.Prelude


-- class AggregateSource a where
--   aggregate :: a -> Either AggregateError (Aggregate b)

newtype AggregateId = AggregateId Text
  deriving (Eq, Ord, Read, Show)

class AggregateSource m src agg e a where
  aggregate :: Aggregate m agg a =>
               FoldM m e a -> src -> m (Either AggregateError agg)

class Monad m => Aggregate m agg a where
  currentValue :: agg -> m a

data AggregateError = AggregateError
  deriving (Eq, Ord, Read, Show)

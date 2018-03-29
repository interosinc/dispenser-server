{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dispenser.Server.StatefulAggregate where

import Dispenser.Prelude           hiding ( state )

import Control.Concurrent.STM.TVar
import Dispenser.Types
import Streaming

data AggregateConfig m a x b s = AggregateConfig
  { step     :: x -> a -> m x
  , initial  :: m x
  , extract  :: x -> m b
  , selector :: s
  }

data StatefulAggregate m a x b s = StatefulAggregate
  { config :: AggregateConfig m a x b s
  , state  :: TVar x
  }

data AggregateError = AggregateError

class AggregateSource source where
  findOrCreate :: MonadIO m
               => AggregateConfig m a x b s
               -> source
               -> m (Either AggregateError (StatefulAggregate m a x b s))

data EphemeralAggregateSource m e r = EphemeralAggregateSource
  { streamFrom :: MonadIO m => [StreamName] -> EventNumber -> m (Stream (Of (Event e)) m r)
  }

instance MonadIO m => AggregateSource (EphemeralAggregateSource m e r) where
  findOrCreate :: forall m a x b s source. MonadIO m
               => AggregateConfig m a x b s
               -> source
               -> m (Either AggregateError (StatefulAggregate m a x b s))
  findOrCreate config@AggregateConfig {..} _ =
    Right . StatefulAggregate config <$> (liftIO . atomically . newTVar =<< initial)

instantValue :: MonadIO m => StatefulAggregate m a x b s -> m b
instantValue StatefulAggregate {..} = extract =<< (liftIO . atomically . readTVar $ state)
  where
    AggregateConfig {..} = config

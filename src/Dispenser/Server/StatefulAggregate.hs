{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dispenser.Server.StatefulAggregate where

import Dispenser.Prelude           hiding ( state )

import qualified Streaming.Prelude as S
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
  { stream :: Stream (Of (Event e)) m r
  }

-- forall m a x b s. MonadIO m

instance AggregateSource (EphemeralAggregateSource m e r) where
  findOrCreate :: MonadIO m1 => AggregateConfig m1 a x b s
               -> EphemeralAggregateSource m e r
               -> m1 (Either AggregateError (StatefulAggregate m1 a x b s))
  -- findOrCreate :: forall m a x b s. MonadIO m
  --              => AggregateConfig m a x b s
  --              -> EphemeralAggregateSource m e r
  --              -> m (Either AggregateError (StatefulAggregate m a x b s))
  findOrCreate config@AggregateConfig {..} EphemeralAggregateSource{..} = do
    var <- (liftIO . atomically . newTVar =<< initial)
    -- TODO: monitor the thread for crashing and crash the aggregate
    -- void . liftIO . forkIO . forever $ do
    --   undefined
    -- void . liftIO . forkIO  $ do
    --   S.effects $ S.mapM f stream
    -- S.effects $ S.mapM f stream
    return . Right . StatefulAggregate config $ var
    where
      f = undefined

      eventNumber' :: EventNumber
      eventNumber' = undefined

      -- TODO:
      streamNames = []

  -- findOrCreate config@AggregateConfig {..} _ =
  --   Right . StatefulAggregate config <$> (liftIO . atomically . newTVar =<< initial)

instantValue :: MonadIO m => StatefulAggregate m a x b s -> m b
instantValue StatefulAggregate {..} = extract =<< (liftIO . atomically . readTVar $ state)
  where
    AggregateConfig {..} = config

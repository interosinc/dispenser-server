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
--  , eventNumber :: EventNumber
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

instance MonadIO m => AggregateSource (EphemeralAggregateSource m e r) where
  findOrCreate :: MonadIO m
               => AggregateConfig m a x b s
               -> EphemeralAggregateSource m e r
               -> m (Either AggregateError (StatefulAggregate m a x b s))
  findOrCreate config@AggregateConfig {..} EphemeralAggregateSource{..} = do
    var <- (liftIO . atomically . newTVar =<< initial)
    -- TODO: monitor the thread for crashing and crash the aggregate

    let stream' :: MonadIO m => Stream (Of e) m r
        stream' = S.mapM f stream

    -- void . liftIO . forkIO  $ do
    --   S.effects $ S.mapM f stream

    -- AAAAAAAAAARGGGH
    S.effects stream'
    -- S.effects (g stream')

    return . Right . StatefulAggregate config $ var
    where
      f :: MonadIO m => Event e -> m e
      f = return . view eventData

      _eventNumber' :: EventNumber
      _eventNumber' = undefined

      -- g :: Stream (Of e) m r -> Stream (Of a0) m1 a1
      -- g = undefined

  -- findOrCreate config@AggregateConfig {..} _ =
  --   Right . StatefulAggregate config <$> (liftIO . atomically . newTVar =<< initial)

instantValue :: MonadIO m => StatefulAggregate m a x b s -> m b
instantValue StatefulAggregate {..} = extract =<< (liftIO . atomically . readTVar $ state)
  where
    AggregateConfig {..} = config

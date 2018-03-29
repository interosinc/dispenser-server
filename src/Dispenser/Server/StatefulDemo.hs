{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Dispenser.Server.StatefulDemo where

import           Dispenser.Prelude
import qualified Streaming.Prelude                  as S

import           Dispenser.Server.StatefulAggregate
import           Dispenser.Types
import           Streaming

demo :: IO ()
demo = do
  putLn "Dispenser StatefulDemo"
  findOrCreate config' source >>= \case
    Left _    -> putLn "ERROR"
    Right agg -> do
      putLn "Aggregate acquired."
      let _ = agg :: StatefulAggregate IO Int Int Int ()
      showAgg agg
      forever $ do
        sleep 3
        showAgg agg
  where
    config' = AggregateConfig step' initial' extract' selector'
      where
        step'     = \a b -> return $ a + b
        initial'  = return (0 :: Int)
        extract'  = return
        selector' = ()

    source = EphemeralAggregateSource streamFrom'
      where
        streamFrom' :: MonadIO m
                    => [StreamName] -> EventNumber -> IO (Stream (Of (Event Integer)) m ())
        streamFrom' _ (EventNumber n) = return $ S.mapM makeEvent . S.each $ [n..]
          where
            makeEvent :: MonadIO m => Integer -> m (Event Integer)
            makeEvent n' = do
              sleep 1
              Event (EventNumber n') [] n' <$> liftIO now

    showAgg = (putLn . ("n = " <>) . show =<<) . instantValue

    sleep :: MonadIO m => Int -> m ()
    sleep = liftIO . threadDelay . seconds

    seconds n = n * 1000 * 1000

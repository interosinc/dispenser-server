{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dispenser.Server.Primitives
     ( pgAppendEvents
     , pgPostEvent
     , pgReadBatchFrom
     ) where

import           Dispenser.Server.Prelude

import qualified Data.List                as List
import           Data.String                      ( fromString )
import           Data.Text                        ( unpack )
import           Dispenser.Server.Orphans         ()
import           Dispenser.Server.Types

-- TODO: `Foldable a` instead of `[a]`?
pgAppendEvents :: forall a. EventData a
               => [StreamName] -> NonEmptyBatch a -> PGConnection
               -> IO (Async EventNumber)
pgAppendEvents streamNames (NonEmptyBatch b) conn =
  async $ withResource (conn ^. pool) $ \dbConn ->
    List.last <$> returning dbConn q (fmap f $ toJSON <$> toList b)
  where
    f :: Value -> (Value, [StreamName])
    f v = (v, streamNames)

    q :: Query
    q = fromString . unpack
          $ "INSERT INTO " <> unPartitionName (conn ^. partitionName)
         <> " (event_data, stream_names)"
         <> " VALUES (?, ?)"
         <> " RETURNING event_number"

pgPostEvent :: EventData a
          => [StreamName] -> a -> PGConnection -> IO (Async EventNumber)
pgPostEvent sns e conn = pgAppendEvents sns (NonEmptyBatch $ e :| []) conn

-- Right now there is no limit on batch size... so obviously we should uh... do
-- something about that.
pgReadBatchFrom :: ( EventData a ) =>
                 EventNumber -> BatchSize -> PGConnection -> IO (Async (Batch (Event a)))
pgReadBatchFrom (EventNumber n) (BatchSize sz) conn
  | sz <= 0   = async (return $ Batch [])
  | otherwise = async $ withResource (conn ^. pool) $ \dbConn -> do
      batchValue :: Batch (Event Value) <- Batch <$> query dbConn q params
      return $ f batchValue
  where
    f :: EventData a => Batch (Event Value) -> Batch (Event a)
    f = fmap (g . fromJSON <$>)

    g :: Result a -> a
    g = \case
      Error e   -> panic $ "unexpected error deserializing result: " <> show e
      Success x -> x

    q :: Query
    q = fromString . unpack
          $ "SELECT event_number, stream_names, event_data, created_at"
         <> " FROM " <> unPartitionName (conn ^. partitionName)
         <> " WHERE event_number >= ?"
         <> " ORDER BY event_number"
         <> " LIMIT ?"

    params :: (Int, Int)
    params = bimap fromIntegral fromIntegral (n, sz)

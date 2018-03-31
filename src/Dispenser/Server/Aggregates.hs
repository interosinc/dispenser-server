{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE NoImplicitPrelude      #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TemplateHaskell        #-}

module Dispenser.Server.Aggregates where

import           Dispenser.Server.Prelude                hiding ( state )
import qualified Streaming.Prelude                as S

import           Control.Concurrent.STM.TVar
import           Data.String                                    ( fromString )
import           Data.Text                                      ( unlines
                                                                , unpack
                                                                )
import           Dispenser.Server.Partition              hiding ( eventNumber )
import qualified Dispenser.Server.Partition       as DSP
import           Dispenser.Server.Streams.Catchup
import           Streaming

--import Dispenser.Types hiding (eventNumber)

data Aggregate m a x b = Aggregate
  { aggregateAggregateId :: AggregateId
  , aggregateExtract     :: x -> m b
  , aggregateInitial     :: m x
  , aggregateSnapshotVar :: TVar (Snapshot x)
  , aggregateStep        :: x -> a -> m x
  }

-- (x -> a -> m x) (m x) (x -> m b)
-- FoldM step initial extract

data AggFold m a x b = AggFold (x -> a -> m x) (m x) (x -> m b)

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

-- instance FromField (Snapshot x) where
--   fromField f mb = Snapshot <$> fromField f mb

currentSnapshot :: MonadIO m => Aggregate m a x b -> m b
currentSnapshot agg = do
  snapshot' <- liftIO . atomically . readTVar $ var
  (agg ^.  extract) (snapshot' ^. state)
  where
    var = agg ^. snapshotVar

create :: forall m a x b. (EventData a, FromField x, MonadIO m)
       => PGConnection -> AggregateId -> AggFold m a x b
       -> m (Aggregate m a x b)
create conn id aggFold = do
  snapshotMay <- liftIO $ latestSnapshot conn id
  case snapshotMay of
    Just snapshot' -> do
      stream <- fromEventNumber conn (succ $ snapshot' ^. eventNumber) batchSize
      var <- liftIO . atomically . newTVar $ snapshot'
      forkUpdater aggFold var stream
      return $ Aggregate id ex' initial' var step'
    Nothing -> do
      stream <- fromZero conn batchSize
      initSnapshot <- Snapshot (EventNumber (-1)) <$> initial'
      var <- liftIO . atomically . newTVar $ initSnapshot
      forkUpdater aggFold var stream
      return $ Aggregate id ex' initial' var step'
  where
    batchSize = BatchSize 100 -- TODO
    AggFold step' initial' ex' = aggFold

forkUpdater :: forall m a x b r. (EventData a, MonadIO m)
            => AggFold m a x b -> TVar (Snapshot x) -> Stream (Of (Event a)) m r
            -> m ()
forkUpdater aggFold var = void . S.effects . S.mapM updateVar
  where
    updateVar :: Event a -> m ()
    updateVar e = do
      curSnapshot :: Snapshot x <- liftIO . atomically $ readTVar var
      let s :: x = curSnapshot ^. state
      x' <- _step' s (e ^. eventData)
      let en'       = e ^. DSP.eventNumber
          snapshot' = Snapshot en' x'
      liftIO . atomically $ writeTVar var snapshot'

    AggFold _step' _initial' _ex' = aggFold

latestSnapshot :: FromField x => PGConnection -> AggregateId -> IO (Maybe (Snapshot x))
latestSnapshot conn (AggregateId id) = withResource (conn ^. pool) $ \dbConn ->
  query dbConn q params >>= \case
    [(en, x)] -> return . Just $ Snapshot en x
    _         -> return Nothing
  where
    q = fromString . unpack . unlines $
          [ "SELECT event_number, state"
          , "FROM " <> snapshotTableName conn
          , "WHERE aggregate_id = ?"
          ]
    params = Only id

snapshotTableName :: PGConnection -> Text
snapshotTableName conn = partName <> "_aggregates"
  where
    partName = unPartitionName $ conn ^. (connectedPartition . partitionName)

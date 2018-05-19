{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE InstanceSigs           #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE NoImplicitPrelude      #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TemplateHaskell        #-}

module Dispenser.Server.Aggregates where

import           Dispenser.Server.Prelude           hiding ( state )
import qualified Streaming.Prelude           as S

import           Control.Concurrent.STM.TVar
import           Control.Monad.Trans.Control               ( liftBaseDiscard )
import           Data.String                               ( fromString )
import           Data.Text                                 ( unlines
                                                           , unpack
                                                           )
import           Dispenser.Server.Db                       ( runSQL )
import           Dispenser.Server.Partition         hiding ( eventNumber )
import qualified Dispenser.Server.Partition  as DSP
import           Streaming

data Aggregate m a x b = Aggregate
  { aggregateAggregateId :: AggregateId
  , aggregateExtract     :: x -> m b
  , aggregateInitial     :: m x
  , aggregateSnapshotVar :: TVar (Snapshot x)
  , aggregateStep        :: x -> a -> m x
  }

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

currentSnapshot :: MonadIO m => Aggregate m a x b -> m b
currentSnapshot agg = (liftIO . atomically . readTVar $ agg ^. snapshotVar)
  >>= (agg ^.  extract) . view state

-- TODO: eliminate MonadResource constraint by proper lifting/interleaving within
create :: forall m a x b.
          (EventData a, FromField x, MonadIO m, MonadBaseControl IO m
          , MonadThrow m)
       => PGConnection a -> AggregateId -> AggFold m a x b
       -> m (Aggregate m a x b)
create conn id aggFold = join . runResourceT $ do
  debug $ "Aggregates.create, id=" <> show id
  snapshotMay :: Maybe (Snapshot x) <- latestSnapshot conn id
  case snapshotMay of
    Just snapshot' -> do
      debug "snapshotMay.Just"
      --foo :: Stream (Of (Event a)) (ResourceT m) r
      -- foo :: Stream (Of (Event a)) m r
      --         <- lift $ fromEventNumber conn (succ $ snapshot' ^. eventNumber) batchSize
      let foo = panic "foo undefined in Aggregates.create"
      let _ = foo :: Stream (Of (Event a)) m r
      return $ startFrom snapshot' foo
      -- lift $ fromEventNumber conn (succ $ snapshot' ^. eventNumber) batchSize
      --   >>= startFrom snapshot'
    Nothing -> do
      debug "snapshotMay.Nothing"
      initSnapshot :: Snapshot x <- lift (Snapshot (EventNumber (-1)) <$> initial')

      let _ = initial' :: m x

      let _start :: Stream (Of (Event a)) m r -> m (Aggregate m a x b)
          _start = startFrom initSnapshot

      let _fz :: MonadResource m => m (Stream (Of (Event a)) m r)
          _fz = fromZero conn batchSize
            where
              fromZero = panic "no fromZero!"

      _sp :: Stream (Of (Event a)) m r <- panic "_sp undefined in Aggregates.create"

      -- res :: Aggregate m a x b <- undefined -- return $ start sp
      -- res

      let blah :: m (Aggregate m a x b)
          blah = panic "blah undefined in Aggregates.create"

      return blah

      -- foo :: Aggregate m a x b <- (startFrom initSnapshot) =<< (lift $ fromZero conn batchSize)
      --undefined

      -- bar :: Stream (Of (Event a)) (ResourceT m) r <- fromZero conn batchSize

      -- startFrom initSnapshot bar

--      foo :: Aggregate m a x b <- lift $ startFrom initSnapshot bar
      -- foo :: Stream (Of (Event a)) m r <- lift $
      -- foo :: Stream (Of (Event a)) m r <- lift $ startFrom initSnapshot
      --fromZero conn batchSize foo
      -- undefined
  where
    batchSize = BatchSize 100 -- TODO
    AggFold step' initial' ex' = aggFold

    startFrom :: Snapshot x -> Stream (Of (Event a)) m r -> m (Aggregate m a x b)
    startFrom snapshot' stream = do
      var <- liftIO . atomically . newTVar $ snapshot'
      forkUpdater aggFold var stream
      return $ Aggregate id ex' initial' var step'

createAggTable :: PGConnection a -> IO ()
createAggTable conn = withResource (conn ^. pool) $ \dbConn -> do
  createTable dbConn
  createIndexes dbConn
  where
    createTable :: Connection -> IO ()
    createTable dbConn = runSQL dbConn $
      "CREATE TABLE " <> table
      <> " ( aggregate_id TEXT PRIMARY KEY"
      <> " , event_number BIGSERIAL"
      <> " , state        JSONB NOT NULL"
      <> " )"

    table = snapshotTableName conn

    createIndexes = const $ return () -- TODO

dropAggTable :: PGConnection a -> IO ()
dropAggTable conn = withResource (conn ^. pool) $ \dbConn ->
  runSQL dbConn $ "DROP TABLE IF EXISTS " <> snapshotTableName conn

forkUpdater :: forall m a x b r. (EventData a, MonadIO m, MonadBaseControl IO m)
            => AggFold m a x b -> TVar (Snapshot x) -> Stream (Of (Event a)) m r
            -> m ()
forkUpdater aggFold var =
  void . liftBaseDiscard forkIO . void . S.effects . S.mapM updateVar
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

latestSnapshot :: (MonadIO m, MonadResource m)
               => FromField x => PGConnection a -> AggregateId -> m (Maybe (Snapshot x))
latestSnapshot conn (AggregateId id) = liftIO . withResource (conn ^. pool) $ \dbConn ->
  -- TODO: One problem is that if the below fails then it will appear as if
  --       there is no snapshot and the aggregate will restart... which would
  --       potentially cause monadic effects to re-trigger, etc.  we should
  --       instead check the event number separately and if we load a snapshot
  --       but then can't deserialize it we fail
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

recreateAggTable :: PGConnection a -> IO ()
recreateAggTable conn = do
  dropAggTable conn
  createAggTable conn

snapshotTableName :: PGConnection a -> Text
snapshotTableName conn = partName <> "_aggregates"
  where
    partName = unPartitionName $ conn ^. (connectedPartition . partitionName)

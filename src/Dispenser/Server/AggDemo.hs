{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dispenser.Server.AggDemo where

import           Dispenser.Prelude

import           Data.Aeson
import qualified Data.Map.Strict                      as Map
import           Data.Text                                   ( toLower
                                                             , words
                                                             )
import           Database.PostgreSQL.Simple.FromField        ( FromField
                                                             , fromField
                                                             )
import           Dispenser.Server.Aggregates
import qualified Dispenser.Server.Aggregates          as Agg
import           Dispenser.Server.Partition                  ( pgConnect
                                                             , recreate
                                                             )
import           Dispenser.Types

data DemoEvent
  = MessageEvent Text
  deriving (Eq, Generic, Ord, Read, Show)

instance EventData DemoEvent
instance FromJSON  DemoEvent
instance ToJSON    DemoEvent

newtype WordCounts = WordCounts (Map Text Int)
  deriving (Eq, Generic, Ord, Read, Show)

instance FromField WordCounts where
  -- fromField f mb = WordCounts <$> fromField f mb
  fromField f mb = do
    s <- fromField f mb
    case eitherDecode s of
      Left  _ -> mzero
      Right x -> return $ WordCounts x

demo :: IO ()
demo = do
  putLn "AggDemo"
  conn <- pgConnect part (PoolSize 10)
  recreate conn
  -- TODO: recreateAgg ...
  agg :: Aggregate IO DemoEvent WordCounts WordCounts <- Agg.create conn id aggFold
  snapshot :: WordCounts <- currentSnapshot agg
  putLn $ "Snapshot: " <> show snapshot
  where
    id        = AggregateId "demoAgg1"
    aggFold   = AggFold step' initial' extract'

    step' :: WordCounts -> DemoEvent -> IO WordCounts
    step' (WordCounts m) (MessageEvent txt) = return . WordCounts $ foldr f z xs
      where
        f :: Text -> Map Text Int -> Map Text Int
        f word = Map.insertWith (+) word 1

        z  = m
        xs = words . toLower $ txt

    initial'  = return $ WordCounts Map.empty
    extract'  = return

    part      = Partition (DatabaseURL dbUrl') (PartitionName partName')
    dbUrl'    = "postgres://dispenser@localhost:5432/dispenser"
    partName' = "agg_demo"

postMessage :: Text -> IO ()
postMessage msg = do
  conn <- pgConnect part (PoolSize 10)
  void $ postEvent conn streamNames e
  where
    e :: DemoEvent
    e = MessageEvent msg

    streamNames = []

    part      = Partition (DatabaseURL dbUrl') (PartitionName partName')
    dbUrl'    = "postgres://dispenser@localhost:5432/dispenser"
    partName' = "agg_demo"

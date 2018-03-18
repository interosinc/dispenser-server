{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dispenser.Server.Aggregates.PartitionDemo
     ( PartitionAggregateSource
     , Account
     , balance
     , connect
     , demo1
     ) where

import           Dispenser.Server.Prelude

import           Dispenser.Server.Aggregates.Partition
import qualified Dispenser.Server.Partition            as Partition
import           Dispenser.Server.Types

-- import Dispenser.Aggregates

newtype Account = Account
  { balance :: Integer
  } deriving (Eq, Generic, Ord, Read, Show)

instance FromJSON Account
instance ToJSON Account

demo1 :: IO ()
demo1 = do
  conn <- Partition.connect partition' (PoolSize 5)
  _src  <- connect conn
  -- aggregate (AggregateId "users/123/account") src >>= \case
  --   Left err  -> putLn $ "error acquiring aggregate: " <> show err
  --   Right (agg :: Aggregate Account) -> do
  --     val <- currentValue agg
  --     -- putLn $ "aggregate: " <> show val
  --     putLn $ "account balance: " <> show (balance val)

  notImplemented "demo1"
  where
    partition' = Partition (DatabaseURL url) (TableName tableName')
    url        = "postgres://dispenser@localhost:5432/dispenser"
    tableName' = "agg_demo_1"

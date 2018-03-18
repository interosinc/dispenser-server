{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell   #-}

module Dispenser.Server.Types
     ( module Exports
     , PartitionConnection( PartitionConnection )
     , connectedPartition
     , pool
     ) where

import Dispenser.Server.Prelude

import Dispenser.Types          as Exports

data PartitionConnection = PartitionConnection
  { _connectedPartition :: Partition
  , _pool               :: Pool Connection
  } deriving (Generic)

makeClassy ''PartitionConnection

instance HasPartition PartitionConnection where
  partition = connectedPartition

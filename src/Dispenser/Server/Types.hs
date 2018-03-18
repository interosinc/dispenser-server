{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell   #-}

module Dispenser.Server.Types
     ( module Exports
     , PGConnection( PGConnection )
     , connectedPartition
     , pool
     ) where

import Dispenser.Server.Prelude

import Dispenser.Types          as Exports

data PGConnection = PGConnection
  { _connectedPartition :: Partition
  , _pool               :: Pool Connection
  } deriving (Generic)

makeClassy ''PGConnection

instance HasPartition PGConnection where
  partition = connectedPartition

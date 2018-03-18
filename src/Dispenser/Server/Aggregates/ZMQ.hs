{-# LANGUAGE NoImplicitPrelude #-}

module Dispenser.Server.Aggregates.ZMQ
       ( ZMQAggregateSource
       , connect
       ) where

import           Dispenser.Server.Prelude

import           System.ZMQ4                   hiding ( connect )
import qualified System.ZMQ4              as Z

-- import           Dispenser.Aggregates

newtype ZMQAggregateSource = ZMQAggregateSource
  { _socket :: Socket Req
  }

-- instance AggregateSource ZMQAggregateSource where
--   aggregate = const . Right $ Aggregate

-- TODO: finalization of resources... shutdown/close/etc

connect :: String -> IO ZMQAggregateSource
connect addr = do
  ctx <- context
  sock <- socket ctx Req
  Z.connect sock addr
  create sock

create :: Socket Req -> IO ZMQAggregateSource
create = return . ZMQAggregateSource

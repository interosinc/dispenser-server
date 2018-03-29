{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Dispenser.Server.Types
     ( module Exports
     ) where

import Dispenser.Prelude

import Dispenser.Types   as Exports

newtype DatabaseURL = DatabaseURL Text
  deriving (Eq, Generic, Ord, Read, Show)

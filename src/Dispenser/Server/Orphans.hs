{-# LANGUAGE FlexibleInstances    #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Dispenser.Server.Orphans () where

import Dispenser.Server.Prelude

import Database.PostgreSQL.Simple.Types ( PGArray( PGArray )
                                        , fromPGArray
                                        )
import Dispenser.Server.Types

instance FromField a => FromRow (Event a) where

instance FromField EventNumber where
  fromField f mb = EventNumber <$> fromField f mb

instance ToField EventNumber where
  toField (EventNumber n) = toField n

instance FromRow EventNumber where
  fromRow = EventNumber <$> field

instance FromField [StreamName] where
  fromField f mb = fmap StreamName . fromPGArray <$> fromField f mb

instance ToField [StreamName] where
  toField = toField . PGArray . fmap (encodeUtf8 . unStreamName)

instance FromField Timestamp where
  fromField f mb = Timestamp <$> fromField f mb

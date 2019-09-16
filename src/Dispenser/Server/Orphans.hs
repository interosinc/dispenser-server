{-# LANGUAGE FlexibleInstances    #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Dispenser.Server.Orphans () where

import Dispenser.Server.Prelude

import Data.Set                         ( fromList )
import Database.PostgreSQL.Simple.Types ( PGArray( PGArray )
                                        , fromPGArray
                                        )
import Dispenser.Types                  ( Event
                                        , EventNumber( EventNumber )
                                        , StreamName( StreamName )
                                        , Timestamp( Timestamp )
                                        , unStreamName
                                        )

instance FromField a => FromRow (Event a) where

instance FromField EventNumber where
  fromField f mb = EventNumber <$> fromField f mb

instance ToField EventNumber where
  toField (EventNumber n) = toField n

instance FromRow EventNumber where
  fromRow = EventNumber <$> field

instance FromField (Set StreamName) where
  fromField f mb = fromList . fmap StreamName . fromPGArray <$> fromField f mb

instance ToField (Set StreamName) where
  toField = toField . PGArray . fmap (encodeUtf8 . unStreamName) . toList

instance FromField Timestamp where
  fromField f mb = Timestamp <$> fromField f mb

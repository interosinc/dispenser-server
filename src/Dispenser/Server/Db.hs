{-# LANGUAGE NoImplicitPrelude #-}

module Dispenser.Server.Db
     ( poolFromUrl
     , runSQL
     ) where

import Dispenser.Server.Prelude

import Data.Pool                  ( createPool )
import Data.String                ( fromString )
import Data.Text                  ( unpack )
import Database.PostgreSQL.Simple ( close
                                  , connectPostgreSQL
                                  , execute_
                                  )
import Dispenser.Types            ( DatabaseURL( DatabaseURL ) )

poolFromUrl :: DatabaseURL -> Int -> IO (Pool Connection)
poolFromUrl (DatabaseURL url) size =
  createPool (connectPostgreSQL . encodeUtf8 $ url) close minSize idleSecs maxSize
  where
    minSize  = 1
    maxSize  = size
    idleSecs = 1

runSQL :: Connection -> Text -> IO ()
runSQL conn = void . execute_ conn . fromString . unpack

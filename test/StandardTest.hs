{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module StandardTest where

import Dispenser.Prelude

import Control.Monad.Trans.Resource ( ResourceT )
import Dispenser                    ( PartitionName( PartitionName )
                                    , connect
                                    )
import Dispenser.ResourceTOrphans   ()
import Dispenser.Server             ( PgClient
                                    , PgConnection
                                    , new
                                    )
import Dispenser.Tests              ( TestConfig( TestConfig )
                                    , TestEvent
                                    , trivialSpecFromClient
                                    )
import Test.Tasty.Hspec             ( Spec
                                    , runIO
                                    )

spec_standard :: Spec
spec_standard = do
  let testCfg :: TestConfig (PgClient TestEvent) PgConnection
      testCfg = TestConfig makeConn
        where
          makeConn :: ResourceT IO (PgConnection TestEvent)
          makeConn = do
            -- TODO: env vars
            let poolMax = 5
                url = "postgres://dispenser:dispenser@localhost:5432/dispenser"
            client :: PgClient TestEvent <- liftIO $ new poolMax url
            connect (PartitionName "randomize-me") client

  generatedSpec <- runIO . runResourceT $
    trivialSpecFromClient testCfg "PgClient"
  generatedSpec

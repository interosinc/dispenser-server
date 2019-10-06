{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module StandardTest where

import Dispenser.Prelude

import Control.Monad.Trans.Resource ( ResourceT )
import Data.Text                    ( pack )
import Dispenser.ResourceTOrphans   ()
import Dispenser.Server             ( PgClient
                                    , PgConnection
                                    , connect
                                    , ensureExists
                                    , new
                                    )
import Dispenser.Tests              ( TestConfig( TestConfig )
                                    , TestEvent
                                    , partitionConnectionSpecFrom
                                    , randomPartitionName
                                    )
import System.Environment           ( lookupEnv )
import Test.Tasty.Hspec             ( Spec
                                    , runIO
                                    )

spec_standard :: Spec
spec_standard = join . runIO $ do
  url <- fromMaybe defaultUrl
           <$> (pack <<$>> lookupEnv "DISPENSER_TEST_POSTGRES_URL")
  poolMaxMay <- lookupEnv "DISPENSER_TEST_POSTGRES_POOL_MAX"
  let poolMax = maybe defaultPoolMax
                  (fromMaybe (panic $ "invalid DISPENSER_TEST_POSTGRES_POOL_MAX: ")
                   . readMaybe)
                  poolMaxMay
  client :: PgClient TestEvent <- liftIO $ new poolMax url
  runResourceT $
    partitionConnectionSpecFrom (mkTestCfg client) "PgClient"
  where
    mkTestCfg :: PgClient TestEvent
            -> TestConfig (PgClient TestEvent) PgConnection
    mkTestCfg client = TestConfig makeConn
      where
        makeConn :: ResourceT IO (PgConnection TestEvent)
        makeConn = do
          partName <- liftIO randomPartitionName
          conn <- connect partName client
          liftIO $ ensureExists conn
          pure conn

    defaultUrl = "postgres://dispenser:dispenser@localhost:5432/dispenser"
    defaultPoolMax = 5

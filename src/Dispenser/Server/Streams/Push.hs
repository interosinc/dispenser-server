{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

module Dispenser.Server.Streams.Push
     ( pgFromNow
     ) where

import           Dispenser.Server.Prelude
import qualified Streaming.Prelude                       as S

import           Control.Lens                                    ( makeFields )
import           Control.Monad.Fail                              ( fail )
import           Data.Aeson
import qualified Data.ByteString.Lazy                    as Lazy
import qualified Data.HashMap.Strict                     as HM
import           Data.String                                     ( fromString )
import           Data.Text                                       ( pack
                                                                 , unpack
                                                                 )
import           Database.PostgreSQL.Simple.Notification
import           Dispenser.Server.Partition
import           Streaming

newtype PushEvent a = PushEvent { unEvent :: Event a }
  deriving (Eq, Ord, Read, Show)

makeFields ''PushEvent

-- { "event_number": 4
-- , "stream_names": [ "demo-stream" ]
-- , "event_data": { "tag": "Checkout"
--                 , "contents": 9
--                 }
-- , "created_at": "2018-03-07T17:29:58.747602-07:00"
-- }
instance FromJSON a => FromJSON (PushEvent a) where
  parseJSON = withObject "notificationEvent" $ \obj -> do
    n   <- parseField "event_number" obj
    sns <- parseField "stream_names" obj
    val <- parseField "event_data"   obj
    at  <- parseField "created_at"   obj
    return . PushEvent $ Event (EventNumber n) (map StreamName sns) val (Timestamp at)
    where
      parseField s obj = case HM.lookup s obj of
        Just x  -> parseJSON x
        Nothing -> fail $ "no '" <> unpack s <> "' field"

pgFromNow :: (EventData a, MonadIO m)
        => PGConnection -> m (Stream (Of (Event a)) m r)
pgFromNow partConn = do
  debug $ "pgFromNow!"
  -- TODO: This will leak connections if an exception occurs.
  --       conn should be destroyed or returned
  (conn, _) <- liftIO $ takeResource (partConn ^. pool)
  debug $ "Listening for notifications on: " <> show channelText
  void . liftIO . execute_ conn . fromString . unpack $ "LISTEN " <> channelText
  -- debug "pushStream acquired connection"
  return . forever $ do
    -- debug "pushStream attempting to acquire notification"
    n <- liftIO $ getNotification conn
    -- debug $ "got notification: " <> show n
    when (notificationChannel n == channelBytes) $
      -- debug "notification was for the right channel!"
      case deserializeNotificationEvent . notificationData $ n of
        Left err -> panic $ "pushStream ERROR: " <> show err
        Right e  -> do
          debug $ "yielding event: " <> show e
          S.yield e
  where
    channelBytes = encodeUtf8 channelText
    channelText  = partitionNameToChannelName . unPartitionName
      $ partConn ^. partitionName

deserializeNotificationEvent :: EventData a => ByteString -> Either Text (Event a)
deserializeNotificationEvent = bimap pack unEvent . eitherDecode . Lazy.fromStrict

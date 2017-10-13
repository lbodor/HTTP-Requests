{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}

import           Control.Concurrent       (threadDelay)
import           Control.Concurrent.Async (async, wait)
import           Control.Lens             hiding (each)
import           Data.Aeson               (ToJSON, toJSON)
import           GHC.Generics             (Generic)
import           Network.HTTP.Client      (ManagerSettings,
                                           defaultManagerSettings,
                                           managerRetryableException)
import           Network.Wreq             (Options, auth, basicAuth, defaults,
                                           header, httpProxy, manager, postWith,
                                           proxy)
import           System.Environment       (getArgs)

import           Control.Concurrent.Lock  (Lock)
import qualified Control.Concurrent.Lock  as Lock

import           Control.Monad
import qualified Data.UUID                as UUID (toASCIIBytes)
import qualified Data.UUID.V4             as UUID (nextRandom)
import           Pipes
import           Pipes.Concurrent

newtype Event = Event String deriving (Generic, Show)

instance ToJSON Event

event :: Event
event = Event "event data"

httpClientManagerSettings :: ManagerSettings
httpClientManagerSettings = defaultManagerSettings {
    managerRetryableException = const True
}

requestOptions :: Options
requestOptions = defaults
    & manager .~ Left httpClientManagerSettings
    & proxy   ?~ httpProxy "proxy.inno.lan" 3128
    & auth    ?~ basicAuth "admin" ""
    & header "Content-Type" .~ ["application/json"]
    & header "ES-EventType" .~ ["SomeEvent"]

worker :: (Show a) => Lock -> Int -> Consumer a IO r
worker lock workerId = forever $ do
    objectId <- show <$> await
    let objectUrl = "http://52.64.253.246/streams/object-" ++ objectId

    eventId <- UUID.toASCIIBytes <$> liftIO UUID.nextRandom
    let request = requestOptions & header "EventId" .~ [eventId]

    _ <- liftIO $ postWith request objectUrl (toJSON event)

    lift $ Lock.with lock (putStrLn $ "Worker #" ++ show workerId ++ ": Processed " ++ objectId)
    lift $ threadDelay 100000  -- 1/10th second

main :: IO ()
main = do
    [jobs, threads] <- map read <$> getArgs
    (output, input, seal) <- spawn' unbounded
    lock <- Lock.new
    as <- forM [1..threads] $ \i ->
          async $ runEffect $ fromInput input  >-> worker lock i
    a  <- async $ do runEffect $ each [1..jobs :: Int] >-> toOutput output
                     atomically seal
    mapM_ wait (a:as)

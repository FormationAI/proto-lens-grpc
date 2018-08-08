{-# LANGUAGE AllowAmbiguousTypes     #-}
{-# LANGUAGE DataKinds               #-}
{-# LANGUAGE DeriveFunctor           #-}
{-# LANGUAGE DeriveTraversable       #-}
{-# LANGUAGE FlexibleContexts        #-}
{-# LANGUAGE FunctionalDependencies  #-}
{-# LANGUAGE GADTs                   #-}
{-# LANGUAGE KindSignatures          #-}
{-# LANGUAGE LambdaCase              #-}
{-# LANGUAGE MultiParamTypeClasses   #-}
{-# LANGUAGE OverloadedLists         #-}
{-# LANGUAGE OverloadedLabels        #-}
{-# LANGUAGE OverloadedStrings       #-}
{-# LANGUAGE RecordWildCards         #-}
{-# LANGUAGE ScopedTypeVariables     #-}
{-# LANGUAGE StandaloneDeriving      #-}
{-# LANGUAGE TemplateHaskell         #-}
{-# LANGUAGE TypeApplications        #-}


module Platform.GRPC.Server
  ( AnyHandler(..)
  , Handler(..)
  , ServerOptions(..)
  , Response(..)
  , errNoDetails
  , detailMessage

  , ServerHandler
  , ServerReaderHandler
  , ServerWriterHandler
  , ServerRWHandler
  , ServerHandler'
  , ServerReaderHandler'
  , ServerWriterHandler'
  , ServerRWHandler'

  , defaultOptions
  , serverLoop
  ) where

import           Control.Concurrent.Async
import qualified Control.Exception as CE
import           Control.Lens ((&), (.~), view)
import           Control.Monad (forever)
import qualified Data.ByteString as BS
import           Data.Either (fromRight)
import           Data.Foldable (find)
import           Data.ProtoLens (Message, def, encodeMessage, decodeMessage)
import qualified Data.ProtoLens.Any as Any
import qualified Data.Text as T
import           Lens.Labels.Unwrapped ()
import qualified Network.GRPC.LowLevel as GL
import           Network.GRPC.LowLevel
                 ( GRPCIOError(..)
                 , MetadataMap
                 , MethodName
                 , MethodPayload
                 , ServerCall
                 , ServerHandlerLL
                 , ServerReaderHandlerLL
                 , ServerRWHandlerLL
                 , ServerWriterHandlerLL
                 , StreamSend
                 , StreamRecv
                 , StatusCode
                 , StatusDetails(..)
                 )
import qualified Network.GRPC.LowLevel.Call.Unregistered as U
import qualified Network.GRPC.LowLevel.Server.Unregistered as U
import           Proto.Google.Protobuf.Empty (Empty)
import qualified Proto.Google.Rpc.Status as P

data Response a where
  StatusOk :: a -> Response a
  StatusErr :: forall a e. Message e => StatusCode -> T.Text -> e -> Response a

deriving instance Foldable Response
deriving instance Functor Response
deriving instance Traversable Response

errNoDetails :: StatusCode -> T.Text -> Response a
errNoDetails c m = StatusErr c m (def @Empty)

detailMessage :: StatusDetails -> T.Text
detailMessage (StatusDetails b) =
  fromRight "Value was not a valid grpc/status/status.proto Status message." $
  view #message <$> decodeMessage @P.Status b

type ServerHandler' m a b = ServerCall a
                              -> m (MetadataMap, Response b)

type ServerReaderHandler' m a b = ServerCall (MethodPayload 'GL.ClientStreaming)
                                    -> StreamRecv a
                                    -> m (MetadataMap, Response (Maybe b))

type ServerWriterHandler' m a b = ServerCall a
                                    -> StreamSend b
                                    -> m (MetadataMap, Response ())

type ServerRWHandler' m a b = ServerCall (MethodPayload 'GL.BiDiStreaming)
                                -> StreamRecv a
                                -> StreamSend b
                                -> m (MetadataMap, Response ())

type ServerHandler a b = ServerHandler' IO a b
type ServerReaderHandler a b = ServerReaderHandler' IO a b
type ServerWriterHandler a b = ServerWriterHandler' IO a b
type ServerRWHandler a b = ServerRWHandler' IO a b

statusCode :: Response a -> StatusCode
statusCode (StatusOk _) = GL.StatusOk
statusCode (StatusErr sc _ _) = sc

statusDetails :: Response a -> StatusDetails
statusDetails (StatusOk _) = mempty
statusDetails (StatusErr sc m sd) =
  let status :: P.Status
      status = def & #code .~ fromIntegral (fromEnum sc)
                   & #message .~ m
                   & #details .~ [Any.pack sd]
   in StatusDetails . toBS $ status

convertServerReaderHandler :: (Message a, Message b)
                           => ServerReaderHandler a b
                           -> ServerReaderHandlerLL
convertServerReaderHandler f c recv =
  uncurry toServerResponse <$> f c (convertRecv recv)
  where
    toServerResponse m = \case
      StatusOk a -> (toBS <$> a, m, GL.StatusOk, mempty)
      s -> (mempty, m, statusCode s, statusDetails s)

convertServerWriterHandler :: (Message a, Message b) =>
                              ServerWriterHandler a b
                              -> ServerWriterHandlerLL
convertServerWriterHandler f c send =
  convertResponse <$> f (convert <$> c) (convertSend send)
  where
    convert bs = case decodeMessage bs of
      Left x  -> CE.throw (GRPCIOHandlerException x)
      Right x -> x
    convertResponse (m, s) = (m, statusCode s, statusDetails s)

data ServerOptions = ServerOptions
  { optServerHost           :: GL.Host
    -- ^ Name of the host the server is running on.
  , optServerPort           :: GL.Port
    -- ^ Port on which to listen for requests.
  , optUseCompression       :: Bool
    -- ^ Whether to use compression when communicating with the client.
  , optUserAgentPrefix      :: String
    -- ^ Optional custom prefix to add to the user agent string.
  , optUserAgentSuffix      :: String
    -- ^ Optional custom suffix to add to the user agent string.
  , optInitialMetadata      :: MetadataMap
    -- ^ Metadata to send at the beginning of each call.
  , optSSLConfig            :: Maybe GL.ServerSSLConfig
    -- ^ Security configuration.
  }

defaultOptions :: ServerOptions
defaultOptions = ServerOptions
  { optServerHost           = "localhost"
  , optServerPort           = 50051
  , optUseCompression       = False
  , optUserAgentPrefix      = "grpc-haskell/0.0.0"
  , optUserAgentSuffix      = ""
  , optInitialMetadata      = mempty
  , optSSLConfig            = Nothing
  }

data Handler (a :: GL.GRPCMethodType) where
  UnaryHandler        :: (Message c, Message d) => MethodName -> ServerHandler c d -> Handler 'GL.Normal
  ClientStreamHandler :: (Message c, Message d) => MethodName -> ServerReaderHandler c d -> Handler 'GL.ClientStreaming
  ServerStreamHandler :: (Message c, Message d) => MethodName -> ServerWriterHandler c d -> Handler 'GL.ServerStreaming
  BiDiStreamHandler   :: (Message c, Message d) => MethodName -> ServerRWHandler c d     -> Handler 'GL.BiDiStreaming

data AnyHandler where
  AnyHandler :: Handler (a :: GL.GRPCMethodType) ->  AnyHandler

anyHandlerMethodName :: AnyHandler -> MethodName
anyHandlerMethodName (AnyHandler m) = handlerMethodName m

handlerMethodName :: Handler a -> MethodName
handlerMethodName (UnaryHandler m _)        = m
handlerMethodName (ClientStreamHandler m _) = m
handlerMethodName (ServerStreamHandler m _) = m
handlerMethodName (BiDiStreamHandler m _)   = m

toBS :: Message a => a -> BS.ByteString
toBS = encodeMessage

convertSend :: Message a => StreamSend BS.ByteString -> StreamSend a
convertSend s = s . toBS

convertServerHandler :: (Message a, Message b)
                     => ServerHandler a b
                     -> ServerHandlerLL
convertServerHandler f c = case decodeMessage (GL.payload c) of
  Left x  -> CE.throw (GRPCIOHandlerException x)
  Right x -> uncurry toServerResponse <$> f (fmap (const x) c)
  where
    toServerResponse m = \case
      StatusOk a -> (toBS a, m, GL.StatusOk, mempty)
      s -> (mempty, m, statusCode s, statusDetails s)

convertServerRWHandler :: (Message a, Message b)
                       => ServerRWHandler a b
                       -> ServerRWHandlerLL
convertServerRWHandler f c recv send =
  do
    (m, s) <- f c (convertRecv recv) (convertSend send)
    pure (m, statusCode s, statusDetails s)

convertRecv :: Message a => StreamRecv BS.ByteString -> StreamRecv a
convertRecv =
  fmap $ \e -> do
    msg <- e
    case msg of
      Nothing -> return Nothing
      Just bs -> case decodeMessage bs of
                   Left x  -> Left (GRPCIOHandlerException x)
                   Right x -> return (Just x)

dispatchLoop :: 
       GL.Server
    -> MetadataMap
    -> [AnyHandler]
    -> IO ()
dispatchLoop s md allHandlers = do
    forever $ U.withServerCallAsync s $ \sc -> do
      case findHandler sc allHandlers of
        Just (AnyHandler ah) -> do
            case ah of
                UnaryHandler _ h        -> unaryHandler sc h
                ClientStreamHandler _ h -> csHandler sc h
                ServerStreamHandler _ h -> ssHandler sc h
                BiDiStreamHandler _ h   -> bdHandler sc h
        Nothing -> return ()
  where
    findHandler sc = find ((== U.callMethod sc) . anyHandlerMethodName)

    unaryHandler :: (Message a, Message b)
                 => U.ServerCall -> ServerHandler a b -> IO ()
    unaryHandler sc h =
        handleError . U.serverHandleNormalCall' s sc md $ \_sc' bs ->
          convertServerHandler h (const bs <$> U.convertCall sc)

    handleError :: IO (Either GRPCIOError ()) -> IO ()
    handleError _ = return () -- TODO

    csHandler :: (Message a, Message b)
              => U.ServerCall -> ServerReaderHandler a b -> IO ()
    csHandler sc = handleError . U.serverReader s sc md . convertServerReaderHandler

    ssHandler :: (Message a, Message b)
              => U.ServerCall -> ServerWriterHandler a b -> IO ()
    ssHandler sc = handleError . U.serverWriter s sc md . convertServerWriterHandler

    bdHandler :: (Message a, Message b)
              => U.ServerCall -> ServerRWHandler a b -> IO ()
    bdHandler sc = handleError . U.serverRW s sc md . convertServerRWHandler

serverLoop :: 
              ServerOptions
           -> [AnyHandler]
           -> IO ()
serverLoop ServerOptions{..} hs = do
  -- We run the loop in a new thread so that we can kill the serverLoop thread.
  -- Without this fork, we block on a foreign call, which can't be interrupted.
  tid <- async $ GL.withGRPC $ \grpc ->
    GL.withServer grpc config $ \server -> do
      dispatchLoop server optInitialMetadata hs
  wait tid
  where
    config = GL.ServerConfig
      { GL.host                             = optServerHost
      , GL.port                             = optServerPort
      , GL.methodsToRegisterNormal          = []
      , GL.methodsToRegisterClientStreaming = []
      , GL.methodsToRegisterServerStreaming = []
      , GL.methodsToRegisterBiDiStreaming   = []
      , GL.serverArgs                       =
          [GL.CompressionAlgArg GL.GrpcCompressDeflate | optUseCompression]
          ++
          [ GL.UserAgentPrefix optUserAgentPrefix
          , GL.UserAgentSuffix optUserAgentSuffix
          ]
      , GL.sslConfig = optSSLConfig
      }

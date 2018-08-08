{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE RankNTypes            #-}
{-# OPTIONS_GHC -fno-warn-redundant-constraints          #-}
{-# OPTIONS_GHC -fno-warn-unticked-promoted-constructors #-}

module Platform.GRPC.Magic
  ( Client ()
  , protoToDomainType
  , method
  , withClient
  , RunNormalClient
  , runNormalClient
  , runNormalClient'
  , RunStreamingClient
  , runStreamingClient
  , RunServerStreamingClient
  , runServerStreamingClient
  , RunBiDiStreamingClient
  , runBiDiStreamingClient
  , ServiceRecord
  , service
  , runServer
  ) where

import           Control.Arrow (left)
import           Control.Lens hiding ((:>), (<|))
import           Data.ByteString (ByteString)
import           Data.ByteString.Char8 (pack)
import           Data.IORef (newIORef, writeIORef, readIORef)
import           Data.Kind (Constraint)
import           Data.List.NonEmpty ((<|))
import qualified Data.List.NonEmpty as Nel
import           Data.ProtoLens.Encoding (encodeMessage, decodeMessage)
import           Data.ProtoLens.Message (Message (..))
import qualified Data.ProtoLens.Service.Types as PL
import           Data.ProtoLens.Service.Types ( MethodInput
                                              , MethodOutput
                                              , MethodStreamingType
                                              , StreamingType (..)
                                              , Service
                                              , ServiceMethods
                                              , ServiceName
                                              , ServicePackage
                                              , HasMethod
                                              )
import           Data.Proxy (Proxy (..))
import           Data.These (These(..))
import           GHC.TypeLits
import           Platform.GRPC.Data.ExtensibleRecord ( Record
                                                     , WrappedMethodName (..)
                                                     , Lookup
                                                     , safeFind
                                                     , method
                                                     , service)
import           Platform.GRPC.LowLevelMagic (clientReader')
import qualified Network.GRPC.LowLevel as GL
import           Network.GRPC.LowLevel ( GRPCIOError (..)
                                       , MethodName (..)
                                       , rspBody
                                       , ServerCall
                                       , MetadataMap)
import qualified Network.GRPC.LowLevel as LL
import           Network.GRPC.LowLevel.Client (clientRW', clientWriterCmn)
import           Network.GRPC.LowLevel.Client.Unregistered (clientRequest, withClientCall)
import           Platform.GRPC.Server hiding (ServerHandler, ServerReaderHandler, ServerWriterHandler, ServerRWHandler)
import           Unsafe.Coerce (unsafeCoerce)


------------------------------------------------------------------------------
-- | Wrapped 'LL.Client' associated with the end service.
data Client s = Client LL.Client


------------------------------------------------------------------------------
-- | Wrapped 'LL.withClient' that will associate calls with the correct service.
withClient :: LL.GRPC -> s -> LL.ClientConfig -> (Client s -> IO a) -> IO a
withClient grpc _ cc f = LL.withClient grpc cc $ f . Client


------------------------------------------------------------------------------
-- | Pull out a method name from the rpc metadata.
methodName :: forall s m. (Service s, HasMethod s m) => MethodName
methodName = LL.MethodName
           . pack
           $ mconcat [ "/"
                     , symbolVal (Proxy @(ServicePackage s))
                         & _Cons . _2 <>~ "."
                     , symbolVal $ Proxy @(ServiceName s)
                     , "/"
                     , symbolVal $ Proxy @(PL.MethodName s m)
                     ]


------------------------------------------------------------------------------
-- | Shim to convert between protolens' parse-failure messages into
-- a GRPCIOError. This is probably not the best implementation.
packDecodeError :: String -> GRPCIOError
packDecodeError = GRPCIOHandlerException


------------------------------------------------------------------------------
-- | Better error messages for attempting to run the wrong RPC type.
type family RunClientError (st :: StreamingType) where
    RunClientError 'NonStreaming
        = Text "on a non-streaming method."
     :$$: Text "Did you mean runNormalClient?"
    RunClientError 'ClientStreaming
        = Text "on a server-streaming method."
     :$$: Text "Did you mean runServerStreamingClient?"
    RunClientError 'ServerStreaming
        = Text "on a bidi-streaming method."
     :$$: Text "Did you mean runBiDiStreamingClient?"
    RunClientError 'BiDiStreaming
        = Text "on a client-streaming method."
     :$$: Text "Did you mean runStreamingClient?"

type family RunNormalClient (st :: StreamingType) :: Constraint where
  RunNormalClient 'NonStreaming = ()
  RunNormalClient st =
    TypeError (Text "Called 'runNormalClient' " :<>: RunClientError st)

type family RunStreamingClient (st :: StreamingType) :: Constraint where
  RunStreamingClient 'ClientStreaming = ()
  RunStreamingClient st =
    TypeError (Text "Called 'runStreamingClient' " :<>: RunClientError st)

type family RunServerStreamingClient (st :: StreamingType) :: Constraint where
  RunServerStreamingClient 'ServerStreaming  = ()
  RunServerStreamingClient st =
    TypeError (Text "Called 'runServerStreamingClient' " :<>: RunClientError st)

type family RunBiDiStreamingClient (st :: StreamingType) :: Constraint where
  RunBiDiStreamingClient 'BiDiStreaming  = ()
  RunBiDiStreamingClient st =
    TypeError (Text "Called 'runBiDiStreamingClient' " :<>: RunClientError st)


------------------------------------------------------------------------------
-- | Run a non-streaming RPC with customer with a custom set of
-- allowable 'StatusCode's.
runNormalClient'
    :: forall s m
     . ( HasMethod s m
       , RunNormalClient (MethodStreamingType s m)
       )
    => Client s
    -> WrappedMethodName m
    -> Int
    -> MethodInput s m
    -> [GL.StatusCode] -- A list of 'StatusCode's (in addition to 'StatusOk') for
                       -- which 'rspBody' will be decoded. If the status code is
                       -- not 'StatusOk' and not in this list, No `MethodOutput`
                       -- will be returned.
    -> IO (These (Nel.NonEmpty GRPCIOError) (MethodOutput s m))
runNormalClient' (Client client) _ timeout input cs = do
  clientRequest client
    (methodName @s @m)
    timeout
    (encodeMessage input)
    mempty
  >>= pure . \case
    Left err -> This (pure err)
    Right rsp
      | LL.rspCode rsp == GL.StatusOk ->
          either (This . pure) That (toMsg rsp)
      | LL.rspCode rsp `elem` cs ->
          either (This . (toErr rsp <|) . pure) (These . pure . toErr $ rsp) (toMsg rsp)
      | otherwise -> This . pure . toErr $ rsp
  where
    toMsg rsp = left packDecodeError . decodeMessage . rspBody $ rsp
    toErr rsp = GRPCIOBadStatusCode (LL.rspCode rsp) (LL.details rsp)


-- | Run a non-streaming RPC.
runNormalClient
    :: forall s m
     . ( HasMethod s m
       , RunNormalClient (MethodStreamingType s m)
       )
    => Client s
    -> WrappedMethodName m
    -> Int
    -> MethodInput s m
    -> IO (Either GRPCIOError (MethodOutput s m))
runNormalClient client method' timeout input = do
  runNormalClient' client method' timeout input []
  >>= pure . \case
    That b -> Right b
    This a -> Left (Nel.head a)
    These a _ -> Left (Nel.head a)


------------------------------------------------------------------------------
-- | Run a client-streaming RPC.
runStreamingClient
    :: forall s m r
     . ( HasMethod s m
       , RunStreamingClient (MethodStreamingType s m)
       )
    => Client s
    -> WrappedMethodName m
    -> Int
    -> ((MethodInput s m -> IO (Either GRPCIOError ())) -> IO r)
    -> IO (Either GRPCIOError (r, Maybe (MethodOutput s m)))
runStreamingClient (Client client) _ timeout f =
  withClientCall client
                 (methodName @s @m)
                 timeout $ \cc -> do
    ioref <- newIORef $ error "ioref was never set"
    x <- flip (clientWriterCmn client mempty) cc $ \send -> do
            r <- f (send . encodeMessage)
            writeIORef ioref r
    r <- readIORef ioref
    pure $ case x of
      Left err -> Left err
      Right (Nothing, _, _, _, _) -> Right (r, Nothing)
      Right (Just bs,  _, _, _, _) ->
        case decodeMessage bs of
          Left err -> Left $ packDecodeError err
          Right a -> Right (r, Just a)


------------------------------------------------------------------------------
-- | Run a server-streaming RPC.
runServerStreamingClient
    :: forall s m r
     . ( HasMethod s m
       , RunServerStreamingClient (MethodStreamingType s m)
       )
    => Client s
    -> WrappedMethodName m
    -> Int
    -> MethodInput s m
    -> (IO (Either GRPCIOError (Maybe (MethodOutput s m))) -> IO r)
    -> IO (Either GRPCIOError r)
runServerStreamingClient (Client client) _ timeout input f =
  withClientCall client
                 (methodName @s @m)
                 timeout $ \cc -> do
    ioref <- newIORef $ error "ioref was never set"
    x <- clientReader' client cc (encodeMessage input) mempty $ \_ recv -> do
            r <- f (fmap (toRecv @s @m) recv)
            writeIORef ioref r
    r <- readIORef ioref
    pure $ case x of
      Left err -> Left err
      Right (_, _, _) -> Right r



------------------------------------------------------------------------------
-- | Run a bidirectional streaming RPC.
runBiDiStreamingClient
    :: forall s m r
     . ( HasMethod s m
       , RunBiDiStreamingClient (MethodStreamingType s m)
       )
    => Client s
    -> WrappedMethodName m
    -> Int
    -> (  (MethodInput s m -> IO (Either GRPCIOError ()))     -- send
       -> IO (Either GRPCIOError (Maybe (MethodOutput s m)))  -- receive
       -> IO (Either GRPCIOError ())                          -- finish
       -> IO r)
    -> IO (Either GRPCIOError r)
runBiDiStreamingClient (Client client) _ timeout f =
  withClientCall client
                 (methodName @s @m)
                 timeout $ \cc -> do
    ioref <- newIORef $ error "ioref was never set"
    x <- clientRW' client cc mempty $ \_ recv send done -> do
            r <- f (send . encodeMessage) (fmap (toRecv @s @m) recv) done
            writeIORef ioref r
    r <- readIORef ioref
    pure $ r <$ x


------------------------------------------------------------------------------
-- | Convert the low-level recv into one that is 'Message' aware.
toRecv
    :: Message (MethodOutput s m)
    => Either GRPCIOError (Maybe ByteString)
    -> Either GRPCIOError (Maybe (MethodOutput s m))
toRecv (Left x) = Left x
toRecv (Right Nothing) = Right Nothing
toRecv (Right (Just bs)) =
  case decodeMessage bs of
    Left x  -> Left $ packDecodeError x
    Right y -> Right $ Just y


------------------------------------------------------------------------------
-- | A nicer interface to 'runServerImpl'.
runServer
    :: forall s t
     . ( GRPCServer s (ServiceMethods s)
       , RequireEveryHandler s t
       )
    => ServerOptions
    -> Record s t
    -> IO ()
runServer so r =
  runServerImpl @s
                @(ServiceMethods s)
    []
    so
    -- This is safe because 'RequireEveryHandler' will trip if this cast is
    -- invalid. We can't use a type-equality proof to do this, unfortunately,
    -- since that produces an error message before we get a chance to.
    (unsafeCoerce r :: ServiceRecord s)


------------------------------------------------------------------------------
-- | Typeclass which recurses on the service's methods to convert a
-- 'ServiceRecord' into a list of 'AnyHandler's.
class GRPCServer s ts where
  runServerImpl
      :: [AnyHandler]
      -> ServerOptions
      -> ServiceRecord s
      -> IO ()


------------------------------------------------------------------------------
-- | Base case: we have no more methods, so we can run our server.
instance GRPCServer s '[] where
  runServerImpl hs so _ = serverLoop so hs


------------------------------------------------------------------------------
-- | Induction case: Takes a 'ServiceHandler' of the correct type out of the
-- 'ServiceRecord', and existentially packs it into the handler list before
-- recursing.
instance ( GRPCServer s ms
         , HasMethod s m
         , IsServerHandler (MethodStreamingType s m)
                           (MethodInput s m)
                           (MethodOutput s m)
         , Lookup m (UnrollRecord s (ServiceMethods s)) ~
             'Just
                (ServerHandler (MethodStreamingType s m)
                               (MethodInput s m)
                               (MethodOutput s m))
         ) => GRPCServer s (m ': ms) where
  runServerImpl hs so r =
    runServerImpl @s @ms
      ( packHandler @(MethodStreamingType s m)
                    @(MethodInput s m)
                    @(MethodOutput s m)
          (methodName @s @m)
          (safeFind (WrappedMethodName @m) r) : hs)
      so
      r


------------------------------------------------------------------------------
-- | Better error messages for attempting to spawn a server missing some of its
-- handlers.
type family RequireEveryHandlerImpl (missing :: [v]) s t :: Constraint where
  RequireEveryHandlerImpl '[]     s t = UnrollConstraint s t (ServiceMethods s)
  RequireEveryHandlerImpl missing s t = TypeError
      ( Text "A call to 'runServer' for service '"
   :<>: ShowType s
   :<>: Text "' is missing handlers for the methods:"
   :$$: ShowList missing
      )

type RequireEveryHandler s t =
  RequireEveryHandlerImpl (AnyMissing (ServiceMethods s) t) s t


------------------------------------------------------------------------------
-- | Returns a list of values in 'ns' that are not present in 'hs'.
type family AnyMissing (ns :: [k]) (hs :: [(k, v)]) :: [k] where
  AnyMissing '[] hs = '[]
  AnyMissing (n ': ns) hs =
    InsertIfFalse (ListContains n hs) n (AnyMissing ns hs)


------------------------------------------------------------------------------
-- | Adds 'x' to 'xs' if @b ~ 'False@.
type family InsertIfFalse (b :: Bool) (x :: k) (xs :: [k]) :: [k] where
  InsertIfFalse 'False x xs = x ': xs
  InsertIfFalse 'True  x xs =      xs


------------------------------------------------------------------------------
-- | Returns ''True' iff 'n' is a key in 'hs'.
type family ListContains (n :: k) (hs :: [(k, v)]) :: Bool where
  ListContains n '[]             = 'False
  ListContains n ('(n, v) ': hs) = 'True
  ListContains n (x ': hs)       = ListContains n hs


------------------------------------------------------------------------------
-- | Pretty print a type-level list.
type family ShowList (ls :: [k]) :: ErrorMessage where
  ShowList '[]  = Text ""
  ShowList '[x] = ShowType x
  ShowList (x ': xs) = ShowType x :<>: Text ", " :<>: ShowList xs


------------------------------------------------------------------------------
-- | Class for describing the correct form of a handler for a method of input
-- 'a' and output 'b'. 'cs' and 'ss' refer to whether the method is client or
-- server streaming, respectively.
class IsServerHandler (st :: StreamingType) a b where
  type ServerHandler st a b :: *

  -- | Existentially pack a 'ServerHandler' into an 'AnyHandler', so that the
  -- server can refer to them in a type-safe way.
  packHandler
      :: (Message a, Message b)
      => MethodName
      -> ServerHandler st a b
      -> AnyHandler


------------------------------------------------------------------------------
-- | A non-streaming handler is morally just a function @a -> IO b@.
instance IsServerHandler 'NonStreaming a b where
  type ServerHandler 'NonStreaming a b =
    ServerCall a -> IO (MetadataMap, Response b)
  packHandler = (AnyHandler .) . UnaryHandler


------------------------------------------------------------------------------
-- | LL.Client streaming.
instance IsServerHandler 'ClientStreaming a b where
  type ServerHandler 'ClientStreaming a b
      = ServerCall ()
     -> IO (Either GRPCIOError (Maybe a))
     -> IO (MetadataMap, Response (Maybe b))
  packHandler = (AnyHandler .) . ClientStreamHandler


------------------------------------------------------------------------------
-- | Server streaming.
instance IsServerHandler 'ServerStreaming a b where
  type ServerHandler 'ServerStreaming a b
      = ServerCall a
     -> (b -> IO (Either GRPCIOError ()))
     -> IO (MetadataMap, Response ())
  packHandler = (AnyHandler .) . ServerStreamHandler


------------------------------------------------------------------------------
-- | BiDi streaming.
instance IsServerHandler 'BiDiStreaming a b where
  type ServerHandler 'BiDiStreaming a b
      = ServerCall ()
     -> IO (Either GRPCIOError (Maybe a))
     -> (b -> IO (Either GRPCIOError ()))
     -> IO (MetadataMap, Response ())
  packHandler = (AnyHandler .) . BiDiStreamHandler


------------------------------------------------------------------------------
-- | A 'Record' that has a 'ServerHandler' of the correct type for every method
-- in service 's'.
type ServiceRecord s = Record s (UnrollRecord s (ServiceMethods s))


------------------------------------------------------------------------------
-- | Constructs the correct type-level book-keeping for a 'Record' containing a
-- handler of the correct type for every method in service 's'.
type family UnrollRecord s (ts :: [Symbol]) :: [(Symbol, *)] where
  UnrollRecord s '[] = '[]
  UnrollRecord s (m ': ms) =
    '( m
     , (ServerHandler (MethodStreamingType s m)
                     (MethodInput s m)
                     (MethodOutput s m))
     ) ': UnrollRecord s ms


------------------------------------------------------------------------------
-- | Constructs a constraint ensuring that every handler in the 'Record' type
-- 'r' has the correct type according to its 'ServiceHandler'.
type family UnrollConstraint s r (ts :: [Symbol]) :: Constraint where
  UnrollConstraint s r '[] = ()
  UnrollConstraint s r (m ': ms) =
    ( Lookup m r ~ 'Just
        (ServerHandler (MethodStreamingType s m)
                       (MethodInput s m)
                       (MethodOutput s m))
    , UnrollConstraint s r ms
    )

------------------------------------------------------------------------------
-- | Helper function to aid in translating GRPC results from protos to domain
-- types
protoToDomainType :: (Traversal' a b)
                  -> Either LL.GRPCIOError a
                  -> Either LL.GRPCIOError b
protoToDomainType optic eitherA =
  case preview optic <$> eitherA of
    Right Nothing  -> Left $ LL.GRPCIODecodeError "Error decoding to domain type"
    Right (Just b) -> Right b
    Left err -> Left err

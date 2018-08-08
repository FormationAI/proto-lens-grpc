{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
module Platform.GRPC.Client (
  -- * Client implementation
    MethodType (..)
  , method
  , methodWithHandlers
  , methodProto
  , methodProtoWithHandlers
  -- * Soft errors
  -- $domainerrors
  -- ** Handling status codes
  , ErrorHandler (..)
  , constHandler
  , decodeHandler
  , decodeMapHandler
  , emptyHandler
  , statusHandler
  , statusDetailsHandler
  , statusDetailsHandler1
  -- ** Handling multiple failure cases
  , HandlerMap (..)
  , mkHandlerMap
  , emptyHandlerMap
  , lookupHandler
  , handleErrors
  -- * Hard errors
  , ServiceUnavailable (..)
  , ResponseFailedValidation (..)
  , HandlerError (..)
  -- * Retries
  -- $retry
  , ShouldRetry (..)
  , retryBool
  , shouldRetry
  , shouldRetryCode
  ) where


import           Control.Exception (Exception, handle, throwIO)
import           Control.Lens ((^.))
import           Control.Monad.Catch (Handler (..))
import           Control.Retry (RetryPolicyM)
import qualified Control.Retry as Retry

import           Data.Bifunctor (first)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.ProtoLens (Message, decodeMessage, messageName)
import qualified Data.ProtoLens.Any as Any
import           Data.Proxy (Proxy (..))
import           Data.Text (Text)
import           Data.Traversable (for)
import           Data.Void (Void, absurd)

import qualified Network.GRPC.LowLevel as G

import           Lens.Labels.Unwrapped ()

import           Proto.Google.Protobuf.Empty (Empty)
import qualified Proto.Google.Rpc.Status as P


-- | A sum type indicating whether a method is idempotent or not. This
-- is used in 'method' et al to determine retry semantics.
data MethodType =
    Idempotent
  -- ^ The method is idempotent, e.g. sending the same request twice
  --   has the same effect as sending it just the once.
  --   Marking a method as 'Idempotent' means clients will retry
  --   failed requests in many more situations.
  | Unsafe
  -- ^ The method is non-idempotent. Sending the same request twice
  --   may have undefined behaviour.
  deriving (Eq, Ord, Show)


-- | An 'Exception' thrown when a retryable error occurs and retries
-- are exhausted.
--
-- Servers contacting other services should catch this and return
-- 'G.StatusUnavailable'.
data ServiceUnavailable =
    ServiceUnavailable
  deriving (Eq, Ord, Show)

instance Exception ServiceUnavailable


-- | An 'Exception' thrown when a response fails client-side validation.
--
-- This error is not considered recoverable.
data ResponseFailedValidation =
    ResponseFailedValidation
  deriving (Eq, Ord, Show)

instance Exception ResponseFailedValidation


-- | An 'Exception' thrown when a domain error is not decoded properly.
-- This can only happen if client and server are misaligned and sending
-- the wrong proto message.
--
-- This error indicates a broken invariant as is not considered
-- recoverable.
data HandlerError =
  -- | The error string produced by 'Data.ProtoLens.decodeMessage',
  -- plus the name of the proto message we expected.
    HandlerFailedDecode
      Text
      String

  -- | The error message produced by 'Data.ProtoLens.Any.unpack'.
  | AnyFailedUnpack
      Any.UnpackError

  -- | A 'P.Status' proto was unexpectedly missing its 'G.StatusDetails'.
  | HandlerMissingDetails

  -- | A user-provided /error validation/ function failed. This means
  -- a domain error handler did not succeed. Likely programmer error.
  | HandlerFailedValidation
  deriving (Eq, Show)

instance Exception HandlerError


-- | Shorthand for constructing a well-behaved GRPC client using
-- domain types and validation (but /not/ using domain errors.)
--
-- See 'methodWithHandlers' for more information.
method ::
     MethodType
  -> RetryPolicyM IO
  -> (request -> IO (Either G.GRPCIOError response))
  -> (a -> request)
  -> (response -> Either invalid b)
  -> (a -> IO b)
method mtype policy call encode decode =
  fmap (either absurd id) . methodWithHandlers
    mtype
    policy
    call
    encode
    emptyHandlerMap
    decode


-- | Shorthand for constructing a well-behaved GRPC client using
-- domain types, domain errors, and validation. Enforces standardised retry
-- semantics and error handling.
--
-- For clients that do not handle any non-OK status codes (i.e. no
-- domain errors are possible), use 'method'.
--
-- For simple clients that do not use domain types, see 'methodProto'.
-- All GRPC client libraries should use 'methodWithHandlers', 'method',
-- or 'methodProto' barring exceptional circumstances.
--
-- >>> :{
-- let myRetryPolicy = fullJitterBackoff backoff <> limitRetries numRetries
--     call label = G.runNormalClient client label timeout
-- in  method Idempotent myRetryPolicy (call #myMethod) encode decode
-- :}
methodWithHandlers ::
     MethodType
  -> RetryPolicyM IO
  -> (request -> IO (Either G.GRPCIOError response))
  -- ^ Function to make the call, typically a closure around 'G.runNormalClient'.
  -> (a -> request)
  -- ^ Function to convert the domain request type into the message proto.
  -> HandlerMap error
  -- ^ Set of handlers for soft / domain errors.
  -> (response -> Either invalid b)
  -- ^ Validation function for the response.
  -> (a -> IO (Either error b))
methodWithHandlers mtype policy call encode handlers decode arg =
  withRetries mtype policy $ do
    -- Make the remote call.
    rsp <- call (encode arg)
    case rsp of
      Left grpcError ->
        -- Something went wrong.
        case grpcError of
          G.GRPCIOBadStatusCode code details ->
            case handleErrors handlers code details of
              Just (Right domainError) ->
                -- A recoverable domain error was decoded.
                -- Return it to the user.
                pure (Left domainError)

              Just (Left handlerError) ->
                -- A recoverable domain error was not decoded properly.
                -- This is a broken invariant. Throw.
                throwIO handlerError

              Nothing ->
                -- The status code was unhandled. This is not a domain error.
                -- Rethrow the GRPC error.
                throwIO grpcError

          _ ->
            -- The error is out-of-band.
            -- Rethrow the GRPC library error.
            throwIO grpcError

      Right proto ->
        -- Looks OK so far.
        case decode proto of
          Left _validationError ->
            -- We don't understand the response.
            -- This is a broken invariant. Throw.
            -- TODO: Enrich exception with a bit more information.
            throwIO ResponseFailedValidation

          Right val ->
            -- We understand the response.
            -- Return it to the user.
            pure (Right val)

-- | Like 'method', but operating directly on protos.
methodProto ::
     MethodType
  -> RetryPolicyM IO
  -> (request -> IO (Either G.GRPCIOError response))
  -> (request -> IO response)
methodProto mtype policy call =
  method
    mtype
    policy
    call
    id
    pure

-- | Like 'methodWithHandlers', but operating directly on protos.
methodProtoWithHandlers ::
     MethodType
  -> RetryPolicyM IO
  -> (request -> IO (Either G.GRPCIOError response))
  -> HandlerMap error
  -> (request -> IO (Either error response))
methodProtoWithHandlers mtype policy call handlers =
  methodWithHandlers
    mtype
    policy
    call
    id
    handlers
    pure

-- | A standardised retry handler for use in GRPC client libraries.
--
-- In general, users should avoid calling this directly and instead
-- use 'method' or 'methodProto', which take care of retries for you.
withRetries :: MethodType -> RetryPolicyM IO -> IO r -> IO r
withRetries mtype policy k =
  let
    handler :: Retry.RetryStatus -> Handler IO Bool
    handler _retryState = Handler (pure . retryBool . shouldRetry mtype)

    unavailable e =
      case shouldRetry mtype e of
        Retry ->
          -- Propagate the retry to a higher level.
          throwIO ServiceUnavailable
        Stop ->
          -- Rethrow the GRPC error (hard failure).
          throwIO e
  in
    handle unavailable $
      Retry.recovering policy [handler] (\_retryState -> k)


-- -----------------------------------------------------------------------------
-- Handling domain errors

-- $domainerrors
--
-- By /domain errors/ we mean errors that:
--
-- * are plausibly /recoverable/
--
-- * Are represented by some combination of non-OK GRPC status codes and
--   structured proto errors smuggled in the `G.StatusDetails` field
--
-- * can be represented in the abstract interface without mention of GRPC
--
-- Our domain error strategy revolves around the 'ErrorHandler' type,
-- which encodes a relationship between a GRPC status code, a proto
-- message type, and a domain error. It is assumed each method can
-- return only one error type. Likewise, there must be a one-to-one
-- relationship between status codes and proto messages for each
-- method. See 'ErrorHandler' for more information.
--
-- Programmers should exercise due diligence when lining up error
-- types. There is no typed relationship keeping clients and servers
-- aligned, only tests.


-- | A map from 'G.StatusCode' to 'ErrorHandler'.
--
-- Construct with 'mkHandlerMap' or 'emptyHandlerMap'.
-- Consume with 'lookupHandler' or 'handleErrors'.
--
-- Typically this map is passed into 'method' or 'methodProto'.
newtype HandlerMap e = HandlerMap {
    unHandlerMap :: Map Int (ErrorHandler e)
  } deriving (Functor)

-- | Construct an empty 'HandlerMap'.
emptyHandlerMap :: HandlerMap Void
emptyHandlerMap =
  HandlerMap mempty

-- | Construct a 'HandlerMap' from an association list.
mkHandlerMap :: [(G.StatusCode, ErrorHandler e)] -> HandlerMap e
mkHandlerMap =
  HandlerMap . M.fromList . fmap (first fromEnum)

-- | Find an 'ErrorHandler' in a 'HandlerMap' by its 'G.StatusCode'.
lookupHandler :: G.StatusCode -> HandlerMap e -> Maybe (ErrorHandler e)
lookupHandler code =
  M.lookup (fromEnum code) . unHandlerMap

-- | Given a 'G.StatusCode' and 'G.StatusDetails', find a matching
-- handler if one exists, and attempt to run it.
handleErrors ::
     HandlerMap e
  -> G.StatusCode
  -> G.StatusDetails
  -> Maybe (Either HandlerError e)
handleErrors handlers code details =
  fmap (decodeDetails details) (lookupHandler code handlers)

decodeDetails ::
     G.StatusDetails
  -> ErrorHandler e
  -> Either HandlerError e
decodeDetails details (ErrorHandler f) =
  f details

-- | Handle a structured proto error from the 'G.StatusDetails' field of a
-- GRPC response.
--
-- It is expected there be a direct total mapping from the given proto
-- to the domain error. If this turns out to be too restrictive, this
-- may be relaxed in the future in favour of another exception.
data ErrorHandler e = ErrorHandler {
    runHandler :: G.StatusDetails -> Either HandlerError e
  }

instance Functor ErrorHandler where
  fmap f (ErrorHandler g) = ErrorHandler (fmap f . g)


decodeHandler ::
     forall proto e. Message proto
  => (proto -> Either HandlerError e)
  -> ErrorHandler e
decodeHandler f =
  ErrorHandler $ \details -> do
    proto <- first (HandlerFailedDecode expectedName) $
      decodeMessage (G.unStatusDetails details)
    f proto
  where
    expectedName = messageName (Proxy @proto)

-- | An 'fmap'-like handler that can't fail.
decodeMapHandler :: Message proto => (proto -> e) -> ErrorHandler e
decodeMapHandler f =
  decodeHandler (pure . f)

-- | A constant handler that doesn't look at the details field at all.
constHandler :: e -> ErrorHandler e
constHandler e =
  ErrorHandler $ \_ -> pure e

-- | A constant handler that expects the details field to be 'Empty'.
-- This is much more brittle than 'constHandler'.
emptyHandler :: e -> ErrorHandler e
emptyHandler e =
  decodeHandler (\(_ :: Empty) -> pure e)

-- | A handler that directly inspects the 'P.Status' message.
statusHandler :: (P.Status -> e) -> ErrorHandler e
statusHandler = decodeMapHandler

-- | A handler that decodes and interprets each item in the details
-- field of the 'P.Status' message.
statusDetailsHandler :: Message proto => (proto -> Either x e) -> ErrorHandler [e]
statusDetailsHandler f =
  decodeHandler $ \(s :: P.Status) ->
    for (s ^. #details) $ \x -> do
      y <- first AnyFailedUnpack (Any.unpack x)
      first (const HandlerFailedValidation) $ f y
--    traverse (either AnyFailedUnpack f . Any.unpack) (s ^. #details)

-- | Like 'statusDetailsHandler', but handling only the first error.
statusDetailsHandler1 :: Message proto => (proto -> Either x e) -> ErrorHandler e
statusDetailsHandler1 f =
  decodeHandler $ \(s :: P.Status) ->
    case s ^. #details of
      [] ->
        Left HandlerMissingDetails
      x:_ -> do
        y <- first AnyFailedUnpack (Any.unpack x)
        first (const HandlerFailedValidation) $ f y

-- -----------------------------------------------------------------------------
-- Retrying operations

-- $retry
--
-- This module contains the retry handlers used by all platform GRPC clients.
-- It also aims to serve as a status code reference for service implementors.
--
-- Our treatment of each status code is a little less well specified, but at
-- the very least we have defined the systemwide behaviour in one place.
-- This is currently somewhat conservative and may be adjusted in the future.
--
-- = GRPC Status Reference
--
-- This section is broken down into five sections per status code:
--
-- - /Meaning/, i.e. a one-sentence description of the code
--
-- - /Generated by/, i.e. whether the code is produced by us or the GRPC
--    library (on client side or server side). Often the answer is "all three",
--    but several codes are reserved for user code and never generated by the library.
--
-- - /Usage/, i.e. when you might choose to use this code over another.
--
-- - /HTTP equivalent/, i.e. the equivalent HTTP response code. This is not
--   always a precise mapping, but may be useful when building intuition.
--
-- - /Retried/, i.e. how platform services react to this code.
--
--
-- The meaning and usage information for each code is taken from the
-- [GRPC source code](https://github.com/grpc/grpc/blob/39604e410d7d626777bec17e5d94b13798e08dbb/include/grpcpp/impl/codegen/status_code_enum.h).
-- It is supplemented in places from usage information in the
-- [GRPC protos](https://github.com/googleapis/googleapis/blob/6a3277c0656219174ff7c345f31fb20a90b30b97/google/rpc/code.proto).
-- The GRPC library information is taken from the
-- [GRPC wiki](https://github.com/grpc/grpc/blob/master/doc/statuscodes.md).
-- The retry semantics are specific to our team and this library, though
-- they are informed by the above documents.
--
--
-- * 'G.StatusOk'
--
--     * __Meaning__:
--       Not an error; returned on success.
--
--     * __Generated by__:
--       User code.
--
--     * __Usage__:
--       Return 'G.StatusOk' whenever a request succeeds without errors.
--
--     * __HTTP equivalent__:
--       200 OK
--
--     * __Retried__:
--       No. The request succeeded.
--
-- * 'G.StatusCancelled'
--
--     * __Meaning__:
--       The operation was cancelled (typically by the caller).
--
--     * __Generated by__:
--       GRPC (both client- and server-side).
--
--     * __Usage__:
--       Generated automatically by the library when a request is cancelled.
--
--     * __HTTP equivalent__:
--       499 Client Closed Request
--
--     * __Retried__:
--       No. The cancellation is assumed to have been intentional.
--
-- * 'G.StatusUnknown'
--
--     * __Meaning__:
--       Unknown error.
--
--     * __Generated by__:
--       User code (on exception) and GRPC (both client- and server-side).
--       'G.StatusUnknown' may be returned if a server-side handler throws an
--       exception. It may also be raised if the GRPC library fails to parse a
--       returned status code.
--
--     * __Usage__:
--       An example of where this error may be returned is if a
--       Status value received from another address space belongs to an error-space
--       that is not known in this address space. Also errors raised by APIs that
--       do not return enough error information may be converted to this error.
--
--     * __HTTP equivalent__:
--       500 Internal Server Error
--
--     * __Retried__:
--       No. We do not know enough about the failure to reason about it.
--
-- * 'G.StatusInvalidArgument'
--
--     * __Meaning__:
--       Client specified an invalid argument.
--
--     * __Generated by__:
--       User code.
--
--     * __Usage__:
--       Note that this differs from
--       'G.StatusFailedPrecondition'. 'G.StatusInvalidArgument' indicates arguments
--       that are problematic regardless of the state of the system
--       (e.g., a malformed filename).
--
--     * __HTTP equivalent__:
--       400 Bad Request
--
--     * __Retried__:
--       No. This typically indicates a mistake on the
--       client side that cannot be fixed by retrying. In our
--       architecture, this probably indicates a deployment failure
--       state on either client or server.
--
-- * 'G.StatusDeadlineExceeded'
--
--     * __Meaning__:
--       Deadline expired before operation could complete.
--
--     * __Generated by__:
--       GRPC (both client- and server-side).
--
--     * __Usage__:
--       This may occur either when the client is unable to send the request to the
--       server or when the server fails to respond in time.
--       For operations that change the state of the
--       system, this error may be returned even if the operation has
--       completed successfully. For example, a successful response
--       from a server could have been delayed long enough for the
--       deadline to expire.
--
--     * __HTTP equivalent__:
--       504 Gateway Timeout
--
--     * __Retried__:
--       Only for idempotent operations.
--
-- * 'G.StatusNotFound'
--
--     * __Meaning__:
--       Some requested entity (e.g., file or directory) was not found.
--
--     * __Generated by__:
--       User code.
--
--     * __Usage__:
--       If the request refers to a single entity that is missing, 'G.StatusNotFound'
--       is appropriate. In other cases, consider 'G.StatusFailedPrecondition'.
--       Note to server developers: if a request is denied for an entire class
--       of users, such as gradual feature rollout or undocumented whitelist,
--       'G.StatusNotFound' may be used. If a request is denied for some users within
--       a class of users, such as user-based access control, 'G.StatusPermissionDenied'
--       must be used.
--
--     * __HTTP equivalent__:
--       404 Not Found
--
--     * __Retried__:
--       No. This is a domain error. User code is expected to retry explicitly if it needs to.
--
-- * 'G.StatusAlreadyExists'
--
--     * __Meaning__:
--       Some entity that we attempted to create (e.g., file or directory) already
--       exists.
--
--     * __Generated by__:
--       User code.
--
--     * __Usage__:
--       Use 'G.StatusAlreadyExists' in the case of a conflict against the request's
--       direct referent. Like with 'G.StatusNotFound', consider 'G.StatusFailedPrecondition'
--       for more complex preconditions.
--
--     * __HTTP equivalent__:
--       409 Conflict
--
--     * __Retried__:
--       No. This is a failed precondition. The user should correct it and retry
--       explicitly if necessary.
--
-- * 'G.StatusPermissionDenied'
--
--     * __Meaning__:
--       The caller does not have permission to execute the specified
--       operation.
--
--     * __Generated by__:
--       User code.
--
--     * __Usage__:
--       'G.StatusPermissionDenied' must not be used for rejections
--       caused by exhausting some resource (use 'G.StatusResourceExhausted'
--       instead for those errors). 'G.StatusPermissionDenied' must not be
--       used if the caller can not be identified (use 'G.StatusUnauthenticated'
--       instead for those errors). This error code does not imply the
--       request is valid or the requested entity exists or satisfies
--       other pre-conditions.
--
--     * __HTTP equivalent__:
--       403 Forbidden
--
--     * __Retried__:
--       No. The user should fix their credentials and retry explicitly.
--
-- * 'G.StatusUnauthenticated'
--
--     * __Meaning__:
--       The request does not have valid authentication credentials for the
--       operation.
--
--     * __Generated by__:
--       GRPC (client- and server-side). Can be caused by missing / invalid
--       credentials, invalid @:authority@ header, etc.
--
--     * __Usage__:
--       Generated by GRPC when server-side authentication is in use.
--
--     * __HTTP equivalent__:
--       401 Unauthorized
--
--     * __Retried__:
--       No. User should self-correct and retry explicitly.
--
-- * 'G.StatusResourceExhausted'
--
--     * __Meaning__:
--       Some resource has been exhausted.
--
--     * __Generated by__:
--       User code and GRPC (client- and server-side).
--
--     * __Usage__:
--       Use 'G.StatusResourceExhausted' to signal local availability problems.
--       It is also generated by the GRPC library on both client and server in
--       the cases of memory exhaustion or resource limitations.
--
--     * __HTTP equivalent__:
--       429 Too Many Requests
--
--     * __Retried__:
--       Yes.
--
-- * 'G.StatusFailedPrecondition'
--
--     * __Meaning__:
--       The operation was rejected because the system is not in a state
--       required for the operation's execution.
--
--     * __Generated by__:
--       User code.
--
--     * __Usage__:
--       Service implementors can use the following guidelines to decide
--       between 'G.StatusFailedPrecondition', 'G.StatusAborted', and
--       'G.StatusUnavailable'.
--
--        * Use 'G.StatusUnavailable' if the client can retry just the failing call.
--
--        * Use 'G.StatusAborted' if the client should retry at a higher level
--          (e.g., when a client-specified test-and-set fails, indicating the
--          client should restart a read-modify-write sequence).
--
--        * Use 'G.StatusFailedPrecondition' if the client should not retry until
--          the system state has been explicitly fixed.  E.g., if an "rmdir"
--          fails because the directory is non-empty, 'G.StatusFailedPrecondition'
--          should be returned since the client should not retry unless
--          the files are deleted from the directory.
--
--     * __HTTP equivalent__:
--       400 Bad Request
--
--     * __Retried__:
--       No. The client should not retry until the system state has been
--       explicitly fixed.
--
-- * 'G.StatusAborted'
--
--     * __Meaning__:
--       The operation was aborted, typically due to a concurrency issue such as
--       a sequencer check failure or transaction abort.
--
--     * __Generated by__:
--       User code.
--
--     * __Usage__:
--       See litmus test above for deciding between FAILED_PRECONDITION, ABORTED,
--       and UNAVAILABLE.
--
--     * __HTTP equivalent__:
--       409 Conflict
--
--     * __Retried__:
--       No. The client should retry at a higher level. See litmus test above.
--
-- * 'G.StatusOutOfRange'
--
--     * __Meaning__:
--       The operation was attempted past the valid range.  E.g., seeking or
--       reading past end-of-file.
--
--     * __Generated by__:
--       User code.
--
--     * __Usage__:
--       Unlike 'G.StatusInvalidArgument', this error
--       indicates a problem that may be fixed if the system state
--       changes. For example, a 32-bit file system will generate
--       'G.StatusInvalidArgument' if asked to read at an offset that
--       is not in the range [0,2^32-1], but it will generate
--       'G.StatusOutOfRange' if asked to read from an offset past the
--       current file size. There is a fair bit of overlap between
--       'G.StatusFailedPrecondition' and 'G.StatusOutOfRange'. We
--       recommend using 'G.StatusOutOfRange' (the more specific
--       error) when it applies so that callers who are iterating
--       through a space can easily look for an 'G.StatusOutOfRange'
--       error to detect when they are done.
--
--     * __HTTP equivalent__:
--       400 Bad Request
--
--     * __Retried__:
--       No. The user should retry explicitly.
--
-- * 'G.StatusUnimplemented'
--
--     * __Meaning__:
--       The operation is not implemented or is not supported/enabled in this
--       service.
--
--     * __Generated by__:
--       User code and GRPC (client- and server-side).
--
--     * __Usage__:
--       User code may produce 'G.StatusUnimplemented' for methods that are
--       defined but not complete. In general we try not to do this in production.
--       GRPC may produce this status code when the requested method is not
--       found on the server, compression methods are unsupported, or protocol
--       invariants are broken.
--
--     * __HTTP equivalent__:
--       501 Not Implemented
--
--     * __Retried__:
--       No. This is not a transient condition.
--
-- * 'G.StatusInternal'
--
--     * __Meaning__:
--       Internal errors.  This means that some invariants expected by the
--       underlying system have been broken.  This error code is reserved
--       for serious errors.
--
--     * __Generated by__:
--       User code, GRPC (client- and server-side).
--
--     * __Usage__:
--       Users may produce 'G.StatusInternal' for serious errors that do not
--       fit under any other status code.
--       GRPC may produce this status code when decompression or proto decode
--       fails.
--
--     * __HTTP equivalent__:
--       500 Internal Server Error
--
--     * __Retried__:
--       No. This is considered a hard failure. This choice may be adjusted
--       at some point.
--
-- * 'G.StatusUnavailable'
--
--     * __Meaning__:
--       The service is currently unavailable.  This is most likely a
--       transient condition, which can be corrected by retrying with
--       a backoff.
--
--     * __Generated by__:
--       User code, GRPC (client- and server-side).
--
--     * __Usage__:
--       See the above guidelines for deciding between 'G.StatusFailedPrecondition',
--       'G.StatusAborted', and 'G.StatusUnavailable'.
--       GRPC may generate 'G.StatusUnavailable' in the case of a failed network request
--       or if the server has initiated shutdown.
--       Although data MIGHT not have been transmitted when this
--       status occurs, there is NOT A GUARANTEE that the server has not seen
--       anything. So in general it is unsafe to retry on this status code
--       if the call is non-idempotent.
--
--     * __HTTP equivalent__:
--       503 Service Unavailable
--
--     * __Retried__:
--       Only for idempotent methods. This is a transient failure that will often
--       self-correct, but we cannot assume the request had no effect.
--
-- * 'G.StatusDataLoss'
--
--     * __Meaning__:
--       Unrecoverable data loss or corruption.
--
--     * __Generated by__:
--       User code.
--
--     * __Usage__:
--       Use 'G.StatusDataLoss' for data loss conditions severe enough to page
--       a sleeping operator. i.e. very rarely.
--
--     * __HTTP equivalent__:
--       500 Internal Server Error
--
--     * __Retried__:
--       No. This is a very hard alertable failure. The underlying system is
--       assumed to be completely unusable.


data ShouldRetry =
    Retry
  | Stop
  deriving (Eq, Ord, Show)

-- | Convert the 'ShouldRetry' type to a 'Bool' suitable for use with
-- 'Control.Retry.recovering'.
retryBool :: ShouldRetry -> Bool
retryBool = \case
  Retry -> True
  Stop  -> False

-- | A predicate to determine whether or not a failed request should
-- be retried based on its 'MethodType' and 'G.GRPCIOError'.
--
-- This handles both bad status codes and out-of-band library errors.
-- If we received a status code other than 'StatusOk', this calls
-- the relevant 'shouldRetryCode' function.
shouldRetry :: MethodType -> G.GRPCIOError -> ShouldRetry
shouldRetry mtype err =
  case mtype of
    Idempotent ->
      shouldRetryIdempotent err
    Unsafe ->
      shouldRetryUnsafe err

-- | A predicate to determine whether or not a failed request should
-- be retried based on its 'G.GRPCIOError'.
--
-- This function is intended for unsafe (i.e. non-idempotent) methods
-- and is thus quite conservative.
shouldRetryIdempotent :: G.GRPCIOError -> ShouldRetry
shouldRetryIdempotent = \case
  G.GRPCIOBadStatusCode code _deets   -> shouldRetryCodeIdempotent code
  G.GRPCIOTimeout                     -> Retry
  G.GRPCIOShutdown                    -> Retry
  G.GRPCIOCallError _ce               -> Stop
  G.GRPCIOShutdownFailure             -> Stop
  G.GRPCIOUnknownError                -> Stop
  G.GRPCIODecodeError _str            -> Stop
  G.GRPCIOInternalUnexpectedRecv _str -> Stop
  G.GRPCIOHandlerException _str       -> Stop


-- | A predicate to determine whether or not a failed request should
-- be retried based on its 'G.GRPCIOError'.
--
-- This function is intended for unsafe (i.e. non-idempotent) methods
-- and is thus quite conservative.
shouldRetryUnsafe :: G.GRPCIOError -> ShouldRetry
shouldRetryUnsafe = \case
  G.GRPCIOBadStatusCode code _deets   -> shouldRetryCodeUnsafe code
  G.GRPCIOTimeout                     -> Stop
  G.GRPCIOShutdown                    -> Stop
  G.GRPCIOCallError _ce               -> Stop
  G.GRPCIOShutdownFailure             -> Stop
  G.GRPCIOUnknownError                -> Stop
  G.GRPCIODecodeError _str            -> Stop
  G.GRPCIOInternalUnexpectedRecv _str -> Stop
  G.GRPCIOHandlerException _str       -> Stop


-- | A predicate to determine whether or not a failed request should
-- be retried based on its 'MethodType' and 'G.StatusCode'.
shouldRetryCode :: MethodType -> G.StatusCode -> ShouldRetry
shouldRetryCode mtype code =
  case mtype of
    Idempotent ->
      shouldRetryCodeIdempotent code
    Unsafe ->
      shouldRetryCodeUnsafe code

-- | A predicate to determine whether or not a failed request should
-- be retried based on its 'G.StatusCode'.
--
-- This function is intended for idempotent methods, and thus suggests
-- retrying in a fairly wide range of scenarios. Loosely speaking, it
-- aims to retry on failures that appear transient.
shouldRetryCodeIdempotent :: G.StatusCode -> ShouldRetry
shouldRetryCodeIdempotent = \case
  G.StatusUnavailable        -> Retry
  G.StatusResourceExhausted  -> Retry
  G.StatusDeadlineExceeded   -> Retry
  G.StatusInternal           -> Stop
  G.StatusDoNotUse           -> Stop
  G.StatusOk                 -> Stop
  G.StatusCancelled          -> Stop
  G.StatusUnknown            -> Stop
  G.StatusInvalidArgument    -> Stop
  G.StatusNotFound           -> Stop
  G.StatusAlreadyExists      -> Stop
  G.StatusPermissionDenied   -> Stop
  G.StatusFailedPrecondition -> Stop
  G.StatusAborted            -> Stop
  G.StatusOutOfRange         -> Stop
  G.StatusUnimplemented      -> Stop
  G.StatusDataLoss           -> Stop
  G.StatusUnauthenticated    -> Stop

-- | A predicate to determine whether or not a failed request should
-- be retried based on its 'G.StatusCode'.
--
-- This function is intended for unsafe (i.e. non-idempotent) methods
-- and is thus quite conservative. It could probably be made even more
-- conservative.
shouldRetryCodeUnsafe :: G.StatusCode -> ShouldRetry
shouldRetryCodeUnsafe = \case
  G.StatusResourceExhausted  -> Retry
  G.StatusUnavailable        -> Stop
  G.StatusDeadlineExceeded   -> Stop
  G.StatusDoNotUse           -> Stop
  G.StatusOk                 -> Stop
  G.StatusCancelled          -> Stop
  G.StatusUnknown            -> Stop
  G.StatusInvalidArgument    -> Stop
  G.StatusNotFound           -> Stop
  G.StatusAlreadyExists      -> Stop
  G.StatusPermissionDenied   -> Stop
  G.StatusFailedPrecondition -> Stop
  G.StatusAborted            -> Stop
  G.StatusOutOfRange         -> Stop
  G.StatusUnimplemented      -> Stop
  G.StatusInternal           -> Stop
  G.StatusDataLoss           -> Stop
  G.StatusUnauthenticated    -> Stop

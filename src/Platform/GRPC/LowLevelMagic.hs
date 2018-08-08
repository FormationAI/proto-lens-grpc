{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms   #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}
{-# LANGUAGE ViewPatterns      #-}

-- | This module defines data structures and operations pertaining to registered
-- clients using registered calls; for unregistered support, see
-- `Network.GRPC.LowLevel.Client.Unregistered`.
module Platform.GRPC.LowLevelMagic where

import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Except
import           Data.ByteString                       (ByteString)
import           Network.GRPC.LowLevel.Call
import           Network.GRPC.LowLevel.GRPC
import           Network.GRPC.LowLevel.Op
import           Network.GRPC.LowLevel.Client

--------------------------------------------------------------------------------
-- clientReader (client side of server streaming mode)

clientReader'
    :: Client
    -> ClientCall
    -> ByteString -- ^ The body of the request
    -> MetadataMap -- ^ Metadata to send with the request
    -> ClientReaderHandler
    -> IO (Either GRPCIOError ClientReaderResult)
clientReader' Client{ clientCQ = cq } (unsafeCC -> c) body initMeta f =
    runExceptT $ do
      void $ runOps' c cq [ OpSendInitialMetadata initMeta
                          , OpSendMessage body
                          , OpSendCloseFromClient
                          ]
      srvMD <- recvInitialMetadata c cq
      liftIO $ f srvMD (streamRecvPrim c cq)
      recvStatusOnClient c cq


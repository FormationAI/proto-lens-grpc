{-# LANGUAGE DataKinds                          #-}
{-# LANGUAGE FlexibleInstances                  #-}
{-# LANGUAGE GADTs                              #-}
{-# LANGUAGE MultiParamTypeClasses              #-}
{-# LANGUAGE OverloadedLabels                   #-}
{-# LANGUAGE PolyKinds                          #-}
{-# LANGUAGE RankNTypes                         #-}
{-# LANGUAGE ScopedTypeVariables                #-}
{-# LANGUAGE TypeApplications                   #-}
{-# LANGUAGE TypeFamilies                       #-}
{-# LANGUAGE TypeOperators                      #-}
{-# OPTIONS_GHC -fno-warn-redundant-constraints #-} -- Getter requires this

module Data.ProtoLens.GRPC.Data.ExtensibleRecord
  ( Record ()
  , safeFind
  , method
  , getMethod
  , service
  , WrappedMethodName (..)
  , Lookup
  ) where

import Control.Lens hiding ((:>))
import Data.ProtoLens.Service.Types
import GHC.OverloadedLabels
import GHC.TypeLits
import Unsafe.Coerce (unsafeCoerce)


------------------------------------------------------------------------------
-- | Data structure intended to be used to say that 's' has a method 'sym' whose
-- handler has type 'b'.
data MethodPair s (sym :: Symbol) b where
  (:=) :: HasMethod s sym
       => WrappedMethodName sym
       -> b
       -> MethodPair s sym b

infixr 4 :=


------------------------------------------------------------------------------
-- | An extensible record, designed to related a service 's''s methods (the
-- 'Symbol's) to its handlers.
data Record s (ts :: [(Symbol, *)]) where
  Nil  :: Record s '[]
  (:>) :: MethodPair s sym a
       -> Record s ts
       -> Record s ('(sym, a) ': ts)

infixr 3 :>


------------------------------------------------------------------------------
-- | Pull a value out of an extensible 'Record'.
safeFind
    :: forall a s sym ts
     . WrappedMethodName sym
    -> Record s ts
    -> a
safeFind = unsafeFind
  where
    -- What's going on here, you might be asking? We know from our
    -- @Lookup sym ts ~ 'Just a@
    -- constraint that this 'Record' contains an 'a' under the name 'sym', but
    -- it's hard to convince the recursive part of this function about that.
    unsafeFind
        :: forall ts'
         . WrappedMethodName sym
        -> Record s ts'
        -> a
    unsafeFind (sym@WrappedMethodName)
               (sym'@WrappedMethodName := a
                  :> (ts :: Record s ts'')) =
      if symbolVal sym == symbolVal sym'
        then unsafeCoerce a
        else unsafeFind @ts'' sym ts
    unsafeFind _ Nil =
      error "got to the end of unsafeFind"


------------------------------------------------------------------------------
-- | Transforms a label corresponding to a service's method into a 'Setter'.
-- This allows for incremental construction of a service's handlers. To be used
-- like:
--
-- @@
-- service MyService
--   & method #myMethod .~ someHandler
-- @@
method
    :: forall sym a s ts
     . HasMethod s sym
    => WrappedMethodName sym
    -> Setter (Record s ts)
            (Record s ('(sym, a) ': ts))
            a
            a
method sym = lens (error "impossible due to type sig")
                  (\s a -> sym := a :> s)


------------------------------------------------------------------------------
-- | A 'Getter' for 'Record' labels.
getMethod
    :: forall sym a s ts
     . WrappedMethodName sym
    -> Getter (Record s ts) a
getMethod = to . safeFind @a


------------------------------------------------------------------------------
-- | Constructor of an empty 'Record' of type 's'. To be used like:
--
-- @@
-- service MyService
--   & method #myMethod .~ someHandler
-- @@
service :: s -> Record s '[]
service _ = Nil


------------------------------------------------------------------------------
-- | Wrapped to avoid orphan instances of 'IsLabel' for method identifiers.
data WrappedMethodName (sym :: Symbol) where
  WrappedMethodName :: KnownSymbol sym => WrappedMethodName sym

instance (KnownSymbol sym, sym ~ sym') => IsLabel sym (WrappedMethodName sym') where
  fromLabel = WrappedMethodName


------------------------------------------------------------------------------
-- | Promoted version of Prelude's 'lookup'.
type family Lookup (n :: k) (hs :: [(k, v)]) :: Maybe v where
  Lookup n '[]             = 'Nothing
  Lookup n ('(n, v) ': hs) = 'Just v
  Lookup n (x ': hs)       = Lookup n hs


// SPDX-License-Identifier: Apache-2.0

//! FFI-safe request context types.

use abi_stable::std_types::{RHashMap, RString};
use abi_stable::StableAbi;

/// FFI-safe request context containing HTTP headers or gRPC metadata
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub enum RRequestContext {
    Http(RHttpContext),
    Grpc(RGrpcContext),
}

/// FFI-safe HTTP context
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RHttpContext {
    pub headers: RHashMap<RString, RString>,
}

/// FFI-safe gRPC context
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RGrpcContext {
    pub metadata: RHashMap<RString, RString>,
}

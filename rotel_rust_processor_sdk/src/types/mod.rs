// SPDX-License-Identifier: Apache-2.0

//! FFI-safe types for Rotel processors.
//!
//! These types mirror the OpenTelemetry protobuf types but use abi_stable
//! types to ensure a stable ABI across Rust versions.

mod common;
mod context;
mod logs;
mod metrics;
mod traces;

pub use common::*;
pub use context::*;
pub use logs::*;
pub use metrics::*;
pub use traces::*;

// SPDX-License-Identifier: Apache-2.0

// Use jemalloc when the jemalloc feature is enabled
#[cfg(feature = "jemalloc")]
use jemallocator::Jemalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL_JEMALLOC: Jemalloc = Jemalloc;

// Use mimalloc when the mimalloc feature is enabled
#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_MIMALLOC: MiMalloc = MiMalloc;

// Emit compile error if both allocators are enabled
#[cfg(all(feature = "jemalloc", feature = "mimalloc"))]
compile_error!(
    "Cannot enable both jemalloc and mimalloc features. Please select only one allocator."
);

pub mod aws_api;
pub mod bounded_channel;
pub mod crypto;
pub mod exporters;
pub mod init;
pub mod listener;
pub mod otlp;
pub mod receivers;
pub mod semconv;
pub mod telemetry;
pub mod topology;

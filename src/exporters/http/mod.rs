// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "datadog")]
pub mod client;
#[cfg(feature = "datadog")]
pub mod grpc_client;
#[cfg(feature = "datadog")]
pub mod http_client;
#[cfg(feature = "datadog")]
pub mod request;
#[cfg(feature = "datadog")]
pub mod response;
#[cfg(feature = "datadog")]
pub mod retry;
#[cfg(feature = "datadog")]
pub mod types;

// currently shared
pub mod tls;

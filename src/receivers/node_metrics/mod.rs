// SPDX-License-Identifier: Apache-2.0

//! Node metrics receiver
//!
//! Collects system metrics (CPU, memory, load, network, disk, filesystem, etc.) and
//! converts them to OpenTelemetry metrics.

pub mod collector;
pub mod config;
pub mod convert;
pub mod receiver;

pub use config::NodeMetricsConfig;
pub use receiver::NodeMetricsReceiver;

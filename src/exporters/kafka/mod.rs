// SPDX-License-Identifier: Apache-2.0

//! Kafka exporter implementation.
//!
//! This module provides functionality for exporting telemetry data to Apache Kafka.
//! It supports sending traces, metrics, and logs to Kafka topics using the rdkafka library.
//!
//! # Features
//!
//! - Support for traces, metrics, and logs
//! - Configurable topic names for each telemetry type
//! - Batch processing of telemetry data
//! - Configurable serialization format (JSON or Protobuf)
//! - Producer configuration options
//! - Retry and error handling
//!
//! # Modules
//!
//! - `config`: Configuration structures for Kafka exporter
//! - `errors`: Error types specific to Kafka export operations
//! - `exporter`: Core exporter implementation
//! - `request_builder`: Request building and serialization
//! - `producer`: Kafka producer management

pub mod config;
pub mod errors;
pub mod exporter;
pub mod request_builder;

#[cfg(test)]
mod tests;

pub use config::KafkaExporterConfig;
pub use exporter::{
    KafkaExporter, build_traces_exporter, build_metrics_exporter, build_logs_exporter
};

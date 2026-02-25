// SPDX-License-Identifier: Apache-2.0

//! Linux kernel message (kmsg) receiver
//!
//! This receiver reads kernel log messages from `/dev/kmsg` and converts them
//! to OTLP logs. The kmsg device provides kernel log messages in a simple
//! text format: `priority,sequence,timestamp;message\n`
//!
//! This receiver is Linux-only as `/dev/kmsg` doesn't exist on other platforms.
//!
//! ## At-Least-Once Delivery
//!
//! When offset persistence is enabled (`offsets_path` config), the receiver
//! implements at-least-once delivery semantics by tracking message acknowledgements
//! from downstream exporters. Offsets are only persisted after messages have been
//! successfully exported, ensuring no messages are lost on crash or restart.

pub mod config;
pub mod convert;
pub mod error;
pub mod offset_committer;
pub mod offset_tracker;
pub mod parser;
pub mod persistence;
pub mod receiver;

pub use config::KmsgReceiverConfig;
pub use error::{KmsgReceiverError, Result};
pub use receiver::KmsgReceiver;

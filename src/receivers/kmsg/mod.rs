// SPDX-License-Identifier: Apache-2.0

//! Linux kernel message (kmsg) receiver
//!
//! This receiver reads kernel log messages from `/dev/kmsg` and converts them
//! to OTLP logs. The kmsg device provides kernel log messages in a simple
//! text format: `priority,sequence,timestamp;message\n`
//!
//! This receiver is Linux-only as `/dev/kmsg` doesn't exist on other platforms.

pub mod config;
pub mod convert;
pub mod error;
pub mod parser;
pub mod receiver;

pub use config::KmsgReceiverConfig;
pub use error::{KmsgReceiverError, Result};
pub use receiver::KmsgReceiver;

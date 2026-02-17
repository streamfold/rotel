// SPDX-License-Identifier: Apache-2.0

use crate::receivers::kmsg::config::{DEFAULT_PRIORITY_LEVEL, KmsgReceiverConfig};
use clap::Args;
use serde::Deserialize;

#[derive(Debug, Args, Clone, Deserialize)]
#[serde(default)]
pub struct KmsgReceiverArgs {
    /// Maximum priority level to include (0-7, default: 6)
    /// 0=emerg, 1=alert, 2=crit, 3=err, 4=warn, 5=notice, 6=info, 7=debug
    #[arg(long, env = "ROTEL_KMSG_RECEIVER_PRIORITY_LEVEL")]
    pub kmsg_receiver_priority_level: Option<u8>,

    /// Read existing messages from the kernel ring buffer on startup
    #[arg(
        long,
        env = "ROTEL_KMSG_RECEIVER_READ_EXISTING",
        default_value = "false"
    )]
    pub kmsg_receiver_read_existing: bool,

    /// Maximum number of log records to batch before sending (default: 100)
    #[arg(long, env = "ROTEL_KMSG_RECEIVER_BATCH_SIZE")]
    pub kmsg_receiver_batch_size: Option<usize>,

    /// Maximum time to wait before flushing a batch in milliseconds (default: 250)
    #[arg(long, env = "ROTEL_KMSG_RECEIVER_BATCH_TIMEOUT_MS")]
    pub kmsg_receiver_batch_timeout_ms: Option<u64>,
}

impl Default for KmsgReceiverArgs {
    fn default() -> Self {
        Self {
            kmsg_receiver_priority_level: None,
            kmsg_receiver_read_existing: false,
            kmsg_receiver_batch_size: None,
            kmsg_receiver_batch_timeout_ms: None,
        }
    }
}

impl KmsgReceiverArgs {
    pub fn build_config(&self) -> KmsgReceiverConfig {
        let mut config = KmsgReceiverConfig::new(
            self.kmsg_receiver_priority_level
                .unwrap_or(DEFAULT_PRIORITY_LEVEL),
            self.kmsg_receiver_read_existing,
        );

        if let Some(batch_size) = self.kmsg_receiver_batch_size {
            config = config.with_batch_size(batch_size);
        }
        if let Some(batch_timeout_ms) = self.kmsg_receiver_batch_timeout_ms {
            config = config.with_batch_timeout_ms(batch_timeout_ms);
        }

        config
    }
}

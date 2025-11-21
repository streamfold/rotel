// SPDX-License-Identifier: Apache-2.0

use crate::receivers::fluent::config::FluentReceiverConfig;
use clap::Args;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Args, Clone, Deserialize)]
#[serde(default)]
pub struct FluentReceiverArgs {
    /// Path to the UNIX socket file for Fluent receiver
    #[arg(
        long,
        env = "ROTEL_FLUENT_RECEIVER_SOCKET",
        default_value = "/var/run/fluent.sock"
    )]
    pub fluent_receiver_socket: PathBuf,

    /// Enable traces for Fluent receiver
    #[arg(long, env = "ROTEL_FLUENT_RECEIVER_TRACES", default_value = "false")]
    pub fluent_receiver_traces: bool,

    /// Enable metrics for Fluent receiver
    #[arg(long, env = "ROTEL_FLUENT_RECEIVER_METRICS", default_value = "false")]
    pub fluent_receiver_metrics: bool,

    /// Enable logs for Fluent receiver
    #[arg(long, env = "ROTEL_FLUENT_RECEIVER_LOGS", default_value = "true")]
    pub fluent_receiver_logs: bool,
}

impl Default for FluentReceiverArgs {
    fn default() -> Self {
        Self {
            fluent_receiver_socket: PathBuf::from("/var/run/fluent.sock"),
            fluent_receiver_traces: false,
            fluent_receiver_metrics: false,
            fluent_receiver_logs: true,
        }
    }
}

impl FluentReceiverArgs {
    pub fn build_config(&self) -> FluentReceiverConfig {
        FluentReceiverConfig::new(self.fluent_receiver_socket.clone())
            .with_traces(self.fluent_receiver_traces)
            .with_metrics(self.fluent_receiver_metrics)
            .with_logs(self.fluent_receiver_logs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_args() {
        let args = FluentReceiverArgs::default();
        assert_eq!(
            args.fluent_receiver_socket,
            PathBuf::from("/var/run/fluent.sock")
        );
        assert!(!args.fluent_receiver_traces);
        assert!(!args.fluent_receiver_metrics);
        assert!(args.fluent_receiver_logs);
    }

    #[test]
    fn test_build_config() {
        let args = FluentReceiverArgs {
            fluent_receiver_socket: PathBuf::from("/tmp/test.sock"),
            fluent_receiver_traces: true,
            fluent_receiver_metrics: true,
            fluent_receiver_logs: false,
        };

        let config = args.build_config();
        assert_eq!(config.socket_path, PathBuf::from("/tmp/test.sock"));
        assert!(config.traces);
        assert!(config.metrics);
        assert!(!config.logs);
    }
}

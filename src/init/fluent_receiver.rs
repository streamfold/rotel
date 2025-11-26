// SPDX-License-Identifier: Apache-2.0

use crate::init::parse;
use crate::receivers::fluent::config::FluentReceiverConfig;
use clap::Args;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Args, Clone, Deserialize)]
#[serde(default)]
pub struct FluentReceiverArgs {
    /// Path to the UNIX socket file for Fluent receiver
    #[arg(long, env = "ROTEL_FLUENT_RECEIVER_SOCKET")]
    pub fluent_receiver_socket: Option<PathBuf>,

    /// TCP endpoint for Fluent receiver (e.g., 127.0.0.1:23890)
    #[arg(long, env = "ROTEL_FLUENT_RECEIVER_ENDPOINT", value_parser = parse::parse_endpoint)]
    pub fluent_receiver_endpoint: Option<SocketAddr>,
}

impl Default for FluentReceiverArgs {
    fn default() -> Self {
        Self {
            fluent_receiver_socket: Some(PathBuf::from("/var/run/fluent.sock")),
            fluent_receiver_endpoint: None,
        }
    }
}

impl FluentReceiverArgs {
    pub fn build_config(&self) -> FluentReceiverConfig {
        FluentReceiverConfig::new(
            self.fluent_receiver_socket.clone(),
            self.fluent_receiver_endpoint,
        )
    }
}

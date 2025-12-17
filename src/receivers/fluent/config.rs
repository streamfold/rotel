// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;
use std::path::PathBuf;

/// Configuration for the Fluent receiver
#[derive(Debug, Clone)]
pub struct FluentReceiverConfig {
    /// Path to the UNIX socket file (optional)
    pub socket_path: Option<PathBuf>,

    /// TCP endpoint to bind to (optional)
    pub endpoint: Option<SocketAddr>,
}

impl Default for FluentReceiverConfig {
    fn default() -> Self {
        Self {
            socket_path: None,
            endpoint: None,
        }
    }
}

impl FluentReceiverConfig {
    pub fn new(socket_path: Option<PathBuf>, endpoint: Option<SocketAddr>) -> Self {
        Self {
            socket_path,
            endpoint,
        }
    }
}

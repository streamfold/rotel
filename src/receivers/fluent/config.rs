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
            socket_path: Some(PathBuf::from("/var/run/fluent.sock")),
            endpoint: None,
        }
    }
}

impl FluentReceiverConfig {
    pub fn new(socket_path: Option<PathBuf>, endpoint: Option<SocketAddr>) -> Self {
        Self {
            socket_path,
            endpoint,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_with_endpoint() {
        let addr: SocketAddr = "127.0.0.1:23890".parse().unwrap();
        let config = FluentReceiverConfig::new(None, Some(addr));

        assert_eq!(config.socket_path, None);
        assert_eq!(config.endpoint, Some(addr));
    }

    #[test]
    fn test_config_with_both() {
        let addr: SocketAddr = "127.0.0.1:23890".parse().unwrap();
        let config = FluentReceiverConfig::new(Some(PathBuf::from("/tmp/test.sock")), Some(addr));

        assert_eq!(config.socket_path, Some(PathBuf::from("/tmp/test.sock")));
        assert_eq!(config.endpoint, Some(addr));
    }
}

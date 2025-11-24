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

    /// Enable traces processing
    pub traces: bool,

    /// Enable metrics processing
    pub metrics: bool,

    /// Enable logs processing
    pub logs: bool,
}

impl Default for FluentReceiverConfig {
    fn default() -> Self {
        Self {
            socket_path: Some(PathBuf::from("/var/run/fluent.sock")),
            endpoint: None,
            traces: false,
            metrics: false,
            logs: true,
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

    pub fn with_traces(mut self, enabled: bool) -> Self {
        self.traces = enabled;
        self
    }

    pub fn with_metrics(mut self, enabled: bool) -> Self {
        self.metrics = enabled;
        self
    }

    pub fn with_logs(mut self, enabled: bool) -> Self {
        self.logs = enabled;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = FluentReceiverConfig::default();
        assert_eq!(
            config.socket_path,
            Some(PathBuf::from("/var/run/fluent.sock"))
        );
        assert_eq!(config.endpoint, None);
        assert!(!config.traces);
        assert!(!config.metrics);
        assert!(config.logs);
    }

    #[test]
    fn test_config_builder() {
        let config = FluentReceiverConfig::new(Some(PathBuf::from("/tmp/test.sock")), None)
            .with_traces(true)
            .with_metrics(true)
            .with_logs(false);

        assert_eq!(config.socket_path, Some(PathBuf::from("/tmp/test.sock")));
        assert_eq!(config.endpoint, None);
        assert!(config.traces);
        assert!(config.metrics);
        assert!(!config.logs);
    }

    #[test]
    fn test_config_with_endpoint() {
        let addr: SocketAddr = "127.0.0.1:23890".parse().unwrap();
        let config = FluentReceiverConfig::new(None, Some(addr)).with_logs(true);

        assert_eq!(config.socket_path, None);
        assert_eq!(config.endpoint, Some(addr));
        assert!(config.logs);
    }

    #[test]
    fn test_config_with_both() {
        let addr: SocketAddr = "127.0.0.1:23890".parse().unwrap();
        let config = FluentReceiverConfig::new(Some(PathBuf::from("/tmp/test.sock")), Some(addr))
            .with_logs(true);

        assert_eq!(config.socket_path, Some(PathBuf::from("/tmp/test.sock")));
        assert_eq!(config.endpoint, Some(addr));
        assert!(config.logs);
    }
}

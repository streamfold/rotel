// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

/// Configuration for the Fluent receiver
#[derive(Debug, Clone)]
pub struct FluentReceiverConfig {
    /// Path to the UNIX socket file
    pub socket_path: PathBuf,

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
            socket_path: PathBuf::from("/var/run/fluent.sock"),
            traces: false,
            metrics: false,
            logs: true,
        }
    }
}

impl FluentReceiverConfig {
    pub fn new(socket_path: PathBuf) -> Self {
        Self {
            socket_path,
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
        assert_eq!(config.socket_path, PathBuf::from("/var/run/fluent.sock"));
        assert!(!config.traces);
        assert!(!config.metrics);
        assert!(config.logs);
    }

    #[test]
    fn test_config_builder() {
        let config = FluentReceiverConfig::new(PathBuf::from("/tmp/test.sock"))
            .with_traces(true)
            .with_metrics(true)
            .with_logs(false);

        assert_eq!(config.socket_path, PathBuf::from("/tmp/test.sock"));
        assert!(config.traces);
        assert!(config.metrics);
        assert!(!config.logs);
    }
}

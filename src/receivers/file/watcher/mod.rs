// SPDX-License-Identifier: Apache-2.0

//! File system watcher abstractions for the file receiver.
//!
//! This module provides two watching strategies:
//! - **Native watching**: Uses OS-level file system notifications (inotify on Linux,
//!   FSEvents on macOS, ReadDirectoryChangesW on Windows) for immediate event detection.
//! - **Poll watching**: Falls back to periodic file system polling for environments
//!   where native watching isn't available or reliable (e.g., NFS, network shares).
//!
//! The recommended approach is to use `auto` mode which attempts native watching
//! first and falls back to polling if it fails.

mod native;
mod poll;
mod traits;

pub use native::NativeWatcher;
pub use poll::PollWatcher;
pub use traits::{FileEvent, FileEventKind, FileWatcher, WatcherError};

use std::path::Path;
use std::time::Duration;

/// Watch mode configuration
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WatchMode {
    /// Automatically select the best watching strategy.
    /// Tries native watching first, falls back to polling on failure.
    #[default]
    Auto,
    /// Force native file system watching (inotify/kqueue/FSEvents).
    /// Will fail if native watching is not supported.
    Native,
    /// Force polling mode. Use this for network file systems (NFS)
    /// or when native watching is unreliable.
    Poll,
}

impl std::str::FromStr for WatchMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(WatchMode::Auto),
            "native" => Ok(WatchMode::Native),
            "poll" | "polling" => Ok(WatchMode::Poll),
            _ => Err(format!(
                "Invalid watch mode '{}'. Valid options: auto, native, poll",
                s
            )),
        }
    }
}

/// Configuration for the file watcher
#[derive(Debug, Clone)]
pub struct WatcherConfig {
    /// Watch mode: auto, native, or poll
    pub mode: WatchMode,
    /// Poll interval when using poll mode (or as a fallback check)
    pub poll_interval: Duration,
    /// Debounce interval for native events to batch rapid changes
    pub debounce_interval: Duration,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            mode: WatchMode::Auto,
            poll_interval: Duration::from_millis(250),
            debounce_interval: Duration::from_millis(100),
        }
    }
}

/// Create a watcher based on the configuration.
///
/// In `Auto` mode, this tries native watching first and falls back to polling
/// if native watching fails to initialize.
pub fn create_watcher(
    config: &WatcherConfig,
    directories: &[&Path],
) -> Result<Box<dyn FileWatcher + Send + Sync>, WatcherError> {
    match config.mode {
        WatchMode::Native => {
            let watcher = NativeWatcher::new(config.debounce_interval)?;
            Ok(Box::new(watcher))
        }
        WatchMode::Poll => {
            let watcher = PollWatcher::new(directories, config.poll_interval)?;
            Ok(Box::new(watcher))
        }
        WatchMode::Auto => {
            // Try native first
            match NativeWatcher::new(config.debounce_interval) {
                Ok(watcher) => {
                    tracing::info!("Using native file system watcher");
                    Ok(Box::new(watcher))
                }
                Err(e) => {
                    tracing::warn!(
                        "Native file watching unavailable ({}), falling back to polling",
                        e
                    );
                    let watcher = PollWatcher::new(directories, config.poll_interval)?;
                    Ok(Box::new(watcher))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watch_mode_from_str() {
        assert_eq!("auto".parse::<WatchMode>().unwrap(), WatchMode::Auto);
        assert_eq!("native".parse::<WatchMode>().unwrap(), WatchMode::Native);
        assert_eq!("poll".parse::<WatchMode>().unwrap(), WatchMode::Poll);
        assert_eq!("polling".parse::<WatchMode>().unwrap(), WatchMode::Poll);
        assert_eq!("AUTO".parse::<WatchMode>().unwrap(), WatchMode::Auto);
        assert!("invalid".parse::<WatchMode>().is_err());
    }

    #[test]
    fn test_watcher_config_default() {
        let config = WatcherConfig::default();
        assert_eq!(config.mode, WatchMode::Auto);
        assert_eq!(config.poll_interval, Duration::from_millis(250));
        assert_eq!(config.debounce_interval, Duration::from_millis(100));
    }
}

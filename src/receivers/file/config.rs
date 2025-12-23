// SPDX-License-Identifier: Apache-2.0

//! Configuration for the file receiver.

use std::path::PathBuf;
use std::time::Duration;

use crate::receivers::file::input::StartAt;
use crate::receivers::file::watcher::WatchMode;

/// Parser type for log parsing
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ParserType {
    /// No parsing, raw log lines
    #[default]
    None,
    /// Parse as JSON
    Json,
    /// Parse with regex pattern
    Regex,
    /// Parse nginx access logs
    NginxAccess,
    /// Parse nginx error logs
    NginxError,
}

/// Configuration for the file receiver
#[derive(Debug, Clone)]
pub struct FileReceiverConfig {
    /// Glob patterns for files to include
    pub include: Vec<String>,
    /// Glob patterns for files to exclude
    pub exclude: Vec<String>,
    /// Parser type
    pub parser: ParserType,
    /// Regex pattern (when parser is Regex)
    pub regex_pattern: Option<String>,
    /// Where to start reading: beginning or end of file
    pub start_at: StartAt,
    /// Watch mode: auto, native, or poll
    pub watch_mode: WatchMode,
    /// Poll interval for checking file changes (used in poll mode or as fallback)
    pub poll_interval: Duration,
    /// Path to store file offsets for persistence
    pub offsets_path: PathBuf,
    /// Number of bytes to use for file fingerprinting
    pub fingerprint_size: usize,
    /// Maximum log line size in bytes
    pub max_log_size: usize,
    /// Include file name as log attribute
    pub include_file_name: bool,
    /// Include file path as log attribute
    pub include_file_path: bool,
    /// Maximum number of concurrent file processing workers
    pub max_concurrent_files: usize,
}

impl Default for FileReceiverConfig {
    fn default() -> Self {
        Self {
            include: Vec::new(),
            exclude: Vec::new(),
            parser: ParserType::None,
            regex_pattern: None,
            start_at: StartAt::End,
            watch_mode: WatchMode::Auto,
            poll_interval: Duration::from_millis(250),
            offsets_path: PathBuf::from("/var/lib/rotel/file_offsets.json"),
            fingerprint_size: 1000,
            max_log_size: 65536,
            include_file_name: true,
            include_file_path: false,
            max_concurrent_files: 64,
        }
    }
}

impl FileReceiverConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.include.is_empty() {
            return Err("At least one include pattern must be specified".to_string());
        }

        if self.parser == ParserType::Regex && self.regex_pattern.is_none() {
            return Err("Regex pattern must be specified when parser is 'regex'".to_string());
        }

        Ok(())
    }
}

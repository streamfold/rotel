// SPDX-License-Identifier: Apache-2.0

use clap::{Args, ValueEnum};
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;

use crate::receivers::file::config::{FileReceiverConfig, ParserType as ConfigParserType};
use crate::receivers::file::input::StartAt;
use crate::receivers::file::watcher::WatchMode;

/// Parser type for log parsing
#[derive(Copy, Clone, Debug, Default, ValueEnum, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParserType {
    /// No parsing, raw log lines
    #[default]
    None,
    /// Parse as JSON
    Json,
    /// Parse with regex pattern
    Regex,
    /// Parse nginx access logs
    #[value(name = "nginx_access")]
    NginxAccess,
    /// Parse nginx error logs
    #[value(name = "nginx_error")]
    NginxError,
}

impl From<ParserType> for ConfigParserType {
    fn from(p: ParserType) -> Self {
        match p {
            ParserType::None => ConfigParserType::None,
            ParserType::Json => ConfigParserType::Json,
            ParserType::Regex => ConfigParserType::Regex,
            ParserType::NginxAccess => ConfigParserType::NginxAccess,
            ParserType::NginxError => ConfigParserType::NginxError,
        }
    }
}

/// Where to start reading files
#[derive(Copy, Clone, Debug, Default, ValueEnum, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StartAtArg {
    /// Start at the beginning of the file
    Beginning,
    /// Start at the end of the file (tail mode)
    #[default]
    End,
}

impl From<StartAtArg> for StartAt {
    fn from(s: StartAtArg) -> Self {
        match s {
            StartAtArg::Beginning => StartAt::Beginning,
            StartAtArg::End => StartAt::End,
        }
    }
}

/// Watch mode for file system monitoring
#[derive(Copy, Clone, Debug, Default, ValueEnum, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WatchModeArg {
    /// Automatically select the best watching strategy (native first, poll fallback)
    #[default]
    Auto,
    /// Force native file system watching (inotify/kqueue/FSEvents)
    Native,
    /// Force polling mode (use for NFS or when native watching is unreliable)
    Poll,
}

impl From<WatchModeArg> for WatchMode {
    fn from(w: WatchModeArg) -> Self {
        match w {
            WatchModeArg::Auto => WatchMode::Auto,
            WatchModeArg::Native => WatchMode::Native,
            WatchModeArg::Poll => WatchMode::Poll,
        }
    }
}

#[derive(Debug, Args, Clone, Deserialize)]
#[serde(default)]
pub struct FileReceiverArgs {
    /// Comma-separated glob patterns for files to include (e.g., "/var/log/*.log,/tmp/*.log")
    #[arg(long, env = "ROTEL_FILE_RECEIVER_INCLUDE", value_delimiter = ',')]
    pub file_receiver_include: Vec<String>,

    /// Comma-separated glob patterns for files to exclude
    #[arg(long, env = "ROTEL_FILE_RECEIVER_EXCLUDE", value_delimiter = ',')]
    pub file_receiver_exclude: Vec<String>,

    /// Parser type: none, json, regex, nginx_access, nginx_error
    #[arg(
        value_enum,
        long,
        env = "ROTEL_FILE_RECEIVER_PARSER",
        default_value = "none"
    )]
    pub file_receiver_parser: ParserType,

    /// Regex pattern with named capture groups (when parser=regex)
    #[arg(long, env = "ROTEL_FILE_RECEIVER_REGEX_PATTERN")]
    pub file_receiver_regex_pattern: Option<String>,

    /// Where to start reading: beginning or end of file
    #[arg(
        value_enum,
        long,
        env = "ROTEL_FILE_RECEIVER_START_AT",
        default_value = "end"
    )]
    pub file_receiver_start_at: StartAtArg,

    /// Watch mode: auto (default), native (inotify/kqueue/FSEvents), poll (for NFS)
    #[arg(
        value_enum,
        long,
        env = "ROTEL_FILE_RECEIVER_WATCH_MODE",
        default_value = "auto"
    )]
    pub file_receiver_watch_mode: WatchModeArg,

    /// Poll interval in milliseconds for checking file changes (used in poll mode or as fallback)
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_POLL_INTERVAL_MS",
        default_value = "250"
    )]
    pub file_receiver_poll_interval_ms: u64,

    /// Path to store file offsets for persistence across restarts
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_OFFSETS_PATH",
        default_value = "/var/lib/rotel/file_offsets.json"
    )]
    pub file_receiver_offsets_path: PathBuf,

    /// Number of bytes to use for file fingerprinting (for tracking files across renames)
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_FINGERPRINT_SIZE",
        default_value = "1000"
    )]
    pub file_receiver_fingerprint_size: usize,

    /// Maximum log line size in bytes (lines exceeding this will be truncated)
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_MAX_LOG_SIZE",
        default_value = "65536"
    )]
    pub file_receiver_max_log_size: usize,

    /// Include file name as a log attribute
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_INCLUDE_FILE_NAME",
        default_value = "true"
    )]
    pub file_receiver_include_file_name: bool,

    /// Include full file path as a log attribute
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_INCLUDE_FILE_PATH",
        default_value = "false"
    )]
    pub file_receiver_include_file_path: bool,
}

impl Default for FileReceiverArgs {
    fn default() -> Self {
        Self {
            file_receiver_include: Vec::new(),
            file_receiver_exclude: Vec::new(),
            file_receiver_parser: ParserType::None,
            file_receiver_regex_pattern: None,
            file_receiver_start_at: StartAtArg::End,
            file_receiver_watch_mode: WatchModeArg::Auto,
            file_receiver_poll_interval_ms: 250,
            file_receiver_offsets_path: PathBuf::from("/var/lib/rotel/file_offsets.json"),
            file_receiver_fingerprint_size: 1000,
            file_receiver_max_log_size: 65536,
            file_receiver_include_file_name: true,
            file_receiver_include_file_path: false,
        }
    }
}

impl FileReceiverArgs {
    /// Build the receiver config from command line args
    pub fn build_config(&self) -> FileReceiverConfig {
        FileReceiverConfig {
            include: self.file_receiver_include.clone(),
            exclude: self.file_receiver_exclude.clone(),
            parser: self.file_receiver_parser.into(),
            regex_pattern: self.file_receiver_regex_pattern.clone(),
            start_at: self.file_receiver_start_at.into(),
            watch_mode: self.file_receiver_watch_mode.into(),
            poll_interval: Duration::from_millis(self.file_receiver_poll_interval_ms),
            offsets_path: self.file_receiver_offsets_path.clone(),
            fingerprint_size: self.file_receiver_fingerprint_size,
            max_log_size: self.file_receiver_max_log_size,
            include_file_name: self.file_receiver_include_file_name,
            include_file_path: self.file_receiver_include_file_path,
        }
    }
}

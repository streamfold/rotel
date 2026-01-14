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

    /// Debounce interval in milliseconds for native file watcher to coalesce rapid events.
    /// Higher values reduce CPU usage but increase latency. On Linux with inotify,
    /// consider using 500ms or higher for high-throughput scenarios.
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_DEBOUNCE_INTERVAL_MS",
        default_value = "200"
    )]
    pub file_receiver_debounce_interval_ms: u64,

    /// Path to store file offsets for persistence across restarts
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_OFFSETS_PATH",
        default_value = "/var/lib/rotel/file_offsets.json"
    )]
    pub file_receiver_offsets_path: PathBuf,

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

    /// Maximum number of files to process concurrently
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_MAX_CONCURRENT_FILES",
        default_value = "64"
    )]
    pub file_receiver_max_concurrent_files: usize,

    /// Time in milliseconds to wait after reaching EOF on a rotated file before closing it.
    /// This allows draining any remaining content that the writer may still be flushing.
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_ROTATE_WAIT_MS",
        default_value = "1000"
    )]
    pub file_receiver_rotate_wait_ms: u64,

    /// Maximum time in milliseconds to wait for in-flight workers to complete during shutdown
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_SHUTDOWN_WORKER_DRAIN_TIMEOUT_MS",
        default_value = "3000"
    )]
    pub file_receiver_shutdown_worker_drain_timeout_ms: u64,

    /// Maximum time in milliseconds to wait for log records to be sent to pipeline during shutdown
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_SHUTDOWN_RECORDS_DRAIN_TIMEOUT_MS",
        default_value = "2000"
    )]
    pub file_receiver_shutdown_records_drain_timeout_ms: u64,

    /// Maximum duration in milliseconds of consecutive checkpoint failures before exiting
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_MAX_CHECKPOINT_FAILURE_DURATION_MS",
        default_value = "60000"
    )]
    pub file_receiver_max_checkpoint_failure_duration_ms: u64,

    /// Maximum duration in milliseconds of consecutive poll/file discovery failures before exiting
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_MAX_POLL_FAILURE_DURATION_MS",
        default_value = "60000"
    )]
    pub file_receiver_max_poll_failure_duration_ms: u64,

    /// Maximum duration in milliseconds of consecutive watcher errors before falling back to polling
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_MAX_WATCHER_ERROR_DURATION_MS",
        default_value = "60000"
    )]
    pub file_receiver_max_watcher_error_duration_ms: u64,

    /// Maximum number of log records to accumulate before sending a batch to the pipeline
    #[arg(
        long,
        env = "ROTEL_FILE_RECEIVER_MAX_BATCH_SIZE",
        default_value = "100"
    )]
    pub file_receiver_max_batch_size: usize,
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
            file_receiver_debounce_interval_ms: 200,
            file_receiver_offsets_path: PathBuf::from("/var/lib/rotel/file_offsets.json"),
            file_receiver_max_log_size: 65536,
            file_receiver_include_file_name: true,
            file_receiver_include_file_path: false,
            file_receiver_max_concurrent_files: 64,
            file_receiver_rotate_wait_ms: 1000,
            file_receiver_shutdown_worker_drain_timeout_ms: 3000,
            file_receiver_shutdown_records_drain_timeout_ms: 2000,
            file_receiver_max_checkpoint_failure_duration_ms: 60000,
            file_receiver_max_poll_failure_duration_ms: 60000,
            file_receiver_max_watcher_error_duration_ms: 60000,
            file_receiver_max_batch_size: 100,
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
            debounce_interval: Duration::from_millis(self.file_receiver_debounce_interval_ms),
            offsets_path: self.file_receiver_offsets_path.clone(),
            max_log_size: self.file_receiver_max_log_size,
            include_file_name: self.file_receiver_include_file_name,
            include_file_path: self.file_receiver_include_file_path,
            max_concurrent_files: self.file_receiver_max_concurrent_files,
            rotate_wait: Duration::from_millis(self.file_receiver_rotate_wait_ms),
            shutdown_worker_drain_timeout: Duration::from_millis(
                self.file_receiver_shutdown_worker_drain_timeout_ms,
            ),
            shutdown_records_drain_timeout: Duration::from_millis(
                self.file_receiver_shutdown_records_drain_timeout_ms,
            ),
            max_checkpoint_failure_duration: Duration::from_millis(
                self.file_receiver_max_checkpoint_failure_duration_ms,
            ),
            max_poll_failure_duration: Duration::from_millis(
                self.file_receiver_max_poll_failure_duration_ms,
            ),
            max_watcher_error_duration: Duration::from_millis(
                self.file_receiver_max_watcher_error_duration_ms,
            ),
            max_batch_size: self.file_receiver_max_batch_size,
        }
    }
}

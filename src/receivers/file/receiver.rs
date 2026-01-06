// SPDX-License-Identifier: Apache-2.0

//! File receiver implementation.
//!
//! Uses native file system notifications (inotify/kqueue/FSEvents) when available,
//! with automatic fallback to polling for unsupported file systems like NFS.
//!
//! Architecture:
//! - A coordinator thread watches for file changes and dispatches work
//! - Worker tasks (via spawn_blocking) process files in parallel: read, parse, build LogRecords
//! - Workers send LogRecordBatch directly to an async task for export
//! - This prevents file I/O from blocking the tokio runtime and enables parallel processing

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::stream::FuturesOrdered;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use opentelemetry_proto::tonic::common::v1::KeyValue as OtlpKeyValue;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use tokio::select;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tower::BoxError;
use tracing::{debug, error, info, warn};

use crate::bounded_channel::{self, BoundedReceiver, BoundedSender};

use crate::receivers::file::config::{FileReceiverConfig, ParserType};
use crate::receivers::file::error::{Error, Result};
use crate::receivers::file::input::{FileFinder, FileId, FileReader, StartAt, get_path_from_file};
use crate::receivers::file::parser::{JsonParser, Parser, RegexParser, nginx};
use crate::receivers::file::persistence::{
    JsonFileDatabase, JsonFilePersister, Persister, PersisterExt,
};
use crate::receivers::file::watcher::{FileEventKind, FileWatcher, WatcherConfig, create_watcher};
use crate::receivers::get_meter;
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::payload;

const KNOWN_FILES_KEY: &str = "knownFiles";

/// Persisted state for all known files
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct PersistedState {
    files: Vec<PersistedFileState>,
}

/// Persisted state for a single file
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PersistedFileState {
    /// Device ID (Unix) or volume serial (Windows)
    dev: u64,
    /// Inode number (Unix) or file index (Windows)
    ino: u64,
    /// Current read offset
    offset: u64,
}

/// File receiver for tailing log files
pub struct FileReceiver {
    config: FileReceiverConfig,
    logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
}

impl FileReceiver {
    /// Create a new file receiver
    pub async fn new(
        config: FileReceiverConfig,
        logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    ) -> std::result::Result<Self, BoxError> {
        config.validate().map_err(|e| -> BoxError { e.into() })?;

        Ok(Self {
            config,
            logs_output,
        })
    }

    /// Build the parser based on config
    fn build_parser(config: &FileReceiverConfig) -> Result<Option<Box<dyn Parser + Send + Sync>>> {
        match config.parser {
            ParserType::None => Ok(None),
            ParserType::Json => Ok(Some(Box::new(JsonParser::lenient()))),
            ParserType::Regex => {
                let pattern = config
                    .regex_pattern
                    .as_ref()
                    .ok_or_else(|| Error::Config("Regex pattern required".to_string()))?;
                let parser = RegexParser::new(pattern).map_err(|e| Error::Regex(e.to_string()))?;
                Ok(Some(Box::new(parser)))
            }
            ParserType::NginxAccess => {
                let parser = nginx::access_parser().map_err(|e| Error::Regex(e.to_string()))?;
                Ok(Some(Box::new(parser)))
            }
            ParserType::NginxError => {
                let parser = nginx::error_parser().map_err(|e| Error::Regex(e.to_string()))?;
                Ok(Some(Box::new(parser)))
            }
        }
    }

    /// Start the file receiver
    pub async fn start(
        self,
        task_set: &mut JoinSet<std::result::Result<(), BoxError>>,
        receivers_cancel: &CancellationToken,
    ) -> std::result::Result<(), BoxError> {
        info!(
            include = ?self.config.include,
            exclude = ?self.config.exclude,
            parser = ?self.config.parser,
            watch_mode = ?self.config.watch_mode,
            "Starting file receiver"
        );

        let config = self.config.clone();
        let logs_output = self.logs_output.clone();
        let cancel = receivers_cancel.clone();

        task_set.spawn(async move {
            let mut handler = FileHandler::new(config, logs_output)?;
            handler.run(cancel).await;
            Ok(())
        });

        Ok(())
    }
}

/// Checkpoint configuration for batching persistence writes
struct CheckpointConfig {
    /// Maximum records before forcing a checkpoint
    max_records: u64,
    /// Maximum time between checkpoints
    max_interval: Duration,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            max_records: 1000,
            max_interval: Duration::from_secs(1),
        }
    }
}

/// Message sent from workers to the async task
struct LogRecordBatch {
    log_records: Vec<LogRecord>,
    count: u64,
}

/// Work item sent from coordinator to workers
struct FileWorkItem {
    /// FileId for this file (used to match results back to tracked files)
    file_id: FileId,
    /// Path to the file to process
    path: PathBuf,
    /// Starting offset in the file
    offset: u64,
    /// File name (cached to avoid re-extracting)
    file_name: Option<String>,
    /// Maximum log line size
    max_log_size: usize,
}

/// Result sent from workers back to coordinator
struct FileWorkResult {
    /// FileId identifying the file
    file_id: FileId,
    /// New offset after reading
    new_offset: u64,
    /// Whether EOF was reached
    reached_eof: bool,
}

/// Shared worker context (immutable, cloned to each worker)
#[derive(Clone)]
struct WorkerContext {
    /// Parser for log lines
    parser: Option<Arc<dyn Parser + Send + Sync>>,
    /// Whether to include file name in attributes
    include_file_name: bool,
    /// Whether to include file path in attributes
    include_file_path: bool,
    /// Channel to send log records to async handler
    records_tx: BoundedSender<LogRecordBatch>,
    /// Channel to send offset updates back to coordinator
    results_tx: BoundedSender<FileWorkResult>,
}

/// Maximum number of log records to accumulate before sending a batch
const MAX_BATCH_SIZE: usize = 100;

/// Default maximum number of concurrent file processing workers
const DEFAULT_MAX_CONCURRENT_WORKERS: usize = 64;

/// Process a single file work item (runs in spawn_blocking)
fn process_file_work(work: FileWorkItem, ctx: WorkerContext) {
    // Create a reader for this work item
    let mut reader = match FileReader::new(&work.path, work.offset, work.max_log_size) {
        Ok(r) => r,
        Err(e) => {
            debug!("Failed to create reader for {:?}: {}", work.path, e);
            let result = FileWorkResult {
                file_id: work.file_id,
                new_offset: work.offset,
                reached_eof: false,
            };
            let _ = ctx.results_tx.send_blocking(result);
            return;
        }
    };

    // Get current timestamp for all log records in this batch
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;

    // Read lines and build LogRecords, sending in small batches
    let mut log_records = Vec::with_capacity(MAX_BATCH_SIZE);
    let file_name = work.file_name.as_deref();
    let path_str = if ctx.include_file_path {
        Some(work.path.display().to_string())
    } else {
        None
    };

    let records_tx = &ctx.records_tx;

    let parse_result = reader.read_lines_into(|line| {
        // Build attributes
        let mut attributes = Vec::new();

        if ctx.include_file_name {
            if let Some(name) = file_name {
                attributes.push(OtlpKeyValue {
                    key: "log.file.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(name.to_string())),
                    }),
                });
            }
        }

        if let Some(ref path) = path_str {
            attributes.push(OtlpKeyValue {
                key: "log.file.path".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(path.clone())),
                }),
            });
        }

        // Parse line if parser is configured
        let (parsed_attributes, severity_number, severity_text) =
            if let Some(ref parser) = ctx.parser {
                match parser.parse(&line) {
                    Ok(parsed) => (
                        parsed.attributes,
                        parsed.severity_number.unwrap_or(0),
                        parsed.severity_text.unwrap_or_default(),
                    ),
                    Err(e) => {
                        debug!("Parse error: {}", e);
                        return; // Skip unparseable entries
                    }
                }
            } else {
                (Vec::new(), 0, String::new())
            };

        // Add parsed attributes
        attributes.extend(parsed_attributes);

        // Build LogRecord
        let log_record = LogRecord {
            time_unix_nano: now,
            observed_time_unix_nano: now,
            severity_number,
            severity_text,
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(line)),
            }),
            attributes,
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![],
            span_id: vec![],
            event_name: String::new(),
        };

        log_records.push(log_record);

        // Send batch when we reach max size
        if log_records.len() >= MAX_BATCH_SIZE {
            let batch = LogRecordBatch {
                count: log_records.len() as u64,
                log_records: std::mem::replace(
                    &mut log_records,
                    Vec::with_capacity(MAX_BATCH_SIZE),
                ),
            };
            let _ = records_tx.send_blocking(batch);
        }
    });

    if let Err(e) = parse_result {
        error!("Error reading file {:?}: {}", work.path, e);
    }

    let new_offset = reader.offset();

    // Send any remaining log records
    if !log_records.is_empty() {
        let batch = LogRecordBatch {
            count: log_records.len() as u64,
            log_records,
        };
        let _ = ctx.records_tx.send_blocking(batch);
    }

    // Send offset update back to coordinator
    let result = FileWorkResult {
        file_id: work.file_id,
        new_offset,
        reached_eof: reader.is_eof(),
    };
    let _ = ctx.results_tx.send_blocking(result);
}

/// State of a tracked file with respect to rotation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileState {
    /// File is at its original path (matches glob pattern)
    Active,
    /// File was rotated (renamed away from glob pattern), still draining
    Rotated,
    /// File reached EOF while rotated, waiting for rotate_wait to expire
    RotatedEof { eof_time: Instant },
}

/// Tracked state for a file, keeping the file handle open for inode-based tracking
struct TrackedFile {
    /// The unique file identity (device + inode)
    file_id: FileId,
    /// Open file handle (kept open to maintain access after rotation)
    #[allow(dead_code)]
    file: File,
    /// Current path of the file (updated when rotation is detected)
    path: PathBuf,
    /// Current read offset
    offset: u64,
    /// Current state of the file
    state: FileState,
    /// Generation counter for detecting stale entries (only used for Active files)
    generation: u64,
    /// Whether work is currently in flight for this file (prevents duplicate dispatches)
    in_flight: bool,
}

/// Coordinator that manages file watching and dispatches work to workers.
/// All state updates happen here (single writer pattern).
struct FileCoordinator {
    config: FileReceiverConfig,
    finder: FileFinder,
    persister: JsonFilePersister,
    /// Map from FileId to tracked file state (inode-based tracking)
    tracked_files: HashMap<FileId, TrackedFile>,
    watcher: Box<dyn FileWatcher + Send>,
    watched_dirs: Vec<PathBuf>,
    first_check: bool,
    // Checkpoint batching state
    checkpoint_config: CheckpointConfig,
    records_since_checkpoint: u64,
    last_checkpoint: Instant,
    // Channel to send work items to the async handler
    work_tx: BoundedSender<FileWorkItem>,
    // Channel to receive offset updates from workers
    results_rx: BoundedReceiver<FileWorkResult>,
}

impl FileCoordinator {
    /// Load previously saved file states from the persister.
    /// We use FileId (device + inode) to match persisted state to current files.
    fn load_state(&mut self) -> Result<()> {
        self.persister.load()?;

        if let Some(state) = self.persister.get_json::<PersistedState>(KNOWN_FILES_KEY) {
            debug!("Loaded {} known files from persister", state.files.len());

            // Find current files and try to match them to persisted state
            let paths = self.finder.find_files().unwrap_or_default();

            for path in paths {
                // Open file and get FileId
                let file = match File::open(&path) {
                    Ok(f) => f,
                    Err(_) => continue,
                };

                let file_id = match FileId::from_file(&file) {
                    Ok(id) => id,
                    Err(_) => continue,
                };

                // Try to find matching persisted state by FileId
                let matched_offset = state.files.iter().find_map(|persisted| {
                    let persisted_id = FileId::new(persisted.dev, persisted.ino);
                    if file_id == persisted_id {
                        Some(persisted.offset)
                    } else {
                        None
                    }
                });

                if let Some(offset) = matched_offset {
                    debug!("Restored file {:?} at offset {}", path, offset);
                    self.tracked_files.insert(
                        file_id,
                        TrackedFile {
                            file_id,
                            file,
                            path: path.clone(),
                            offset,
                            state: FileState::Active,
                            generation: 0,
                            in_flight: false,
                        },
                    );
                }
            }
        }

        Ok(())
    }

    /// Save current file states to the persister (unconditional, always writes to disk).
    /// We persist FileId (device + inode) to match files across restarts.
    fn save_state(&mut self) -> Result<()> {
        let state = PersistedState {
            files: self
                .tracked_files
                .values()
                .map(|t| PersistedFileState {
                    dev: t.file_id.dev(),
                    ino: t.file_id.ino(),
                    offset: t.offset,
                })
                .collect(),
        };

        self.persister.set_json(KNOWN_FILES_KEY, &state)?;
        self.persister.sync()?;

        // Reset checkpoint tracking
        self.records_since_checkpoint = 0;
        self.last_checkpoint = Instant::now();

        Ok(())
    }

    /// Check if we should checkpoint based on records processed and time elapsed
    fn should_checkpoint(&self) -> bool {
        self.records_since_checkpoint >= self.checkpoint_config.max_records
            || self.last_checkpoint.elapsed() >= self.checkpoint_config.max_interval
    }

    /// Conditionally save state if checkpoint thresholds are met
    fn maybe_checkpoint(&mut self) -> Result<()> {
        if self.should_checkpoint() {
            self.save_state()?;
        }
        Ok(())
    }

    /// Ensure directories are being watched
    fn setup_watches(&mut self) -> Result<()> {
        // Get all matching files
        let paths = self.finder.find_files()?;

        // Extract unique parent directories
        let mut dirs_to_watch: Vec<PathBuf> = paths
            .iter()
            .filter_map(|p| p.parent())
            .map(|p| p.to_path_buf())
            .collect();
        dirs_to_watch.sort();
        dirs_to_watch.dedup();

        // Also add directories from glob patterns that might not have matching files yet
        for pattern in &self.config.include {
            let pattern_path = Path::new(pattern);
            let mut dir = PathBuf::new();
            for component in pattern_path.components() {
                let comp_str = component.as_os_str().to_string_lossy();
                if comp_str.contains('*') || comp_str.contains('?') || comp_str.contains('[') {
                    break;
                }
                dir.push(component);
            }
            if dir.is_dir() && !dirs_to_watch.contains(&dir) {
                dirs_to_watch.push(dir);
            }
        }

        // Watch new directories
        for dir in &dirs_to_watch {
            if !self.watched_dirs.contains(dir) {
                if let Err(e) = self.watcher.watch(dir) {
                    warn!("Failed to watch directory {:?}: {}", dir, e);
                } else {
                    debug!("Watching directory: {:?}", dir);
                    self.watched_dirs.push(dir.clone());
                }
            }
        }

        // Unwatch removed directories
        self.watched_dirs.retain(|dir| {
            if !dirs_to_watch.contains(dir) {
                let _ = self.watcher.unwatch(dir);
                debug!("Stopped watching directory: {:?}", dir);
                false
            } else {
                true
            }
        });

        Ok(())
    }

    /// Process any pending results from workers (offset updates and EOF handling)
    fn process_results(&mut self) {
        let rotate_wait = self.config.rotate_wait;

        // Non-blocking drain of all available results
        while let Some(result) = self.results_rx.try_recv() {
            if let Some(tracked) = self.tracked_files.get_mut(&result.file_id) {
                tracked.offset = result.new_offset;
                tracked.in_flight = false; // Work completed, allow new dispatches
                self.records_since_checkpoint += 1;

                // Handle EOF for rotated files
                if result.reached_eof {
                    match tracked.state {
                        FileState::Rotated => {
                            // Transition to RotatedEof, start rotate_wait timer
                            tracked.state = FileState::RotatedEof {
                                eof_time: Instant::now(),
                            };
                            debug!(
                                "Rotated file {:?} reached EOF, waiting {:?} before cleanup",
                                tracked.path, rotate_wait
                            );
                        }
                        FileState::RotatedEof { .. } => {
                            // Already in RotatedEof, update the time (new data may have arrived)
                            tracked.state = FileState::RotatedEof {
                                eof_time: Instant::now(),
                            };
                        }
                        FileState::Active => {
                            // Active files reaching EOF is normal, no state change
                        }
                    }
                } else {
                    // If we got more data, reset RotatedEof back to Rotated
                    if let FileState::RotatedEof { .. } = tracked.state {
                        tracked.state = FileState::Rotated;
                    }
                }
            }
        }
    }

    /// Poll for file changes and dispatch work to workers
    fn poll(&mut self) -> Result<()> {
        // Process any pending results first
        self.process_results();

        // Find files matching our patterns
        let paths = self.finder.find_files()?;

        if self.first_check && paths.is_empty() {
            warn!(
                "No files match the configured include patterns: {:?}",
                self.config.include
            );
        }
        self.first_check = false;

        // Update watches if new directories appeared
        if let Err(e) = self.setup_watches() {
            debug!("Failed to update watches: {}", e);
        }

        // Build a set of FileIds currently at glob-matching paths
        let mut active_file_ids: HashMap<FileId, PathBuf> = HashMap::new();
        for path in &paths {
            if let Ok(file_id) = FileId::from_path(path) {
                active_file_ids.insert(file_id, path.clone());
            }
        }

        // Increment generation on all Active files (for stale detection)
        for tracked in self.tracked_files.values_mut() {
            if tracked.state == FileState::Active {
                tracked.generation += 1;
            }
        }

        // Process each glob-matching file
        for path in &paths {
            if let Err(e) = self.process_active_file(path) {
                error!("Error processing file {:?}: {}", path, e);
            }
        }

        // Mark files as rotated if they're no longer at a glob-matching path
        for tracked in self.tracked_files.values_mut() {
            if tracked.state == FileState::Active {
                if !active_file_ids.contains_key(&tracked.file_id) {
                    // File is no longer at a glob path - it was rotated
                    tracked.state = FileState::Rotated;

                    // Try to discover the new path
                    if let Ok(new_path) = get_path_from_file(&tracked.file) {
                        debug!("File rotated: {:?} -> {:?}", tracked.path, new_path);
                        tracked.path = new_path;
                    } else {
                        debug!("File {:?} rotated (new path unknown)", tracked.path);
                    }
                }
            }
        }

        // Dispatch work for rotated files (to drain remaining content)
        let rotated_file_ids: Vec<FileId> = self
            .tracked_files
            .values()
            .filter(|t| matches!(t.state, FileState::Rotated))
            .map(|t| t.file_id)
            .collect();

        for file_id in rotated_file_ids {
            if let Err(e) = self.dispatch_work_for_file(file_id) {
                debug!("Error dispatching work for rotated file: {}", e);
            }
        }

        // Remove files that have been in RotatedEof state past rotate_wait
        let rotate_wait = self.config.rotate_wait;
        self.tracked_files.retain(|_, tracked| {
            match tracked.state {
                FileState::RotatedEof { eof_time } => {
                    if eof_time.elapsed() >= rotate_wait {
                        debug!("Closing rotated file {:?} after rotate_wait", tracked.path);
                        false
                    } else {
                        true
                    }
                }
                FileState::Active => {
                    // Remove stale active files (not seen for 3+ generations)
                    tracked.generation <= 3
                }
                FileState::Rotated => true, // Keep draining
            }
        });

        // Conditionally checkpoint based on records processed and time elapsed
        if let Err(e) = self.maybe_checkpoint() {
            error!("Failed to save state: {}", e);
        }

        Ok(())
    }

    /// Process an active file (one that matches glob patterns)
    fn process_active_file(&mut self, path: &PathBuf) -> Result<()> {
        // Get FileId directly from path (no file handle needed yet)
        let file_id = match FileId::from_path(path) {
            Ok(id) => id,
            Err(e) => {
                debug!("Failed to get FileId for {:?}: {}", path, e);
                return Ok(());
            }
        };

        // Check if we're already tracking this file
        if let Some(tracked) = self.tracked_files.get_mut(&file_id) {
            // File is already tracked - reset generation and ensure it's active
            tracked.generation = 0;
            tracked.state = FileState::Active;
            tracked.path = path.clone();

            // Dispatch work
            return self.dispatch_work_for_file(file_id);
        }

        // New file - now we need to open it to keep the handle
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                debug!("Failed to open file {:?}: {}", path, e);
                return Ok(());
            }
        };

        // Skip empty files
        let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
        if file_len == 0 {
            return Ok(());
        }

        // Determine starting offset
        let offset = if self.config.start_at == StartAt::End {
            file_len
        } else {
            0
        };

        debug!("Tracking new file {:?} (FileId: {})", path, file_id);

        // Track the new file
        self.tracked_files.insert(
            file_id,
            TrackedFile {
                file_id,
                file,
                path: path.clone(),
                offset,
                state: FileState::Active,
                generation: 0,
                in_flight: false,
            },
        );

        // Dispatch work
        self.dispatch_work_for_file(file_id)
    }

    /// Dispatch work for a tracked file
    fn dispatch_work_for_file(&mut self, file_id: FileId) -> Result<()> {
        let tracked = match self.tracked_files.get_mut(&file_id) {
            Some(t) => t,
            None => return Ok(()),
        };

        // Don't dispatch if work is already in flight
        if tracked.in_flight {
            return Ok(());
        }

        // Extract file name
        let file_name = tracked
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string());

        // Create work item
        let work = FileWorkItem {
            file_id,
            path: tracked.path.clone(),
            offset: tracked.offset,
            file_name,
            max_log_size: self.config.max_log_size,
        };

        // Mark as in flight before sending
        tracked.in_flight = true;

        // Send work to async handler via bounded channel (blocks if at capacity)
        if let Err(e) = self.work_tx.send_blocking(work) {
            warn!("Failed to send work item: {}", e);
            // Reset in_flight if send failed
            if let Some(t) = self.tracked_files.get_mut(&file_id) {
                t.in_flight = false;
            }
        }

        Ok(())
    }

    /// Run the coordinator main loop
    fn run(mut self, cancel: CancellationToken) {
        // Load persisted state
        if let Err(e) = self.load_state() {
            warn!("Failed to load persisted state: {}", e);
        }

        // Initial setup and file scan
        if let Err(e) = self.setup_watches() {
            warn!("Failed to setup watches: {}", e);
        }

        // Do initial poll
        if let Err(e) = self.poll() {
            error!("Initial poll error: {}", e);
        }

        let poll_interval = self.config.poll_interval;

        // Main loop: wait for watcher events, dispatch work
        loop {
            if cancel.is_cancelled() {
                break;
            }

            // Wait for watcher events (or timeout)
            match self.watcher.recv_timeout(poll_interval) {
                Ok(events) => {
                    // Log file events
                    for event in &events {
                        if let FileEventKind::Remove = event.kind {
                            for path in &event.paths {
                                debug!("File removed: {:?}", path);
                            }
                        }
                    }

                    // Poll and dispatch work
                    if let Err(e) = self.poll() {
                        error!("Poll error: {}", e);
                    }
                }
                Err(e) => {
                    warn!("Watcher error: {}", e);
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }

        // Process any remaining results
        self.process_results();

        // Final state save
        if let Err(e) = self.save_state() {
            error!("Failed to save final state: {}", e);
        }

        info!("File coordinator stopped");
    }
}

/// Internal handler for file watching.
/// This runs on the tokio runtime and only handles sending to exporters.
/// All blocking file I/O is done on a dedicated OS thread.
struct FileHandler {
    config: FileReceiverConfig,
    logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    accepted_counter: Counter<u64>,
    refused_counter: Counter<u64>,
    tags: [KeyValue; 1],
}

impl FileHandler {
    fn new(
        config: FileReceiverConfig,
        logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    ) -> std::result::Result<Self, BoxError> {
        let accepted_counter = get_meter()
            .u64_counter("rotel_receiver_accepted_log_records")
            .with_description("Number of log records successfully pushed into the pipeline.")
            .with_unit("log_records")
            .build();

        let refused_counter = get_meter()
            .u64_counter("rotel_receiver_refused_log_records")
            .with_description("Number of log records that could not be pushed into the pipeline.")
            .with_unit("log_records")
            .build();

        Ok(Self {
            config,
            logs_output,
            accepted_counter,
            refused_counter,
            tags: [KeyValue::new("receiver", "file")],
        })
    }

    /// Run the main event loop.
    /// Spawns a coordinator thread that watches for file changes and sends work items.
    /// This async task manages worker concurrency via FuturesOrdered and sends to exporters.
    async fn run(&mut self, cancel: CancellationToken) {
        // Build parser (needs to be Arc for Send across threads)
        let parser: Option<Arc<dyn Parser + Send + Sync>> =
            match FileReceiver::build_parser(&self.config) {
                Ok(Some(p)) => Some(Arc::from(p)),
                Ok(None) => None,
                Err(e) => {
                    error!("Failed to build parser: {}", e);
                    return;
                }
            };

        // Open or create the persistence database
        let db = match JsonFileDatabase::open(&self.config.offsets_path) {
            Ok(db) => db,
            Err(e) => {
                error!("Failed to open persistence database: {}", e);
                return;
            }
        };
        let persister = db.persister("file_receiver");

        // Create watcher config
        let watcher_config = WatcherConfig {
            mode: self.config.watch_mode,
            poll_interval: self.config.poll_interval,
            debounce_interval: Duration::from_millis(50),
        };

        // Create the watcher (auto mode will try native first, fall back to poll)
        let watcher = match create_watcher(&watcher_config, &[]) {
            Ok(w) => w,
            Err(e) => {
                error!("Failed to create watcher: {}", e);
                return;
            }
        };

        info!(
            backend = watcher.backend_name(),
            native = watcher.is_native(),
            "File watcher initialized"
        );

        // Get max concurrent files from config (or use default)
        let max_concurrent_files = if self.config.max_concurrent_files > 0 {
            self.config.max_concurrent_files
        } else {
            DEFAULT_MAX_CONCURRENT_WORKERS
        };

        // Create channels using flume-based bounded_channel which supports both
        // blocking and async operations on the same channel - no bridge thread needed.
        //
        // - work_tx/rx: coordinator -> async handler (work items to process)
        // - records_tx/rx: workers -> async handler (log records for export)
        // - results_tx/rx: workers -> coordinator (offset updates)
        let (work_tx, mut work_rx) = bounded_channel::bounded::<FileWorkItem>(max_concurrent_files);
        let (records_tx, mut records_rx) = bounded_channel::bounded::<LogRecordBatch>(100);
        let (results_tx, results_rx) =
            bounded_channel::bounded::<FileWorkResult>(max_concurrent_files);

        // Create worker context (shared by all workers)
        let worker_ctx = WorkerContext {
            parser,
            include_file_name: self.config.include_file_name,
            include_file_path: self.config.include_file_path,
            records_tx,
            results_tx,
        };

        // Create the coordinator
        let finder = FileFinder::new(self.config.include.clone(), self.config.exclude.clone());
        let coordinator = FileCoordinator {
            config: self.config.clone(),
            finder,
            persister,
            tracked_files: HashMap::new(),
            watcher,
            watched_dirs: Vec::new(),
            first_check: true,
            checkpoint_config: CheckpointConfig::default(),
            records_since_checkpoint: 0,
            last_checkpoint: Instant::now(),
            work_tx,
            results_rx,
        };

        // Spawn the coordinator on a dedicated OS thread
        let coord_cancel = cancel.clone();
        let coord_handle = std::thread::spawn(move || {
            coordinator.run(coord_cancel);
        });

        // Track in-flight worker tasks with FuturesOrdered
        let mut worker_futures: FuturesOrdered<JoinHandle<()>> = FuturesOrdered::new();

        // Main event loop - flume channels support async recv directly, no bridge needed
        loop {
            select! {
                _ = cancel.cancelled() => {
                    debug!("File receiver cancelled, stopping");
                    break;
                }

                // Receive work items and spawn workers (with concurrency limit)
                Some(work) = work_rx.next(), if worker_futures.len() < max_concurrent_files => {
                    let ctx = worker_ctx.clone();
                    let handle = tokio::task::spawn_blocking(move || {
                        process_file_work(work, ctx);
                    });
                    worker_futures.push_back(handle);
                }

                // Process completed workers
                Some(result) = worker_futures.next(), if !worker_futures.is_empty() => {
                    if let Err(e) = result {
                        error!("Worker task failed: {}", e);
                    }
                }

                // Receive log records from workers and send to exporters
                Some(batch) = records_rx.next() => {
                    if !batch.log_records.is_empty() {
                        if let Some(ref logs_output) = self.logs_output {
                            // Build ResourceLogs directly from LogRecords
                            let scope_logs = ScopeLogs {
                                scope: Some(InstrumentationScope {
                                    name: "rotel.file".to_string(),
                                    version: String::new(),
                                    attributes: vec![],
                                    dropped_attributes_count: 0,
                                }),
                                log_records: batch.log_records,
                                schema_url: String::new(),
                            };

                            let resource_logs = ResourceLogs {
                                resource: Some(Resource {
                                    attributes: vec![],
                                    dropped_attributes_count: 0,
                                    entity_refs: vec![],
                                }),
                                scope_logs: vec![scope_logs],
                                schema_url: String::new(),
                            };

                            let payload_msg = payload::Message::new(None, vec![resource_logs]);

                            match logs_output.send(payload_msg).await {
                                Ok(_) => {
                                    self.accepted_counter.add(batch.count, &self.tags);
                                }
                                Err(e) => {
                                    self.refused_counter.add(batch.count, &self.tags);
                                    error!("Failed to send logs: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Wait for remaining workers to complete
        while let Some(result) = worker_futures.next().await {
            if let Err(e) = result {
                error!("Worker task failed during shutdown: {}", e);
            }
        }

        // Wait for coordinator thread to finish
        drop(records_rx);
        drop(work_rx);
        let _ = coord_handle.join();

        info!("File receiver stopped");
    }
}

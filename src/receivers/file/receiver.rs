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
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tower::BoxError;
use tracing::{debug, error, info, warn};

use crate::receivers::file::config::{FileReceiverConfig, ParserType};
use crate::receivers::file::error::{Error, Result};
use crate::receivers::file::input::{FileFinder, FileReader, Fingerprint, StartAt};
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
    fingerprint_bytes: Vec<u8>,
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
    /// Path to the file to process
    path: PathBuf,
    /// Starting offset in the file
    offset: u64,
    /// Fingerprint bytes for this file (used for offset updates)
    fingerprint_bytes: Vec<u8>,
    /// File name (cached to avoid re-extracting)
    file_name: Option<String>,
    /// Maximum log line size
    max_log_size: usize,
}

/// Result sent from workers back to coordinator
struct FileWorkResult {
    /// Fingerprint bytes identifying the file
    fingerprint_bytes: Vec<u8>,
    /// New offset after reading
    new_offset: u64,
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
    records_tx: mpsc::Sender<LogRecordBatch>,
    /// Channel to send offset updates back to coordinator
    results_tx: std::sync::mpsc::Sender<FileWorkResult>,
}

/// Maximum number of log records to accumulate before sending a batch
const MAX_BATCH_SIZE: usize = 100;

/// Default maximum number of concurrent file processing workers
const DEFAULT_MAX_CONCURRENT_WORKERS: usize = 64;

/// Process a single file work item (runs in spawn_blocking)
fn process_file_work(work: FileWorkItem, ctx: WorkerContext) {
    // Verify the file exists (we don't need to keep the handle, FileReader will open it)
    if let Err(e) = File::open(&work.path) {
        debug!("Failed to open file {:?}: {}", work.path, e);
        return;
    }

    // Create a temporary reader just for this work item
    let fp = Fingerprint::from_bytes(work.fingerprint_bytes.clone());
    let mut reader = match FileReader::new(
        &work.path,
        fp,
        work.offset,
        work.fingerprint_bytes.len(), // fingerprint_size not needed for reading
        work.max_log_size,
    ) {
        Ok(r) => r,
        Err(e) => {
            debug!("Failed to create reader for {:?}: {}", work.path, e);
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
            let _ = records_tx.blocking_send(batch);
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
        let _ = ctx.records_tx.blocking_send(batch);
    }

    // Send offset update back to coordinator
    let result = FileWorkResult {
        fingerprint_bytes: work.fingerprint_bytes,
        new_offset,
    };
    let _ = ctx.results_tx.send(result);
}

/// Tracked state for a file (lightweight, no file handle)
struct TrackedFile {
    /// Current read offset
    offset: u64,
    /// Generation counter for detecting stale entries
    generation: u64,
}

/// Coordinator that manages file watching and dispatches work to workers.
/// All state updates happen here (single writer pattern).
struct FileCoordinator {
    config: FileReceiverConfig,
    finder: FileFinder,
    persister: JsonFilePersister,
    /// Map from fingerprint bytes to tracked file state
    tracked_files: HashMap<Vec<u8>, TrackedFile>,
    watcher: Box<dyn FileWatcher + Send>,
    watched_dirs: Vec<PathBuf>,
    first_check: bool,
    // Checkpoint batching state
    checkpoint_config: CheckpointConfig,
    records_since_checkpoint: u64,
    last_checkpoint: Instant,
    // Channel to send work items to the async handler
    work_tx: std::sync::mpsc::SyncSender<FileWorkItem>,
    // Channel to receive offset updates from workers
    results_rx: std::sync::mpsc::Receiver<FileWorkResult>,
}

impl FileCoordinator {
    /// Load previously saved file states from the persister
    fn load_state(&mut self) -> Result<()> {
        self.persister.load()?;

        if let Some(state) = self.persister.get_json::<PersistedState>(KNOWN_FILES_KEY) {
            debug!("Loaded {} known files from persister", state.files.len());
            for file_state in state.files {
                self.tracked_files.insert(
                    file_state.fingerprint_bytes,
                    TrackedFile {
                        offset: file_state.offset,
                        generation: 0,
                    },
                );
            }
        }

        Ok(())
    }

    /// Save current file states to the persister (unconditional, always writes to disk)
    fn save_state(&mut self) -> Result<()> {
        let state = PersistedState {
            files: self
                .tracked_files
                .iter()
                .filter(|(k, _)| !k.is_empty())
                .map(|(k, v)| PersistedFileState {
                    fingerprint_bytes: k.clone(),
                    offset: v.offset,
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

    /// Find a tracked file key that matches the given fingerprint
    fn find_matching_tracked_key(&self, fp: &Fingerprint) -> Option<Vec<u8>> {
        for key in self.tracked_files.keys() {
            let existing_fp = Fingerprint::from_bytes(key.clone());
            if fp.starts_with(&existing_fp) {
                return Some(key.clone());
            }
        }
        None
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

    /// Process any pending results from workers (offset updates)
    fn process_results(&mut self) {
        // Non-blocking drain of all available results
        while let Ok(result) = self.results_rx.try_recv() {
            if let Some(tracked) = self.tracked_files.get_mut(&result.fingerprint_bytes) {
                tracked.offset = result.new_offset;
                self.records_since_checkpoint += 1; // Approximate, actual count comes from batch
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

        // Increment generation on all tracked files
        for tracked in self.tracked_files.values_mut() {
            tracked.generation += 1;
        }

        // Process each file and dispatch work
        for path in paths {
            if let Err(e) = self.dispatch_file_work(&path) {
                error!("Error dispatching work for file {:?}: {}", path, e);
            }
        }

        // Remove tracked files that haven't been seen for more than 3 generations
        self.tracked_files
            .retain(|_, tracked| tracked.generation <= 3);

        // Conditionally checkpoint based on records processed and time elapsed
        if let Err(e) = self.maybe_checkpoint() {
            error!("Failed to save state: {}", e);
        }

        Ok(())
    }

    /// Dispatch work for a single file to a worker
    fn dispatch_file_work(&mut self, path: &PathBuf) -> Result<()> {
        // Open the file to get fingerprint
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                debug!("Failed to open file {:?}: {}", path, e);
                return Ok(());
            }
        };

        // Get fingerprint
        let fp = match Fingerprint::new(&mut file, self.config.fingerprint_size) {
            Ok(fp) => fp,
            Err(e) => {
                debug!("Failed to create fingerprint for {:?}: {}", path, e);
                return Ok(());
            }
        };

        // Skip empty files
        if fp.is_empty() {
            return Ok(());
        }

        let key = fp.bytes().to_vec();
        let existing_key = self.find_matching_tracked_key(&fp);

        let (offset, fingerprint_bytes) = if let Some(existing_key) = existing_key {
            // Existing file - get current offset and reset generation
            if let Some(tracked) = self.tracked_files.get_mut(&existing_key) {
                tracked.generation = 0;
                (tracked.offset, existing_key)
            } else {
                return Ok(());
            }
        } else {
            // New file - determine starting offset
            let offset = if self.config.start_at == StartAt::End {
                file.metadata().map(|m| m.len()).unwrap_or(0)
            } else {
                0
            };

            // Track the new file
            self.tracked_files.insert(
                key.clone(),
                TrackedFile {
                    offset,
                    generation: 0,
                },
            );

            (offset, key)
        };

        // Extract file name
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string());

        // Create work item
        let work = FileWorkItem {
            path: path.clone(),
            offset,
            fingerprint_bytes,
            file_name,
            max_log_size: self.config.max_log_size,
        };

        // Send work to async handler via bounded channel (blocks if at capacity)
        if let Err(e) = self.work_tx.send(work) {
            warn!("Failed to send work item: {}", e);
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

        // Create channels:
        // - work_tx/rx: coordinator -> async handler (work items to process)
        // - records_tx/rx: workers -> async handler (log records for export)
        // - results_tx/rx: workers -> coordinator (offset updates)
        //
        // Use std::sync::mpsc for coordinator channels (blocking sends from OS thread)
        // Use tokio::sync::mpsc for worker -> async handler (async receives)
        let (work_tx, work_rx) =
            std::sync::mpsc::sync_channel::<FileWorkItem>(max_concurrent_files);
        let (records_tx, mut records_rx) = mpsc::channel::<LogRecordBatch>(100);
        let (results_tx, results_rx) = std::sync::mpsc::channel::<FileWorkResult>();

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

        // Wrap work_rx in a tokio task that bridges sync -> async
        let (async_work_tx, mut async_work_rx) =
            mpsc::channel::<FileWorkItem>(max_concurrent_files);
        let work_bridge_cancel = cancel.clone();
        let work_bridge_handle = std::thread::spawn(move || {
            // Bridge from std::sync::mpsc to tokio::sync::mpsc
            loop {
                if work_bridge_cancel.is_cancelled() {
                    break;
                }
                match work_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(work) => {
                        if async_work_tx.blocking_send(work).is_err() {
                            break;
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        });

        // Track in-flight worker tasks with FuturesOrdered
        let mut worker_futures: FuturesOrdered<JoinHandle<()>> = FuturesOrdered::new();

        // Main event loop
        loop {
            select! {
                _ = cancel.cancelled() => {
                    debug!("File receiver cancelled, stopping");
                    break;
                }

                // Receive work items and spawn workers (with concurrency limit)
                Some(work) = async_work_rx.recv(), if worker_futures.len() < max_concurrent_files => {
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
                Some(batch) = records_rx.recv() => {
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

        // Wait for bridge and coordinator threads to finish
        drop(records_rx);
        drop(async_work_rx);
        let _ = work_bridge_handle.join();
        let _ = coord_handle.join();

        info!("File receiver stopped");
    }
}

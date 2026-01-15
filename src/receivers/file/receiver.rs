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
use std::sync::atomic::{AtomicBool, Ordering};
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
use crate::receivers::file::input::{
    FileFinder, FileId, FileReader, GlobFileFinder, StartAt, get_path_from_file,
};
use crate::receivers::file::parser::{JsonParser, Parser, RegexParser, nginx};
use crate::receivers::file::persistence::{
    JsonFileDatabase, JsonFilePersister, Persister, PersisterExt,
};
use crate::receivers::file::watcher::{
    AnyWatcher, FileEventKind, FileWatcher, PollWatcher, WatcherConfig, create_watcher,
};
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
    /// Cloned file handle (avoids reopening the file in workers)
    file: File,
    /// Path to the file (for logging and attributes)
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
    /// Maximum number of log records to accumulate before sending a batch
    max_batch_size: usize,
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

/// Default maximum number of concurrent file processing workers
const DEFAULT_MAX_CONCURRENT_WORKERS: usize = 64;

/// Process a single file work item (runs in spawn_blocking)
// Attribute key constants to avoid per-line allocations
const ATTR_LOG_FILE_NAME: &str = "log.file.name";
const ATTR_LOG_FILE_PATH: &str = "log.file.path";

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
            let mut handler = FileWorkHandler::new(config, logs_output)?;
            handler.run(cancel).await
        });

        Ok(())
    }
}

/// Internal handler for file watching.
/// This runs on the tokio runtime and only handles sending to exporters.
/// All blocking file I/O is done on a dedicated OS thread.
struct FileWorkHandler {
    config: FileReceiverConfig,
    logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    accepted_counter: Counter<u64>,
    refused_counter: Counter<u64>,
    tags: [KeyValue; 1],
}

impl FileWorkHandler {
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
    async fn run(&mut self, cancel: CancellationToken) -> std::result::Result<(), BoxError> {
        // Build parser (needs to be Arc for Send across threads)
        let parser: Option<Arc<dyn Parser + Send + Sync>> =
            match FileReceiver::build_parser(&self.config) {
                Ok(Some(p)) => Some(Arc::from(p)),
                Ok(None) => None,
                Err(e) => {
                    error!("Failed to build parser: {}", e);
                    return Err(e.into());
                }
            };

        // Open or create the persistence database
        let db = match JsonFileDatabase::open(&self.config.offsets_path) {
            Ok(db) => db,
            Err(e) => {
                error!("Failed to open persistence database: {}", e);
                return Err(e.into());
            }
        };
        let persister = db.persister("file_receiver");

        // Create watcher config
        let watcher_config = WatcherConfig {
            mode: self.config.watch_mode,
            poll_interval: self.config.poll_interval,
            debounce_interval: self.config.debounce_interval,
        };

        // Create the watcher (auto mode will try native first, fall back to poll)
        let watcher = match create_watcher(&watcher_config, &[]) {
            Ok(w) => w,
            Err(e) => {
                error!("Failed to create watcher: {}", e);
                return Err(e.into());
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

        // Create channels using flume-based bounded_channel
        //
        // - work_tx/rx: coordinator -> async handler (work items to process)
        // - records_tx/rx: workers -> async handler (log records for export)
        // - results_tx/rx: workers -> coordinator (offset updates)
        // - workers_done_tx/rx: async handler -> coordinator (shutdown signal)
        let (work_tx, mut work_rx) = bounded_channel::bounded::<FileWorkItem>(max_concurrent_files);
        let (records_tx, mut records_rx) = bounded_channel::bounded::<LogRecordBatch>(10);
        let (results_tx, results_rx) = bounded_channel::bounded::<FileWorkResult>(10);
        let (workers_done_tx, workers_done_rx) = std::sync::mpsc::channel::<()>();

        // Create worker context (shared by all workers)
        let worker_ctx = WorkerContext {
            parser,
            include_file_name: self.config.include_file_name,
            include_file_path: self.config.include_file_path,
            records_tx,
            results_tx,
            max_batch_size: self.config.max_batch_size,
        };

        // Create the coordinator
        let finder = GlobFileFinder::new(self.config.include.clone(), self.config.exclude.clone())?;
        let shutting_down = Arc::new(AtomicBool::new(false));
        let shutting_down_async = shutting_down.clone();
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
            checkpoint_first_failure: None,
            poll_first_failure: None,
            watcher_first_error: None,
            work_tx: Some(work_tx),
            results_rx,
            workers_done_rx,
            shutting_down,
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
                    // Set shutting_down before breaking so coordinator sees it
                    shutting_down_async.store(true, Ordering::SeqCst);
                    debug!("File receiver cancelled, stopping");
                    break;
                }

                // Receive work items and spawn workers (with concurrency limit)
                Some(work) = work_rx.next(), if worker_futures.len() < max_concurrent_files => {
                    let current_workers = worker_futures.len();
                    info!(
                        file_id = %work.file_id,
                        path = ?work.path,
                        current_workers = current_workers,
                        max_workers = max_concurrent_files,
                        "Spawning worker for file"
                    );
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

                            let payload_msg = payload::Message::new(None, vec![resource_logs], None);

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

        // Drain workers and records with timeouts
        // Note: We must drop worker_ctx after workers drain so records_tx closes
        self.drain_shutdown(
            &mut worker_futures,
            &mut records_rx,
            workers_done_tx,
            worker_ctx,
        )
        .await;

        // Wait for coordinator thread to finish
        drop(records_rx);
        drop(work_rx);

        // Join coordinator with timeout
        let coord_join_result = tokio::task::spawn_blocking(move || coord_handle.join()).await;
        match coord_join_result {
            Ok(Ok(())) => debug!("Coordinator thread joined successfully"),
            Ok(Err(_)) => {
                error!("Coordinator thread panicked");
                return Err("Coordinator thread panicked".into());
            }
            Err(e) => {
                error!("Failed to join coordinator: {}", e);
                return Err(e.into());
            }
        }

        info!("File receiver stopped");
        Ok(())
    }

    /// Drain in-flight workers and remaining log records during shutdown
    async fn drain_shutdown(
        &mut self,
        worker_futures: &mut FuturesOrdered<JoinHandle<()>>,
        records_rx: &mut BoundedReceiver<LogRecordBatch>,
        workers_done_tx: std::sync::mpsc::Sender<()>,
        worker_ctx: WorkerContext,
    ) {
        use tokio::time::{Instant, timeout_at};

        let worker_deadline = Instant::now() + self.config.shutdown_worker_drain_timeout;
        let records_deadline = Instant::now() + self.config.shutdown_records_drain_timeout;

        // Phase 1: Wait for workers to complete (they produce both results and records)
        let worker_count = worker_futures.len();
        if worker_count > 0 {
            debug!("Draining {} in-flight workers", worker_count);
        }

        loop {
            if worker_futures.is_empty() {
                break;
            }

            match timeout_at(worker_deadline, worker_futures.next()).await {
                Ok(Some(result)) => {
                    if let Err(e) = result {
                        error!("Worker task failed during drain: {}", e);
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    warn!(
                        "Timeout waiting for workers, {} still running",
                        worker_futures.len()
                    );
                    break;
                }
            }
        }

        // Signal coordinator that workers are done so it can drain results and save state
        debug!("Workers drained, signaling coordinator");
        let _ = workers_done_tx.send(());

        // Drop worker_ctx to close records_tx, allowing records_rx to return None when drained
        drop(worker_ctx);

        // Phase 2: Drain log records channel and send to pipeline
        debug!("Draining remaining log records");
        let mut drained_count = 0u64;

        loop {
            match timeout_at(records_deadline, records_rx.next()).await {
                Ok(Some(batch)) => {
                    if !batch.log_records.is_empty() {
                        drained_count += batch.count;
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

                            let payload_msg =
                                payload::Message::new(None, vec![resource_logs], None);

                            match logs_output.send(payload_msg).await {
                                Ok(_) => {
                                    self.accepted_counter.add(batch.count, &self.tags);
                                }
                                Err(e) => {
                                    self.refused_counter.add(batch.count, &self.tags);
                                    error!("Failed to send logs during drain: {}", e);
                                }
                            }
                        }
                    }
                }
                Ok(None) => break, // Channel closed
                Err(_) => {
                    warn!("Timeout draining records channel");
                    break;
                }
            }
        }

        if drained_count > 0 {
            debug!("Drained {} log records during shutdown", drained_count);
        }
    }
}

fn process_file_work(work: FileWorkItem, ctx: WorkerContext) {
    info!(
        file_id = %work.file_id,
        path = ?work.path,
        start_offset = work.offset,
        "Worker starting file processing"
    );

    // Pre-build static attributes outside the loop (file name and path don't change per line)
    let mut static_attributes = Vec::with_capacity(2);

    if ctx.include_file_name {
        if let Some(ref name) = work.file_name {
            static_attributes.push(OtlpKeyValue {
                key: ATTR_LOG_FILE_NAME.to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(name.clone())),
                }),
            });
        }
    }

    if ctx.include_file_path {
        static_attributes.push(OtlpKeyValue {
            key: ATTR_LOG_FILE_PATH.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    work.path.display().to_string(),
                )),
            }),
        });
    }

    // Create a reader using the cloned file handle (avoids reopening)
    let mut reader = FileReader::from_file(work.file, work.path, work.offset, work.max_log_size);

    // Read lines and build LogRecords, sending in small batches
    let max_batch_size = ctx.max_batch_size;
    let mut log_records = Vec::with_capacity(max_batch_size);

    let records_tx = &ctx.records_tx;

    let parse_result = reader.read_lines_into(|line| {
        // Get timestamp for each line individually
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Clone static attributes and extend with parsed attributes
        let mut attributes = static_attributes.clone();

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
        if log_records.len() >= max_batch_size {
            let batch = LogRecordBatch {
                count: log_records.len() as u64,
                log_records: std::mem::replace(
                    &mut log_records,
                    Vec::with_capacity(max_batch_size),
                ),
            };
            if records_tx.send_blocking(batch).is_err() {
                // Channel closed - we're shutting down, remaining lines will be discarded
                debug!("Records channel closed during batch send, discarding remaining lines");
            }
        }
    });

    if let Err(e) = parse_result {
        error!("Error reading file {:?}: {}", reader.path(), e);
    }

    let new_offset = reader.offset();

    // Send any remaining log records
    if !log_records.is_empty() {
        let batch = LogRecordBatch {
            count: log_records.len() as u64,
            log_records,
        };
        if ctx.records_tx.send_blocking(batch).is_err() {
            // Channel closed - we're shutting down, final batch discarded
            debug!("Records channel closed, discarding final batch");
        }
    }

    // Send offset update back to coordinator
    let reached_eof = reader.is_eof();
    info!(
        file_id = %work.file_id,
        start_offset = work.offset,
        end_offset = new_offset,
        bytes_read = new_offset - work.offset,
        reached_eof = reached_eof,
        "Worker finished file processing"
    );

    let result = FileWorkResult {
        file_id: work.file_id,
        new_offset,
        reached_eof,
    };
    if ctx.results_tx.send_blocking(result).is_err() {
        debug!("Results channel closed, offset update discarded");
    }
}

/// Coordinator that manages file watching and dispatches work to workers.
/// All state updates happen here (single writer pattern).
///
/// Generic over:
/// - `F`: FileFinder implementation for discovering files
struct FileCoordinator<F: FileFinder> {
    config: FileReceiverConfig,
    finder: F,
    persister: JsonFilePersister,
    /// Map from FileId to tracked file state (inode-based tracking)
    tracked_files: HashMap<FileId, TrackedFile>,
    watcher: AnyWatcher,
    watched_dirs: Vec<PathBuf>,
    first_check: bool,
    // Checkpoint batching state
    checkpoint_config: CheckpointConfig,
    records_since_checkpoint: u64,
    last_checkpoint: Instant,
    // Error tracking for threshold-based exit
    checkpoint_first_failure: Option<Instant>,
    poll_first_failure: Option<Instant>,
    watcher_first_error: Option<Instant>,
    // Channel to send work items to the async handler (Option for early drop during shutdown)
    work_tx: Option<BoundedSender<FileWorkItem>>,
    // Channel to receive offset updates from workers
    results_rx: BoundedReceiver<FileWorkResult>,
    // Channel to receive "workers done" signal from async handler during shutdown
    workers_done_rx: std::sync::mpsc::Receiver<()>,
    // Flag to indicate shutdown is in progress (for conditional logging)
    shutting_down: Arc<AtomicBool>,
}

impl<F: FileFinder> FileCoordinator<F> {
    /// Load previously saved file states from the persister.
    /// We use FileId (device + inode) to match persisted state to current files.
    /// Returns an error if the persisted state exists but is corrupted (to prevent data loss/duplicates).
    fn load_state(&mut self) -> Result<()> {
        self.persister.load()?;

        // Use try_get_json to distinguish "key doesn't exist" from "key exists but corrupted"
        let state = match self
            .persister
            .try_get_json::<PersistedState>(KNOWN_FILES_KEY)
        {
            Ok(Some(state)) => state,
            Ok(None) => {
                debug!("No persisted state found, starting fresh");
                return Ok(());
            }
            Err(e) => {
                // State file exists but is corrupted - this is a fatal error
                // Continuing would risk data loss (skipping already-processed logs)
                // or duplicates (reprocessing logs we already sent)
                return Err(Error::Persistence(format!(
                    "persisted state is corrupted and cannot be loaded: {}. \
                     To start fresh, delete the offsets file and restart.",
                    e
                )));
            }
        };

        debug!("Loaded {} known files from persister", state.files.len());

        // Find current files and try to match them to persisted state
        let paths = self.finder.find_files().unwrap_or_else(|e| {
            error!("Failed to find files during state load: {}", e);
            vec![]
        });

        for path in paths {
            // Open file and get FileId
            let file = match File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    debug!("Failed to open file {:?} during state load: {}", path, e);
                    continue;
                }
            };

            let file_id = match FileId::from_file(&file) {
                Ok(id) => id,
                Err(e) => {
                    warn!("Failed to get FileId for {:?}: {}", path, e);
                    continue;
                }
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

    /// Conditionally save state if checkpoint thresholds are met.
    /// Tracks consecutive failures and returns Err only when failure duration threshold is breached.
    fn maybe_checkpoint(&mut self) -> Result<()> {
        if !self.should_checkpoint() {
            return Ok(());
        }

        match self.save_state() {
            Ok(()) => {
                // Success - reset failure tracking
                if self.checkpoint_first_failure.is_some() {
                    debug!("Checkpoint succeeded after previous failures");
                    self.checkpoint_first_failure = None;
                }
                Ok(())
            }
            Err(e) => {
                // Track when failures started
                let first_failure = *self
                    .checkpoint_first_failure
                    .get_or_insert_with(Instant::now);

                let failure_duration = first_failure.elapsed();

                if failure_duration >= self.config.max_checkpoint_failure_duration {
                    error!(
                        "Checkpoint failures persisted for {:?}, exiting: {}",
                        failure_duration, e
                    );
                    Err(e)
                } else {
                    warn!(
                        "Checkpoint failed (failures started {:?} ago): {}",
                        failure_duration, e
                    );
                    Ok(())
                }
            }
        }
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
                    warn!(
                        "Failed to watch directory {:?}: {} - will rely on polling",
                        dir, e
                    );
                } else {
                    debug!("Watching directory: {:?}", dir);
                    self.watched_dirs.push(dir.clone());
                }
            }
        }

        // Unwatch removed directories
        self.watched_dirs.retain(|dir| {
            if !dirs_to_watch.contains(dir) {
                if let Err(e) = self.watcher.unwatch(dir) {
                    warn!("Failed to unwatch directory {:?}: {}", dir, e);
                }
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
        let mut results_processed = 0u32;
        while let Some(result) = self.results_rx.try_recv() {
            results_processed += 1;
            if let Some(tracked) = self.tracked_files.get_mut(&result.file_id) {
                info!(
                    file_id = %result.file_id,
                    new_offset = result.new_offset,
                    reached_eof = result.reached_eof,
                    "Worker completed, clearing in_flight"
                );
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

        if results_processed > 0 {
            let in_flight_count = self.tracked_files.values().filter(|t| t.in_flight).count();
            info!(
                results_processed = results_processed,
                remaining_in_flight = in_flight_count,
                "Finished processing worker results"
            );
        }
    }

    /// Poll for file changes and dispatch work to workers
    fn poll(&mut self) -> Result<()> {
        // Process any pending results first
        self.process_results();

        // Find files matching our patterns
        let paths = match self.finder.find_files() {
            Ok(paths) => {
                // Success - reset poll failure tracking
                if self.poll_first_failure.is_some() {
                    debug!("File discovery succeeded after previous failures");
                    self.poll_first_failure = None;
                }
                paths
            }
            Err(e) => {
                // Track when failures started
                let first_failure = *self.poll_first_failure.get_or_insert_with(Instant::now);
                let failure_duration = first_failure.elapsed();

                if failure_duration >= self.config.max_poll_failure_duration {
                    error!(
                        "File discovery failures persisted for {:?}, exiting: {}",
                        failure_duration, e
                    );
                    return Err(e);
                } else {
                    warn!(
                        "File discovery failed (failures started {:?} ago): {}",
                        failure_duration, e
                    );
                    return Ok(());
                }
            }
        };

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
            match FileId::from_path(path) {
                Ok(file_id) => {
                    active_file_ids.insert(file_id, path.clone());
                }
                Err(e) => {
                    debug!("Failed to get FileId for {:?}: {}", path, e);
                }
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
            self.process_active_file(path);
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
            self.dispatch_work_for_file(file_id);
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
        // maybe_checkpoint() only returns Err when failure duration threshold is breached
        self.maybe_checkpoint()?;

        Ok(())
    }

    /// Process an active file (one that matches glob patterns)
    fn process_active_file(&mut self, path: &PathBuf) {
        // Get FileId directly from path (no file handle needed yet)
        let file_id = match FileId::from_path(path) {
            Ok(id) => id,
            Err(e) => {
                error!("Failed to get FileId for {:?}: {}", path, e);
                return;
            }
        };

        // Check if we're already tracking this file
        if let Some(tracked) = self.tracked_files.get_mut(&file_id) {
            // File is already tracked - reset generation and ensure it's active
            tracked.generation = 0;
            tracked.state = FileState::Active;
            tracked.path = path.clone();

            // Dispatch work
            self.dispatch_work_for_file(file_id);
            return;
        }

        // New file - now we need to open it to keep the handle
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                error!("Failed to open file {:?}: {}", path, e);
                return;
            }
        };

        // Skip empty files
        let file_len = file.metadata().map(|m| m.len()).unwrap_or_else(|e| {
            debug!("Failed to get metadata for {:?}: {}", path, e);
            0
        });
        if file_len == 0 {
            return;
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
        self.dispatch_work_for_file(file_id);
    }

    /// Dispatch work for a tracked file
    fn dispatch_work_for_file(&mut self, file_id: FileId) {
        let tracked = match self.tracked_files.get_mut(&file_id) {
            Some(t) => t,
            None => return,
        };

        // Don't dispatch if work is already in flight
        if tracked.in_flight {
            return;
        }

        // Clone the file handle for the worker (avoids reopening)
        let cloned_file = match tracked.file.try_clone() {
            Ok(f) => f,
            Err(e) => {
                warn!("Failed to clone file handle for {:?}: {}", tracked.path, e);
                return;
            }
        };

        // Extract file name
        let file_name = tracked
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string());

        // Create work item
        let work = FileWorkItem {
            file_id,
            file: cloned_file,
            path: tracked.path.clone(),
            offset: tracked.offset,
            file_name,
            max_log_size: self.config.max_log_size,
        };

        // Mark as in flight before sending
        tracked.in_flight = true;

        info!(
            file_id = %file_id,
            path = ?tracked.path,
            offset = tracked.offset,
            "Dispatching work for file"
        );

        // Send work to async handler via bounded channel (blocks if at capacity)
        if let Some(ref work_tx) = self.work_tx {
            if let Err(_e) = work_tx.send_blocking(work) {
                // Channel disconnected - async handler is shutting down
                // Only warn if we're not in shutdown mode (unexpected disconnect)
                if self.shutting_down.load(Ordering::SeqCst) {
                    debug!("Work channel closed during shutdown, cannot dispatch work");
                } else {
                    warn!("Work channel unexpectedly closed, cannot dispatch work");
                }
                // Reset in_flight if send failed
                if let Some(t) = self.tracked_files.get_mut(&file_id) {
                    t.in_flight = false;
                }
            }
        } else {
            // Channel already closed (shutdown in progress)
            tracked.in_flight = false;
        }
    }

    /// Run the coordinator main loop
    fn run(mut self, cancel: CancellationToken) {
        // Load persisted state - fail if corrupted to prevent data loss/duplicates
        if let Err(e) = self.load_state() {
            error!("Failed to load persisted state: {}", e);
            return;
        }

        // Initial setup and file scan
        if let Err(e) = self.setup_watches() {
            warn!("Failed to setup watches: {} - will rely on polling", e);
        }

        // Do initial poll - exit on fatal error (threshold breached)
        if let Err(e) = self.poll() {
            error!("Initial poll failed with fatal error: {}", e);
            self.shutting_down.store(true, Ordering::SeqCst);
            // Fall through to shutdown logic
        }

        let poll_interval = self.config.poll_interval;

        // Main loop: wait for watcher events, dispatch work
        loop {
            if cancel.is_cancelled() {
                // Set shutting_down flag immediately so any in-flight dispatch logs at debug level
                self.shutting_down.store(true, Ordering::SeqCst);
                debug!("Cancellation received, beginning orderly shutdown");
                break;
            }

            // Check if we're shutting down due to fatal error
            if self.shutting_down.load(Ordering::SeqCst) {
                break;
            }

            // Wait for watcher events (or timeout)
            match self.watcher.recv_timeout(poll_interval) {
                Ok(events) => {
                    // Reset watcher error tracking on success
                    if self.watcher_first_error.is_some() {
                        debug!("Watcher recovered after previous errors");
                        self.watcher_first_error = None;
                    }

                    // Log event count for debugging coordination behavior
                    if !events.is_empty() {
                        let in_flight_count =
                            self.tracked_files.values().filter(|t| t.in_flight).count();
                        info!(
                            event_count = events.len(),
                            tracked_files = self.tracked_files.len(),
                            in_flight = in_flight_count,
                            "Watcher events received, calling poll()"
                        );
                    }

                    // Log file events
                    for event in &events {
                        if let FileEventKind::Remove = event.kind {
                            for path in &event.paths {
                                debug!("File removed: {:?}", path);
                            }
                        }
                    }

                    // Poll and dispatch work - exit on fatal error (threshold breached)
                    if let Err(e) = self.poll() {
                        error!("Poll failed with fatal error: {}", e);
                        self.shutting_down.store(true, Ordering::SeqCst);
                        break;
                    }
                }
                // TODO - we need to reevaluate the logic below here switching to PollWatcher is PollWatcher errors
                Err(e) => {
                    // Track when watcher errors started
                    let first_error = *self.watcher_first_error.get_or_insert_with(Instant::now);
                    let error_duration = first_error.elapsed();

                    if error_duration >= self.config.max_watcher_error_duration {
                        // Fall back to polling mode
                        warn!(
                            "Watcher errors persisted for {:?}, falling back to polling mode: {}",
                            error_duration, e
                        );

                        // Create a new poll watcher
                        let dirs: Vec<&Path> =
                            self.watched_dirs.iter().map(|p| p.as_path()).collect();
                        match PollWatcher::new(&dirs, self.config.poll_interval) {
                            Ok(poll_watcher) => {
                                self.watcher = AnyWatcher::Poll(poll_watcher);
                                self.watcher_first_error = None;
                                info!("Switched to polling mode");
                            }
                            Err(poll_err) => {
                                error!("Failed to create poll watcher: {}", poll_err);
                                // Continue with current watcher, will retry next iteration
                            }
                        }
                    } else {
                        warn!(
                            "Watcher error (errors started {:?} ago): {}",
                            error_duration, e
                        );
                    }

                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }

        // Close work_tx to signal no more work coming
        // Take and drop the sender so the async handler sees the channel close
        drop(self.work_tx.take());
        debug!("Work channel closed, waiting for workers to finish");

        // Wait for async handler to signal that workers are done
        let wait_timeout = Duration::from_secs(5);
        match self.workers_done_rx.recv_timeout(wait_timeout) {
            Ok(()) => debug!("Workers finished, draining final results"),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                warn!("Timeout waiting for workers to finish");
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                debug!("Workers done channel disconnected");
            }
        }

        // Now drain any remaining results
        self.drain_results();

        // Final state save
        if let Err(e) = self.save_state() {
            error!("Failed to save final state: {}", e);
        } else {
            debug!(
                "Final state saved with {} tracked files",
                self.tracked_files.len()
            );
        }

        info!("File coordinator stopped");
    }

    /// Drain results from in-flight workers during shutdown
    fn drain_results(&mut self) {
        let drain_timeout = Duration::from_secs(5); // Hardcoded for coordinator side
        let drain_deadline = Instant::now() + drain_timeout;

        loop {
            let in_flight_count = self.tracked_files.values().filter(|t| t.in_flight).count();

            if in_flight_count == 0 {
                debug!("All in-flight work completed");
                break;
            }

            if Instant::now() >= drain_deadline {
                warn!(
                    "Shutdown timeout: {} files still had in-flight work",
                    in_flight_count
                );
                break;
            }

            // Try to receive with short timeout
            match self.results_rx.recv_timeout(Duration::from_millis(100)) {
                Some(result) => {
                    if let Some(tracked) = self.tracked_files.get_mut(&result.file_id) {
                        tracked.offset = result.new_offset;
                        tracked.in_flight = false;
                        self.records_since_checkpoint += 1;
                    }
                }
                None => {
                    // Channel disconnected, workers are done
                    debug!("Results channel disconnected");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receivers::file::input::MockFileFinder;
    use crate::receivers::file::watcher::MockWatcher;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Create a test config with short thresholds for fast tests
    fn test_config(temp_dir: &TempDir) -> FileReceiverConfig {
        FileReceiverConfig {
            include: vec![format!("{}/*.log", temp_dir.path().display())],
            exclude: vec![],
            max_poll_failure_duration: Duration::from_millis(100),
            max_checkpoint_failure_duration: Duration::from_millis(100),
            max_watcher_error_duration: Duration::from_millis(100),
            poll_interval: Duration::from_millis(10),
            offsets_path: temp_dir.path().join("offsets.json"),
            ..Default::default()
        }
    }

    /// Create a minimal coordinator for testing poll failures
    fn create_test_coordinator<F: FileFinder>(
        config: FileReceiverConfig,
        finder: F,
        watcher: AnyWatcher,
        temp_dir: &TempDir,
    ) -> FileCoordinator<F> {
        let db = JsonFileDatabase::open(temp_dir.path().join("offsets.json")).unwrap();
        let persister = db.persister("test");

        let (work_tx, _work_rx) = bounded_channel::bounded::<FileWorkItem>(10);
        let (_results_tx, results_rx) = bounded_channel::bounded::<FileWorkResult>(10);
        let (_workers_done_tx, workers_done_rx) = std::sync::mpsc::channel::<()>();

        FileCoordinator {
            config,
            finder,
            persister,
            tracked_files: HashMap::new(),
            watcher,
            watched_dirs: Vec::new(),
            first_check: true,
            checkpoint_config: CheckpointConfig::default(),
            records_since_checkpoint: 0,
            last_checkpoint: Instant::now(),
            checkpoint_first_failure: None,
            poll_first_failure: None,
            watcher_first_error: None,
            work_tx: Some(work_tx),
            results_rx,
            workers_done_rx,
            shutting_down: Arc::new(AtomicBool::new(false)),
        }
    }

    #[test]
    fn test_poll_failure_threshold_exits_after_duration() {
        let temp_dir = TempDir::new().unwrap();
        let config = test_config(&temp_dir);

        // Create a finder that always fails
        let finder = MockFileFinder::fail_after(0);
        let watcher = AnyWatcher::Mock(MockWatcher::new());

        let mut coordinator = create_test_coordinator(config, finder, watcher, &temp_dir);

        // First poll should succeed (returns Ok because threshold not yet reached)
        let result = coordinator.poll();
        assert!(
            result.is_ok(),
            "First poll should return Ok (threshold not reached)"
        );

        // Wait for threshold duration
        std::thread::sleep(Duration::from_millis(150));

        // Next poll should fail (threshold reached)
        let result = coordinator.poll();
        assert!(
            result.is_err(),
            "Poll should return Err after threshold duration"
        );
    }

    #[test]
    fn test_poll_failure_resets_on_success() {
        let temp_dir = TempDir::new().unwrap();
        let config = test_config(&temp_dir);

        // Create a finder that fails after 2 successful calls
        let finder = MockFileFinder::fail_after(2);
        let watcher = AnyWatcher::Mock(MockWatcher::new());

        let mut coordinator = create_test_coordinator(config, finder, watcher, &temp_dir);

        // First two polls succeed
        assert!(coordinator.poll().is_ok());
        assert!(coordinator.poll().is_ok());

        // Third poll fails but threshold not reached
        assert!(coordinator.poll().is_ok());

        // Reset the finder to succeed again (simulating recovery)
        coordinator.finder = MockFileFinder::new();

        // Poll should succeed and reset the failure tracking
        assert!(coordinator.poll().is_ok());

        // Now make it fail again
        coordinator.finder = MockFileFinder::fail_after(0);

        // This should not immediately fail (threshold was reset)
        let result = coordinator.poll();
        assert!(result.is_ok(), "Threshold should have been reset");
    }

    #[test]
    fn test_watcher_fallback_after_error_duration() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = test_config(&temp_dir);
        config.max_watcher_error_duration = Duration::from_millis(50);
        config.poll_interval = Duration::from_millis(10);

        let finder = MockFileFinder::new();
        // Create a watcher that fails immediately
        let watcher = AnyWatcher::Mock(MockWatcher::fail_after(0));

        let mut coordinator = create_test_coordinator(config, finder, watcher, &temp_dir);

        // Verify we start with a Mock watcher
        assert_eq!(coordinator.watcher.backend_name(), "mock");

        // Simulate the main loop's watcher error handling
        let poll_interval = coordinator.config.poll_interval;
        let max_watcher_error_duration = coordinator.config.max_watcher_error_duration;

        // First error - should not fallback yet
        let _err = coordinator.watcher.recv_timeout(poll_interval).unwrap_err();
        let first_error = *coordinator
            .watcher_first_error
            .get_or_insert_with(Instant::now);
        assert!(first_error.elapsed() < max_watcher_error_duration);

        // Wait for threshold
        std::thread::sleep(Duration::from_millis(60));

        // Try again - should trigger fallback
        let _ = coordinator.watcher.recv_timeout(poll_interval);
        let error_duration = first_error.elapsed();

        if error_duration >= max_watcher_error_duration {
            // Create poll watcher (this is what the main loop does)
            let dirs: Vec<&Path> = coordinator
                .watched_dirs
                .iter()
                .map(|p| p.as_path())
                .collect();
            if let Ok(poll_watcher) = PollWatcher::new(&dirs, coordinator.config.poll_interval) {
                coordinator.watcher = AnyWatcher::Poll(poll_watcher);
                coordinator.watcher_first_error = None;
            }
        }

        // Verify we switched to poll watcher
        assert_eq!(
            coordinator.watcher.backend_name(),
            "poll",
            "Should have fallen back to poll watcher"
        );
    }

    #[test]
    fn test_checkpoint_failure_threshold() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = test_config(&temp_dir);
        config.max_checkpoint_failure_duration = Duration::from_millis(50);

        let finder = MockFileFinder::new();
        let watcher = AnyWatcher::Mock(MockWatcher::new());

        let mut coordinator = create_test_coordinator(config, finder, watcher, &temp_dir);

        // Force checkpoint to be needed
        coordinator.records_since_checkpoint = 10000;

        // Make the persister directory read-only to cause sync failures
        let offsets_dir = temp_dir.path().join("readonly");
        std::fs::create_dir_all(&offsets_dir).unwrap();
        coordinator.persister = JsonFileDatabase::open(offsets_dir.join("offsets.json"))
            .unwrap()
            .persister("test");

        // Make directory read-only (this will cause sync to fail)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&offsets_dir, std::fs::Permissions::from_mode(0o444)).unwrap();
        }

        // First checkpoint failure should not return error
        let result = coordinator.maybe_checkpoint();
        #[cfg(unix)]
        assert!(
            result.is_ok(),
            "First checkpoint failure should return Ok (threshold not reached)"
        );

        // Wait for threshold
        std::thread::sleep(Duration::from_millis(60));

        // Force another checkpoint
        coordinator.records_since_checkpoint = 10000;

        // Next checkpoint should fail
        let result = coordinator.maybe_checkpoint();
        #[cfg(unix)]
        assert!(
            result.is_err(),
            "Checkpoint should return Err after threshold duration"
        );

        // Cleanup - restore permissions
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&offsets_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    #[test]
    fn test_load_state_fails_on_corrupted_state_file() {
        let temp_dir = TempDir::new().unwrap();
        let offsets_path = temp_dir.path().join("offsets.json");

        // Write a valid JSON file structure, but with corrupted data for the knownFiles key.
        // The JSON file format has scopes -> scope_name -> key -> base64(value).
        // We'll write valid base64 that decodes to invalid JSON for the PersistedState struct.
        let corrupted_db = r#"{
            "scopes": {
                "test": {
                    "knownFiles": "bm90IHZhbGlkIGpzb24ge3t7"
                }
            }
        }"#;
        // "bm90IHZhbGlkIGpzb24ge3t7" is base64 for "not valid json {{{"
        std::fs::write(&offsets_path, corrupted_db).unwrap();

        let config = test_config(&temp_dir);
        let finder = MockFileFinder::new();
        let watcher = AnyWatcher::Mock(MockWatcher::new());

        // Create coordinator - this will open the database with corrupted data
        let db = JsonFileDatabase::open(&offsets_path).unwrap();
        let persister = db.persister("test");

        let (work_tx, _work_rx) = bounded_channel::bounded::<FileWorkItem>(10);
        let (_results_tx, results_rx) = bounded_channel::bounded::<FileWorkResult>(10);
        let (_workers_done_tx, workers_done_rx) = std::sync::mpsc::channel::<()>();

        let mut coordinator = FileCoordinator {
            config,
            finder,
            persister,
            tracked_files: HashMap::new(),
            watcher,
            watched_dirs: Vec::new(),
            first_check: true,
            checkpoint_config: CheckpointConfig::default(),
            records_since_checkpoint: 0,
            last_checkpoint: Instant::now(),
            checkpoint_first_failure: None,
            poll_first_failure: None,
            watcher_first_error: None,
            work_tx: Some(work_tx),
            results_rx,
            workers_done_rx,
            shutting_down: Arc::new(AtomicBool::new(false)),
        };

        // load_state should return an error for corrupted data
        let result = coordinator.load_state();
        assert!(
            result.is_err(),
            "load_state should return Err when state file is corrupted"
        );

        // Verify the error message is helpful
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("corrupted"),
            "Error message should mention corruption: {}",
            err_msg
        );
    }

    #[test]
    fn test_load_state_succeeds_with_no_state_file() {
        let temp_dir = TempDir::new().unwrap();
        let config = test_config(&temp_dir);

        let finder = MockFileFinder::new();
        let watcher = AnyWatcher::Mock(MockWatcher::new());

        let mut coordinator = create_test_coordinator(config, finder, watcher, &temp_dir);

        // With no state file, load_state should succeed
        let result = coordinator.load_state();
        assert!(
            result.is_ok(),
            "load_state should succeed when no state file exists"
        );
    }
}

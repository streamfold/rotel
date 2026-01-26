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
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Shared offset tracker for at-least-once delivery.
/// Uses std::sync::Mutex since it's accessed from both async and sync contexts.
pub type SharedOffsetTracker = Arc<StdMutex<FileOffsetTracker>>;

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

use crate::receivers::file::config::{FileReceiverConfig, NginxAccessFormat, ParserType};
use crate::receivers::file::error::{Error, Result};
use crate::receivers::file::input::{
    FileFinder, FileId, FileReader, GlobFileFinder, StartAt, get_path_from_file,
};
use crate::receivers::file::offset_committer::{
    FileOffsetCommitter, OffsetCommitterConfig, TrackedFileInfo,
};
use crate::receivers::file::offset_tracker::{FileOffsetTracker, LineOffset};
use crate::receivers::file::parser::{JsonParser, Parser, RegexParser, nginx};
use crate::receivers::file::persistence::{
    JsonFileDatabase, JsonFilePersister, KNOWN_FILES_KEY, PERSISTED_STATE_VERSION,
    PersistedFileEntryV1, PersistedStateV0, PersistedStateV1, Persister, PersisterExt,
    file_id_to_key,
};
use crate::receivers::file::watcher::{
    AnyWatcher, FileEventKind, FileWatcher, PollWatcher, WatcherConfig, create_watcher,
};
use crate::receivers::get_meter;
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::payload::{self, FileAcknowledgement, FileMetadata, MessageMetadata};

/// Error type for send operations with cancellation support.
#[derive(Debug)]
enum SendError {
    /// The operation was cancelled via the cancellation token.
    Cancelled,
    /// The channel was disconnected.
    ChannelClosed,
}

/// Message sent from workers to the async task
struct LogRecordBatch {
    /// The log records to export
    log_records: Vec<LogRecord>,
    /// Number of log records in this batch
    count: u64,
    /// File ID for this batch (for offset tracking)
    file_id: FileId,
    /// Line offsets (begin offset + length) for each log record (for at-least-once delivery)
    offsets: Vec<LineOffset>,
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
    /// Flag to indicate shutdown is in progress - workers should exit early
    shutting_down: Arc<AtomicBool>,
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
const DEFAULT_MAX_CONCURRENT_WORKERS: usize = 4;

/// Process a single file work item (runs in spawn_blocking)
// Attribute key constants to avoid per-line allocations
const ATTR_LOG_FILE_NAME: &str = "log.file.name";
const ATTR_LOG_FILE_PATH: &str = "log.file.path";

/// File receiver for tailing log files
pub struct FileReceiver {
    config: FileReceiverConfig,
    logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    /// Offset committer for handling acks and persistence (extracted before start)
    offset_committer: Option<FileOffsetCommitter>,
    /// Sender for acks from the pipeline (passed to handler)
    ack_tx: BoundedSender<FileAcknowledgement>,
    /// Sender for file info updates to the committer
    file_info_tx: BoundedSender<TrackedFileInfo>,
    /// Shared offset tracker
    offset_tracker: SharedOffsetTracker,
}

impl FileReceiver {
    /// Create a new file receiver
    pub async fn new(
        config: FileReceiverConfig,
        logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    ) -> std::result::Result<Self, BoxError> {
        config.validate().map_err(|e| -> BoxError { e.into() })?;

        // Open or create the persistence database
        let db = JsonFileDatabase::open(&config.offsets_path).map_err(|e| -> BoxError {
            format!(
                "Failed to open persistence database at {:?}: {}",
                config.offsets_path, e
            )
            .into()
        })?;
        let persister = db.persister("file_receiver");

        // Create channels for acks and file info
        let (ack_tx, ack_rx) = bounded_channel::bounded::<FileAcknowledgement>(100);
        let (file_info_tx, file_info_rx) = bounded_channel::bounded::<TrackedFileInfo>(100);

        // Create shared offset tracker
        let offset_tracker: SharedOffsetTracker = Arc::new(StdMutex::new(FileOffsetTracker::new(
            config.finite_retry_enabled,
        )));

        // Create the offset committer
        let committer_config = OffsetCommitterConfig {
            max_checkpoint_failure_duration: config.max_checkpoint_failure_duration,
            ..Default::default()
        };
        let offset_committer = FileOffsetCommitter::new(
            ack_rx,
            offset_tracker.clone(),
            persister,
            committer_config,
            file_info_rx,
        );

        Ok(Self {
            config,
            logs_output,
            offset_committer: Some(offset_committer),
            ack_tx,
            file_info_tx,
            offset_tracker,
        })
    }

    /// Extract the offset committer for running in a separate task.
    /// Must be called before start() - the committer handles ack processing
    /// and state persistence, and should outlive the main receiver loop.
    pub fn take_offset_committer(&mut self) -> Option<FileOffsetCommitter> {
        self.offset_committer.take()
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
            ParserType::NginxAccess => match config.nginx_access_format {
                NginxAccessFormat::Auto => {
                    let parser =
                        nginx::auto_access_parser().map_err(|e| Error::Regex(e.to_string()))?;
                    Ok(Some(Box::new(parser)))
                }
                NginxAccessFormat::Combined => {
                    let parser = nginx::access_parser().map_err(|e| Error::Regex(e.to_string()))?;
                    Ok(Some(Box::new(parser)))
                }
                NginxAccessFormat::Json => {
                    let parser = nginx::json_access_parser();
                    Ok(Some(Box::new(parser)))
                }
            },
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
        let ack_tx = self.ack_tx.clone();
        let file_info_tx = self.file_info_tx.clone();
        let offset_tracker = self.offset_tracker.clone();

        task_set.spawn(async move {
            let mut handler =
                FileWorkHandler::new(config, logs_output, ack_tx, file_info_tx, offset_tracker)?;
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
    /// Sender for acks to the offset committer
    ack_tx: BoundedSender<FileAcknowledgement>,
    /// Sender for file info updates to the offset committer
    file_info_tx: BoundedSender<TrackedFileInfo>,
    /// Shared offset tracker
    offset_tracker: SharedOffsetTracker,
}

impl FileWorkHandler {
    fn new(
        config: FileReceiverConfig,
        logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
        ack_tx: BoundedSender<FileAcknowledgement>,
        file_info_tx: BoundedSender<TrackedFileInfo>,
        offset_tracker: SharedOffsetTracker,
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
            ack_tx,
            file_info_tx,
            offset_tracker,
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

        // Open persistence database for coordinator (read-only for loading state)
        // The FileOffsetCommitter handles all writes
        let db = match JsonFileDatabase::open(&self.config.offsets_path) {
            Ok(db) => db,
            Err(e) => {
                error!(
                    "Failed to open persistence database at {:?}: {}",
                    self.config.offsets_path, e
                );
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
        // - results_tx/rx: workers -> coordinator (offset updates for EOF tracking)
        // - workers_done_tx/rx: async handler -> coordinator (shutdown signal)
        //
        // Note: ack channel and offset_tracker are created in FileReceiver::new()
        // and shared with the FileOffsetCommitter
        let (work_tx, mut work_rx) = bounded_channel::bounded::<FileWorkItem>(max_concurrent_files);
        let (records_tx, mut records_rx) = bounded_channel::bounded::<LogRecordBatch>(10);
        let (results_tx, results_rx) = bounded_channel::bounded::<FileWorkResult>(10);
        let (workers_done_tx, workers_done_rx) = std::sync::mpsc::channel::<()>();

        // Use shared offset tracker from FileReceiver
        let offset_tracker = self.offset_tracker.clone();
        let ack_tx = self.ack_tx.clone();
        let file_info_tx = self.file_info_tx.clone();

        // Create shutdown flag (shared between coordinator, async handler, and workers)
        let shutting_down = Arc::new(AtomicBool::new(false));
        let shutting_down_async = shutting_down.clone();
        let shutting_down_workers = shutting_down.clone();

        // Create worker context (shared by all workers)
        let worker_ctx = WorkerContext {
            parser,
            include_file_name: self.config.include_file_name,
            include_file_path: self.config.include_file_path,
            records_tx,
            results_tx,
            max_batch_size: self.config.max_batch_size,
            shutting_down: shutting_down_workers,
        };

        // Create the coordinator
        let finder = GlobFileFinder::new(self.config.include.clone(), self.config.exclude.clone())?;
        let coordinator = FileCoordinator {
            config: self.config.clone(),
            finder,
            persister,
            tracked_files: HashMap::new(),
            watcher,
            watched_dirs: Vec::new(),
            first_check: true,
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
                biased;

                // Process completed workers
                Some(result) = worker_futures.next(), if !worker_futures.is_empty() => {
                    if let Err(e) = result {
                        error!("Worker task failed: {}", e);
                    }
                }

                // Receive work items and spawn workers (with concurrency limit)
                Some(work) = work_rx.next(), if worker_futures.len() < max_concurrent_files => {
                    let current_workers = worker_futures.len();
                    debug!(
                        file_id = %work.file_id,
                        path = ?work.path,
                        current_workers = current_workers,
                        max_workers = max_concurrent_files,
                        "Spawning worker for file"
                    );

                    // Send file info to offset committer for persistence metadata
                    let file_info = TrackedFileInfo {
                        file_id: work.file_id,
                        path: work.path.display().to_string(),
                        filename: work.path.file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("")
                            .to_string(),
                    };
                    // Best effort - don't block if channel is full
                    let _ = file_info_tx.try_send(file_info);

                    let ctx = worker_ctx.clone();
                    let handle = tokio::task::spawn_blocking(move || {
                        process_file_work(work, ctx);
                    });
                    worker_futures.push_back(handle);
                }

                // Receive log records from workers and send to exporters
                Some(batch) = records_rx.next() => {
                    if !batch.log_records.is_empty() {
                        // Track offsets for at-least-once delivery
                        {
                            let mut tracker = offset_tracker.lock().unwrap();
                            tracker.track_batch(batch.file_id, &batch.offsets);
                        }

                        if let Some(ref logs_output) = self.logs_output {
                            // Build ResourceLogs directly from LogRecords
                            let scope_logs = ScopeLogs {
                                scope: Some(InstrumentationScope {
                                    name: String::new(),
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

                            // Create metadata for at-least-once delivery
                            let file_metadata = FileMetadata::new(
                                batch.file_id,
                                batch.offsets.clone(),
                                Some(ack_tx.clone()),
                            );
                            let metadata = MessageMetadata::file(file_metadata);

                            let payload_msg = payload::Message::new(Some(metadata), vec![resource_logs], None);

                            match send_with_cancellation(logs_output, payload_msg, &cancel).await {
                                Ok(_) => {
                                    self.accepted_counter.add(batch.count, &self.tags);
                                }
                                Err(SendError::Cancelled) => {
                                    self.refused_counter.add(batch.count, &self.tags);
                                    debug!("Send cancelled during shutdown");
                                    shutting_down_async.store(true, Ordering::SeqCst);
                                    break;
                                }
                                Err(SendError::ChannelClosed) => {
                                    self.refused_counter.add(batch.count, &self.tags);
                                    error!("Output channel closed, stopping file receiver");
                                    shutting_down_async.store(true, Ordering::SeqCst);
                                    break;
                                }
                            }
                        }
                    }
                }

                // Note: Ack processing is handled by FileOffsetCommitter

                _ = cancel.cancelled() => {
                    // Set shutting_down before breaking so coordinator sees it
                    shutting_down_async.store(true, Ordering::SeqCst);
                    info!("File receiver cancelled, starting shutdown sequence");
                    break;
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

        // Note: Ack draining is handled by FileOffsetCommitter which runs until
        // after exporters finish, ensuring all acks are processed

        // Wait for coordinator thread to finish
        drop(records_rx);
        drop(work_rx);

        // Join coordinator with timeout (100ms should be plenty since it checks shutting_down flag)
        let coord_join_result = tokio::time::timeout(
            Duration::from_millis(100),
            tokio::task::spawn_blocking(move || coord_handle.join()),
        )
        .await;
        match coord_join_result {
            Ok(Ok(Ok(()))) => debug!("Coordinator thread joined successfully"),
            Ok(Ok(Err(_))) => {
                error!("Coordinator thread panicked");
                return Err("Coordinator thread panicked".into());
            }
            Ok(Err(e)) => {
                error!("Failed to join coordinator: {}", e);
                return Err(e.into());
            }
            Err(_) => {
                warn!("Timeout waiting for coordinator thread to join");
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
                                    name: String::new(),
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

                            // Use timeout on send to avoid blocking shutdown indefinitely
                            match timeout_at(records_deadline, logs_output.send(payload_msg)).await
                            {
                                Ok(Ok(_)) => {
                                    self.accepted_counter.add(batch.count, &self.tags);
                                }
                                Ok(Err(e)) => {
                                    self.refused_counter.add(batch.count, &self.tags);
                                    error!("Failed to send logs during drain: {}", e);
                                }
                                Err(_) => {
                                    self.refused_counter.add(batch.count, &self.tags);
                                    warn!("Timeout sending logs during drain");
                                    break;
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
    debug!(
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
    let mut line_offsets: Vec<LineOffset> = Vec::with_capacity(max_batch_size);
    let file_id = work.file_id;

    let records_tx = &ctx.records_tx;

    let parse_result = reader.read_lines_into(|line, begin_offset, len| {
        // Check if we're shutting down - exit early to allow worker to finish
        if ctx.shutting_down.load(Ordering::Relaxed) {
            debug!("Worker detected shutdown, stopping file read");
            return false;
        }

        // Get timestamp for each line individually
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Clone static attributes and extend with parsed attributes
        let mut attributes = static_attributes.clone();

        // Parse line if parser is configured
        let (parsed_attributes, severity_number, severity_text, parsed_timestamp) =
            if let Some(ref parser) = ctx.parser {
                match parser.parse(&line) {
                    Ok(parsed) => (
                        parsed.attributes,
                        parsed.severity_number.unwrap_or(0),
                        parsed.severity_text.unwrap_or_default(),
                        parsed.timestamp,
                    ),
                    Err(e) => {
                        debug!("Parse error: {}", e);
                        return true; // Skip unparseable entries but continue reading
                    }
                }
            } else {
                (Vec::new(), 0, String::new(), None)
            };

        // Use parsed timestamp if available, otherwise use current time
        let time_unix_nano = parsed_timestamp.unwrap_or(now);

        // Add parsed attributes
        attributes.extend(parsed_attributes);

        // Build LogRecord
        let log_record = LogRecord {
            time_unix_nano,
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
        line_offsets.push(LineOffset::new(begin_offset, len));

        // Send batch when we reach max size
        if log_records.len() >= max_batch_size {
            let batch = LogRecordBatch {
                count: log_records.len() as u64,
                log_records: std::mem::replace(
                    &mut log_records,
                    Vec::with_capacity(max_batch_size),
                ),
                file_id,
                offsets: std::mem::replace(&mut line_offsets, Vec::with_capacity(max_batch_size)),
            };
            if records_tx.send_blocking(batch).is_err() {
                // Channel closed - we're shutting down, remaining lines will be discarded
                debug!("Records channel closed during batch send, discarding remaining lines");
                return false; // Stop reading
            }
        }

        true // Continue reading
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
            file_id,
            offsets: line_offsets,
        };
        if ctx.records_tx.send_blocking(batch).is_err() {
            // Channel closed - we're shutting down, final batch discarded
            debug!("Records channel closed, discarding final batch");
        }
    }

    // Send offset update back to coordinator
    let reached_eof = reader.is_eof();
    debug!(
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
    // Error tracking for threshold-based exit
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
    ///
    /// Supports both v0 (legacy) and v1 (versioned) schema formats:
    /// - v0: `{files: [{dev, ino, offset}]}`
    /// - v1: `{version: 1, files: {key: {path, filename, dev, ino, offset}}}`
    fn load_state(&mut self) -> Result<()> {
        self.persister.load()?;

        // Try to load v1 format first (raw JSON in raw_json_cache)
        // Then fall back to v0 format (base64 in cache)
        let state_v1: PersistedStateV1 = match self
            .persister
            .try_get_raw_json::<PersistedStateV1>(KNOWN_FILES_KEY)
        {
            Ok(Some(state)) => {
                // V1 format found - use directly
                state
            }
            Ok(None) => {
                // No v1 format - check for v0 (legacy base64)
                let raw_json = match self.persister.try_get_raw(KNOWN_FILES_KEY) {
                    Ok(Some(raw)) => raw,
                    Ok(None) => {
                        debug!("No persisted state found, starting fresh");
                        return Ok(());
                    }
                    Err(e) => {
                        return Err(Error::Persistence(format!(
                            "failed to read persisted state: {}",
                            e
                        )));
                    }
                };

                // V0 format (legacy) - migrate to v1
                let state_v0: PersistedStateV0 = match serde_json::from_str(&raw_json) {
                    Ok(state) => state,
                    Err(e) => {
                        return Err(Error::Persistence(format!(
                            "persisted state is corrupted and cannot be loaded: {}. \
                                 To start fresh, delete the offsets file and restart.",
                            e
                        )));
                    }
                };

                info!(
                    "Migrating {} file entries from v0 to v1 schema",
                    state_v0.files.len()
                );

                // Convert v0 to v1 (path/filename will be empty until first sync)
                let mut files = HashMap::new();
                for entry in state_v0.files {
                    let key = file_id_to_key(entry.dev, entry.ino);
                    files.insert(
                        key,
                        PersistedFileEntryV1 {
                            path: String::new(),     // Will be populated on first save
                            filename: String::new(), // Will be populated on first save
                            dev: entry.dev,
                            ino: entry.ino,
                            offset: entry.offset,
                        },
                    );
                }

                PersistedStateV1 {
                    version: PERSISTED_STATE_VERSION,
                    files,
                }
            }
            Err(e) => {
                return Err(Error::Persistence(format!(
                    "persisted state is corrupted and cannot be loaded: {}. \
                         To start fresh, delete the offsets file and restart.",
                    e
                )));
            }
        };

        debug!(
            "Loaded {} known files from persister (v{})",
            state_v1.files.len(),
            state_v1.version
        );

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

            // Generate key for lookup
            let key = file_id_to_key(file_id.dev(), file_id.ino());

            // Try to find matching persisted state by key
            if let Some(entry) = state_v1.files.get(&key) {
                debug!("Restored file {:?} at offset {}", path, entry.offset);
                self.tracked_files.insert(
                    file_id,
                    TrackedFile {
                        file_id,
                        file,
                        path: path.clone(),
                        offset: entry.offset,
                        state: FileState::Active,
                        generation: 0,
                        in_flight: false,
                    },
                );
            }
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

    /// Process any pending results from workers (EOF handling and in_flight tracking)
    fn process_results(&mut self) {
        let rotate_wait = self.config.rotate_wait;

        // Non-blocking drain of all available results
        let mut results_processed = 0u32;
        while let Some(result) = self.results_rx.try_recv() {
            results_processed += 1;
            if let Some(tracked) = self.tracked_files.get_mut(&result.file_id) {
                debug!(
                    file_id = %result.file_id,
                    new_offset = result.new_offset,
                    reached_eof = result.reached_eof,
                    "Worker completed, clearing in_flight"
                );
                // Update tracked.offset to the position where reading stopped.
                // This is used for dispatching the next work item (where to resume reading).
                // Note: For persistence/crash recovery, we use get_persistable_offset() from
                // the offset tracker, which returns the lowest pending offset (or hwm.end_offset()
                // if all acked) to ensure at-least-once delivery semantics.
                tracked.offset = result.new_offset;
                tracked.in_flight = false; // Work completed, allow new dispatches

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
            debug!(
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

        debug!(
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
                        debug!(
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
        // Keep this short since agent only waits 1s for receiver exit
        let wait_timeout = Duration::from_millis(200);
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

        // Note: Final state persistence is handled by FileOffsetCommitter
        // which continues running after this coordinator stops

        info!("File coordinator stopped");
    }

    /// Drain results from in-flight workers during shutdown
    fn drain_results(&mut self) {
        // Keep this short since agent only waits 1s for receiver exit
        let drain_timeout = Duration::from_millis(100);
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
                        tracked.in_flight = false;
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

/// Helper to send logs with cancellation support.
async fn send_with_cancellation(
    output: &OTLPOutput<payload::Message<ResourceLogs>>,
    message: payload::Message<ResourceLogs>,
    cancel_token: &CancellationToken,
) -> std::result::Result<(), SendError> {
    let send_fut = output.send_async(message);
    tokio::pin!(send_fut);

    select! {
        result = send_fut => {
            match result {
                Ok(()) => Ok(()),
                Err(_) => Err(SendError::ChannelClosed),
            }
        }
        _ = cancel_token.cancelled() => {
            Err(SendError::Cancelled)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receivers::file::input::MockFileFinder;
    use crate::receivers::file::persistence::PersistedFileStateV0;
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

    #[test]
    fn test_load_state_migrates_v0_to_v1_in_memory() {
        let temp_dir = TempDir::new().unwrap();
        let offsets_path = temp_dir.path().join("offsets.json");

        // Create a test file so we can get a real FileId
        let test_file_path = temp_dir.path().join("test.log");
        std::fs::write(&test_file_path, "line1\nline2\nline3\n").unwrap();

        let test_file = File::open(&test_file_path).unwrap();
        let file_id = FileId::from_file(&test_file).unwrap();
        drop(test_file);

        // Write v0 format state using the persister (which handles base64 encoding)
        {
            let db = JsonFileDatabase::open(&offsets_path).unwrap();
            let mut persister = db.persister("test");

            // Create a v0 format state (legacy format without version field)
            let v0_state = PersistedStateV0 {
                files: vec![PersistedFileStateV0 {
                    dev: file_id.dev(),
                    ino: file_id.ino(),
                    offset: 12345,
                }],
            };
            persister.set_json(KNOWN_FILES_KEY, &v0_state).unwrap();
            persister.sync().unwrap();
        }

        // Create config and coordinator
        let mut config = test_config(&temp_dir);
        config.include = vec![test_file_path.display().to_string()];

        let db = JsonFileDatabase::open(&offsets_path).unwrap();
        let persister = db.persister("test");

        let finder = GlobFileFinder::new(config.include.clone(), config.exclude.clone()).unwrap();
        let watcher = AnyWatcher::Mock(MockWatcher::new());

        let (work_tx, _work_rx) = bounded_channel::bounded::<FileWorkItem>(10);
        let (_results_tx, results_rx) = bounded_channel::bounded::<FileWorkResult>(10);
        let (_workers_done_tx, workers_done_rx) = std::sync::mpsc::channel::<()>();

        let mut coordinator = FileCoordinator {
            config: config.clone(),
            finder,
            persister,
            tracked_files: HashMap::new(),
            watcher,
            watched_dirs: Vec::new(),
            first_check: true,
            poll_first_failure: None,
            watcher_first_error: None,
            work_tx: Some(work_tx),
            results_rx,
            workers_done_rx,
            shutting_down: Arc::new(AtomicBool::new(false)),
        };

        // Load state - should migrate v0 to v1 in memory
        let result = coordinator.load_state();
        assert!(result.is_ok(), "load_state should succeed for v0 format");

        // Verify the file was restored with correct offset
        assert_eq!(
            coordinator.tracked_files.len(),
            1,
            "Should have 1 tracked file"
        );
        let tracked = coordinator.tracked_files.get(&file_id).unwrap();
        assert_eq!(
            tracked.offset, 12345,
            "Offset should be restored from v0 state"
        );
    }

    #[test]
    fn test_load_state_reads_v1_format() {
        let temp_dir = TempDir::new().unwrap();
        let offsets_path = temp_dir.path().join("offsets.json");

        // Create a test file so we can get a real FileId
        let test_file_path = temp_dir.path().join("test.log");
        std::fs::write(&test_file_path, "line1\nline2\nline3\n").unwrap();

        let test_file = File::open(&test_file_path).unwrap();
        let file_id = FileId::from_file(&test_file).unwrap();
        drop(test_file);

        // Write v1 format state directly
        {
            let db = JsonFileDatabase::open(&offsets_path).unwrap();
            let mut persister = db.persister("test");

            let key = file_id_to_key(file_id.dev(), file_id.ino());
            let mut files = HashMap::new();
            files.insert(
                key,
                PersistedFileEntryV1 {
                    path: test_file_path.display().to_string(),
                    filename: "test.log".to_string(),
                    dev: file_id.dev(),
                    ino: file_id.ino(),
                    offset: 54321,
                },
            );

            let v1_state = PersistedStateV1 {
                version: PERSISTED_STATE_VERSION,
                files,
            };
            persister.set_raw_json(KNOWN_FILES_KEY, &v1_state).unwrap();
            persister.sync().unwrap();
        }

        // Create config and coordinator
        let mut config = test_config(&temp_dir);
        config.include = vec![test_file_path.display().to_string()];

        let db = JsonFileDatabase::open(&offsets_path).unwrap();
        let persister = db.persister("test");

        let finder = GlobFileFinder::new(config.include.clone(), config.exclude.clone()).unwrap();
        let watcher = AnyWatcher::Mock(MockWatcher::new());

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
            poll_first_failure: None,
            watcher_first_error: None,
            work_tx: Some(work_tx),
            results_rx,
            workers_done_rx,
            shutting_down: Arc::new(AtomicBool::new(false)),
        };

        // Load state from v1 format
        let result = coordinator.load_state();
        assert!(result.is_ok(), "load_state should succeed for v1 format");

        // Verify the file was restored correctly
        assert_eq!(
            coordinator.tracked_files.len(),
            1,
            "Should have 1 tracked file"
        );
        let tracked = coordinator.tracked_files.get(&file_id).unwrap();
        assert_eq!(
            tracked.offset, 54321,
            "Offset should be restored from v1 state"
        );
    }

    #[test]
    fn test_parsed_timestamp_flows_to_log_record() {
        use crate::receivers::file::parser::Parser;
        use crate::receivers::file::parser::nginx::access_parser;

        let parser = access_parser().unwrap();
        let log_line = r#"192.168.1.1 - - [17/Dec/2025:10:15:32 +0000] "GET /api/users HTTP/1.1" 200 1234 "-" "curl/7.68.0""#;
        let parsed = parser.parse(log_line).unwrap();

        assert!(parsed.timestamp.is_some());

        let parsed_timestamp = parsed.timestamp.unwrap();
        let expected_timestamp = 1765966532_000_000_000u64;
        assert_eq!(parsed_timestamp, expected_timestamp);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let time_unix_nano = parsed.timestamp.unwrap_or(now);

        let log_record = LogRecord {
            time_unix_nano,
            observed_time_unix_nano: now,
            severity_number: parsed.severity_number.unwrap_or(0),
            severity_text: parsed.severity_text.unwrap_or_default(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(log_line.to_string())),
            }),
            attributes: parsed.attributes,
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![],
            span_id: vec![],
            event_name: String::new(),
        };

        assert_eq!(log_record.time_unix_nano, expected_timestamp);
        assert_ne!(
            log_record.time_unix_nano,
            log_record.observed_time_unix_nano
        );
    }

    #[test]
    fn test_log_record_uses_current_time_when_no_parsed_timestamp() {
        use crate::receivers::file::parser::{Parser, RegexParser};

        let parser = RegexParser::new(r"^(?P<message>.+)$").unwrap();
        let log_line = "Just a simple log message";
        let parsed = parser.parse(log_line).unwrap();

        assert!(parsed.timestamp.is_none());

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let time_unix_nano = parsed.timestamp.unwrap_or(now);

        let log_record = LogRecord {
            time_unix_nano,
            observed_time_unix_nano: now,
            severity_number: 0,
            severity_text: String::new(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(log_line.to_string())),
            }),
            attributes: parsed.attributes,
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![],
            span_id: vec![],
            event_name: String::new(),
        };

        assert_eq!(
            log_record.time_unix_nano,
            log_record.observed_time_unix_nano
        );
    }

    #[test]
    fn test_process_results_updates_offset() {
        let temp_dir = TempDir::new().unwrap();
        let config = test_config(&temp_dir);
        let finder = MockFileFinder::new();
        let watcher = AnyWatcher::Mock(MockWatcher::new());

        let db = JsonFileDatabase::open(temp_dir.path().join("offsets.json")).unwrap();
        let persister = db.persister("test");

        let (work_tx, _work_rx) = bounded_channel::bounded::<FileWorkItem>(10);
        let (results_tx, results_rx) = bounded_channel::bounded::<FileWorkResult>(10);
        let (_workers_done_tx, workers_done_rx) = std::sync::mpsc::channel::<()>();

        let mut coordinator = FileCoordinator {
            config,
            finder,
            persister,
            tracked_files: HashMap::new(),
            watcher,
            watched_dirs: Vec::new(),
            first_check: true,
            poll_first_failure: None,
            watcher_first_error: None,
            work_tx: Some(work_tx),
            results_rx,
            workers_done_rx,
            shutting_down: Arc::new(AtomicBool::new(false)),
        };

        // Add a tracked file with in_flight = true - need a real file for the File handle
        let test_file_path = temp_dir.path().join("test.log");
        std::fs::write(&test_file_path, "test content").unwrap();
        let file = File::open(&test_file_path).unwrap();
        let file_id = FileId::new(1, 100);
        coordinator.tracked_files.insert(
            file_id,
            TrackedFile {
                file_id,
                file,
                path: test_file_path,
                offset: 0,
                state: FileState::Active,
                generation: 1,
                in_flight: true, // Work is in flight
            },
        );

        // Send a FileWorkResult (simulating worker completion)
        let result = FileWorkResult {
            file_id,
            new_offset: 5000, // Worker read up to 5000
            reached_eof: false,
        };
        results_tx.send_blocking(result).unwrap();

        // Process results
        coordinator.process_results();

        // Verify in_flight is cleared and offset IS updated to new_offset
        // (tracked.offset is used for dispatching next work item, not for persistence)
        let tracked = coordinator.tracked_files.get(&file_id).unwrap();
        assert!(!tracked.in_flight, "in_flight should be cleared");
        assert_eq!(
            tracked.offset, 5000,
            "Offset should be updated to new_offset so next dispatch starts from correct position"
        );
    }
}

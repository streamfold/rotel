// SPDX-License-Identifier: Apache-2.0

//! File receiver implementation.
//!
//! Uses native file system notifications (inotify/kqueue/FSEvents) when available,
//! with automatic fallback to polling for unsupported file systems like NFS.
//!
//! Architecture:
//! - A dedicated OS thread handles all blocking file I/O (reading, parsing, checkpointing)
//! - Parsed entries are sent via channel to an async task
//! - The async task only handles sending to exporters (non-blocking)
//! - This prevents file I/O from blocking the tokio runtime

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tower::BoxError;
use tracing::{debug, error, info, warn};

use crate::receivers::file::config::{FileReceiverConfig, ParserType};
use crate::receivers::file::convert::convert_entries_to_resource_logs;
use crate::receivers::file::entry::Entry;
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

/// Message sent from the I/O thread to the async task
struct EntriesBatch {
    entries: Vec<Entry>,
    count: u64,
}

/// Worker that runs on a dedicated OS thread for all blocking file I/O.
/// This prevents file operations from blocking the tokio runtime.
struct FileIOWorker {
    config: FileReceiverConfig,
    finder: FileFinder,
    parser: Option<Arc<dyn Parser + Send + Sync>>,
    persister: JsonFilePersister,
    readers: HashMap<Vec<u8>, FileReader>,
    watcher: Box<dyn FileWatcher + Send>,
    watched_dirs: Vec<PathBuf>,
    first_check: bool,
    // Checkpoint batching state
    checkpoint_config: CheckpointConfig,
    records_since_checkpoint: u64,
    last_checkpoint: Instant,
}

impl FileIOWorker {
    /// Load previously saved file states from the persister
    fn load_state(&mut self) -> Result<()> {
        self.persister.load()?;

        if let Some(state) = self.persister.get_json::<PersistedState>(KNOWN_FILES_KEY) {
            debug!("Loaded {} known files from persister", state.files.len());
            for file_state in state.files {
                // Store fingerprint bytes as key for later matching
                self.readers.insert(file_state.fingerprint_bytes.clone(), {
                    let fp = Fingerprint::from_bytes(file_state.fingerprint_bytes);
                    FileReader::new(
                        PathBuf::from("/dev/null"), // placeholder, will be replaced when file found
                        fp,
                        file_state.offset,
                        self.config.fingerprint_size,
                        self.config.max_log_size,
                    )
                    .unwrap_or_else(|_| {
                        FileReader::new(
                            PathBuf::from("/dev/null"),
                            Fingerprint::from_bytes(vec![]),
                            0,
                            self.config.fingerprint_size,
                            self.config.max_log_size,
                        )
                        .unwrap()
                    })
                });
            }
        }

        Ok(())
    }

    /// Save current file states to the persister (unconditional, always writes to disk)
    fn save_state(&mut self) -> Result<()> {
        let state = PersistedState {
            files: self
                .readers
                .values()
                .filter(|r| !r.fingerprint().is_empty())
                .map(|r| PersistedFileState {
                    fingerprint_bytes: r.fingerprint().bytes().to_vec(),
                    offset: r.offset(),
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

    /// Find a reader that matches the given fingerprint
    fn find_matching_reader(&self, fp: &Fingerprint) -> Option<u64> {
        for (key, reader) in &self.readers {
            let existing_fp = Fingerprint::from_bytes(key.clone());
            if fp.starts_with(&existing_fp) {
                return Some(reader.offset());
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
            // Extract the directory part before any wildcards
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

    /// Poll for file changes and read new content
    fn poll(&mut self) -> Result<Vec<Entry>> {
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

        // Increment generation on all known readers
        for reader in self.readers.values_mut() {
            reader.increment_generation();
        }

        // Process each file and collect entries
        let mut all_entries = Vec::new();
        let mut new_readers = HashMap::new();

        for path in paths {
            match self.process_file(&path) {
                Ok(Some((key, reader, entries))) => {
                    all_entries.extend(entries);
                    new_readers.insert(key, reader);
                }
                Ok(None) => {}
                Err(e) => {
                    error!("Error processing file {:?}: {}", path, e);
                }
            }
        }

        // Replace old readers with new ones
        // Keep readers that haven't been seen for at most 3 generations
        self.readers.retain(|_, reader| reader.generation() <= 3);
        self.readers.extend(new_readers);

        // Track records for checkpoint batching
        if !all_entries.is_empty() {
            self.records_since_checkpoint += all_entries.len() as u64;
        }

        // Conditionally checkpoint based on records processed and time elapsed
        if let Err(e) = self.maybe_checkpoint() {
            error!("Failed to save state: {}", e);
        }

        Ok(all_entries)
    }

    /// Process a single file
    fn process_file(
        &mut self,
        path: &PathBuf,
    ) -> Result<Option<(Vec<u8>, FileReader, Vec<Entry>)>> {
        // Open the file
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                debug!("Failed to open file {:?}: {}", path, e);
                return Ok(None);
            }
        };

        // Get fingerprint
        let fp = match Fingerprint::new(&mut file, self.config.fingerprint_size) {
            Ok(fp) => fp,
            Err(e) => {
                debug!("Failed to create fingerprint for {:?}: {}", path, e);
                return Ok(None);
            }
        };

        // Skip empty files
        if fp.is_empty() {
            return Ok(None);
        }

        // Check if we've seen this file before
        let offset = self.find_matching_reader(&fp).unwrap_or_else(|| {
            // New file - determine starting offset
            if self.config.start_at == StartAt::End {
                file.metadata().map(|m| m.len()).unwrap_or(0)
            } else {
                0
            }
        });

        // Create or update reader
        let mut reader = FileReader::new(
            path,
            fp.clone(),
            offset,
            self.config.fingerprint_size,
            self.config.max_log_size,
        )?;

        // Read new lines
        let lines = reader.read_lines()?;

        // Convert lines to entries
        let mut entries = Vec::new();
        for line in lines {
            let mut entry = Entry::with_body(line);

            // Add file attributes
            if self.config.include_file_name {
                if let Some(name) = reader.file_name() {
                    entry.add_attribute_string("log.file.name", name);
                }
            }

            if self.config.include_file_path {
                entry.add_attribute_string("log.file.path", path.display().to_string());
            }

            // Parse if parser is configured
            let entry = if let Some(ref parser) = self.parser {
                match parser.parse(entry) {
                    Ok(parsed) => parsed,
                    Err(e) => {
                        debug!("Parse error: {}", e);
                        continue; // Skip unparseable entries
                    }
                }
            } else {
                entry
            };

            entries.push(entry);
        }

        let key = fp.bytes().to_vec();
        Ok(Some((key, reader, entries)))
    }

    /// Run the I/O worker main loop
    fn run(mut self, entries_tx: mpsc::Sender<EntriesBatch>, cancel: CancellationToken) {
        // Load persisted state
        if let Err(e) = self.load_state() {
            warn!("Failed to load persisted state: {}", e);
        }

        // Initial setup and file scan
        if let Err(e) = self.setup_watches() {
            warn!("Failed to setup watches: {}", e);
        }

        // Do initial poll to read existing content
        match self.poll() {
            Ok(entries) if !entries.is_empty() => {
                let batch = EntriesBatch {
                    count: entries.len() as u64,
                    entries,
                };
                if entries_tx.blocking_send(batch).is_err() {
                    return;
                }
            }
            Ok(_) => {}
            Err(e) => {
                error!("Initial poll error: {}", e);
            }
        }

        let poll_interval = self.config.poll_interval;

        // Main loop: wait for watcher events, poll files, send entries
        loop {
            if cancel.is_cancelled() {
                break;
            }

            // Wait for watcher events (or timeout)
            match self.watcher.recv_timeout(poll_interval) {
                Ok(events) => {
                    // Process any specific file events for logging
                    for event in &events {
                        match event.kind {
                            FileEventKind::Create | FileEventKind::Modify => {
                                // Will be picked up by poll()
                            }
                            FileEventKind::Remove => {
                                for path in &event.paths {
                                    debug!("File removed: {:?}", path);
                                }
                            }
                            FileEventKind::Rename | FileEventKind::Other => {
                                // Trigger a full rescan handled by poll()
                            }
                        }
                    }

                    // Poll for changes
                    match self.poll() {
                        Ok(entries) if !entries.is_empty() => {
                            let batch = EntriesBatch {
                                count: entries.len() as u64,
                                entries,
                            };
                            if entries_tx.blocking_send(batch).is_err() {
                                break;
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            error!("Poll error: {}", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Watcher error: {}", e);
                    // On error, sleep briefly before retrying
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }

        // Final state save
        if let Err(e) = self.save_state() {
            error!("Failed to save final state: {}", e);
        }

        info!("File I/O worker stopped");
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
    /// Spawns a dedicated OS thread for all blocking file I/O and receives
    /// parsed entries via channel for sending to exporters.
    async fn run(&mut self, cancel: CancellationToken) {
        // Build parser (needs to be Arc for Send across thread)
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

        // Create the I/O worker
        let finder = FileFinder::new(self.config.include.clone(), self.config.exclude.clone());
        let io_worker = FileIOWorker {
            config: self.config.clone(),
            finder,
            parser,
            persister,
            readers: HashMap::new(),
            watcher,
            watched_dirs: Vec::new(),
            first_check: true,
            checkpoint_config: CheckpointConfig::default(),
            records_since_checkpoint: 0,
            last_checkpoint: Instant::now(),
        };

        // Create channel for entries from I/O thread to async task
        // Keep this small (10) to limit memory - each batch has up to 1000 entries
        let (entries_tx, mut entries_rx) = mpsc::channel::<EntriesBatch>(10);

        // Spawn the I/O worker on a dedicated OS thread
        let io_cancel = cancel.clone();
        let io_handle = std::thread::spawn(move || {
            io_worker.run(entries_tx, io_cancel);
        });

        // Main event loop - receive entries and send to exporters
        loop {
            select! {
                _ = cancel.cancelled() => {
                    debug!("File receiver cancelled, stopping");
                    break;
                }
                Some(batch) = entries_rx.recv() => {
                    if !batch.entries.is_empty() {
                        if let Some(ref logs_output) = self.logs_output {
                            let resource_logs = convert_entries_to_resource_logs(batch.entries);
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

        // Wait for I/O thread to finish
        drop(entries_rx);
        let _ = io_handle.join();

        info!("File receiver stopped");
    }
}

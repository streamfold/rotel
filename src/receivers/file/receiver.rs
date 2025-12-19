// SPDX-License-Identifier: Apache-2.0

//! File receiver implementation.
//!
//! Uses native file system notifications (inotify/kqueue/FSEvents) when available,
//! with automatic fallback to polling for unsupported file systems like NFS.

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use opentelemetry::metrics::Counter;
use opentelemetry::KeyValue;
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
use crate::receivers::file::parser::{nginx, JsonParser, Parser, RegexParser};
use crate::receivers::file::persistence::{JsonFileDatabase, JsonFilePersister, Persister, PersisterExt};
use crate::receivers::file::watcher::{
    create_watcher, FileEvent, FileEventKind, FileWatcher, WatcherConfig,
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
                let pattern = config.regex_pattern.as_ref()
                    .ok_or_else(|| Error::Config("Regex pattern required".to_string()))?;
                let parser = RegexParser::new(pattern)
                    .map_err(|e| Error::Regex(e.to_string()))?;
                Ok(Some(Box::new(parser)))
            }
            ParserType::NginxAccess => {
                let parser = nginx::access_parser()
                    .map_err(|e| Error::Regex(e.to_string()))?;
                Ok(Some(Box::new(parser)))
            }
            ParserType::NginxError => {
                let parser = nginx::error_parser()
                    .map_err(|e| Error::Regex(e.to_string()))?;
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

/// Internal handler for file watching
struct FileHandler {
    config: FileReceiverConfig,
    logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    finder: FileFinder,
    parser: Option<Box<dyn Parser + Send + Sync>>,
    persister: JsonFilePersister,
    readers: HashMap<Vec<u8>, FileReader>,
    watcher: Box<dyn FileWatcher + Send + Sync>,
    watched_dirs: Vec<PathBuf>,
    first_check: bool,
    accepted_counter: Counter<u64>,
    refused_counter: Counter<u64>,
    tags: [KeyValue; 1],
}

impl FileHandler {
    fn new(
        config: FileReceiverConfig,
        logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    ) -> std::result::Result<Self, BoxError> {
        let finder = FileFinder::new(config.include.clone(), config.exclude.clone());
        let parser = FileReceiver::build_parser(&config)?;

        // Open or create the persistence database
        let db = JsonFileDatabase::open(&config.offsets_path)?;
        let persister = db.persister("file_receiver");

        // Create watcher config
        let watcher_config = WatcherConfig {
            mode: config.watch_mode,
            poll_interval: config.poll_interval,
            debounce_interval: Duration::from_millis(50),
        };

        // Create the watcher (auto mode will try native first, fall back to poll)
        let watcher = create_watcher(&watcher_config, &[])
            .map_err(|e| -> BoxError { e.to_string().into() })?;

        info!(
            backend = watcher.backend_name(),
            native = watcher.is_native(),
            "File watcher initialized"
        );

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
            finder,
            parser,
            persister,
            readers: HashMap::new(),
            watcher,
            watched_dirs: Vec::new(),
            first_check: true,
            accepted_counter,
            refused_counter,
            tags: [KeyValue::new("receiver", "file")],
        })
    }

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

    /// Save current file states to the persister
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

    /// Run the main event loop
    async fn run(&mut self, cancel: CancellationToken) {
        // Load persisted state
        if let Err(e) = self.load_state() {
            warn!("Failed to load persisted state: {}", e);
        }

        // Initial setup and file scan
        if let Err(e) = self.setup_watches() {
            warn!("Failed to setup watches: {}", e);
        }

        // Do initial poll to read existing content
        if let Err(e) = self.poll().await {
            error!("Initial poll error: {}", e);
        }

        // Create a channel for watcher events (to bridge sync watcher with async runtime)
        let (event_tx, mut event_rx) = mpsc::channel::<Vec<FileEvent>>(100);

        // Spawn a blocking task to receive watcher events
        let watcher_poll_interval = self.config.poll_interval;
        let mut watcher = std::mem::replace(
            &mut self.watcher,
            Box::new(crate::receivers::file::watcher::PollWatcher::new(&[], Duration::from_secs(1)).unwrap()),
        );

        let watcher_cancel = cancel.clone();
        let watcher_handle = std::thread::spawn(move || {
            loop {
                if watcher_cancel.is_cancelled() {
                    break;
                }

                match watcher.recv_timeout(watcher_poll_interval) {
                    Ok(events) if !events.is_empty() => {
                        if event_tx.blocking_send(events).is_err() {
                            break;
                        }
                    }
                    Ok(_) => {
                        // Timeout with no events - send empty to trigger periodic poll
                        if event_tx.blocking_send(vec![]).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("Watcher error: {}", e);
                        // On error, sleep briefly before retrying
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        });

        // Main event loop
        loop {
            select! {
                _ = cancel.cancelled() => {
                    debug!("File receiver cancelled, stopping");
                    break;
                }
                Some(events) = event_rx.recv() => {
                    // Process any specific file events
                    let mut paths_changed: Vec<PathBuf> = Vec::new();
                    for event in events {
                        match event.kind {
                            FileEventKind::Create | FileEventKind::Modify => {
                                paths_changed.extend(event.paths);
                            }
                            FileEventKind::Remove => {
                                // Mark files as potentially rotated
                                for path in &event.paths {
                                    debug!("File removed: {:?}", path);
                                }
                            }
                            FileEventKind::Rename | FileEventKind::Other => {
                                // Trigger a full rescan
                                paths_changed.clear();
                                break;
                            }
                        }
                    }

                    // Poll for changes
                    if let Err(e) = self.poll().await {
                        error!("Poll error: {}", e);
                    }
                }
            }
        }

        // Wait for watcher thread to finish
        drop(event_rx);
        let _ = watcher_handle.join();

        // Final state save
        if let Err(e) = self.save_state() {
            error!("Failed to save final state: {}", e);
        }

        info!("File receiver stopped");
    }

    /// Poll for file changes and read new content
    async fn poll(&mut self) -> Result<()> {
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
            match self.process_file(&path).await {
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

        // Send entries if we have any
        if !all_entries.is_empty() {
            let count = all_entries.len() as u64;

            if let Some(ref logs_output) = self.logs_output {
                let resource_logs = convert_entries_to_resource_logs(all_entries);
                let payload_msg = payload::Message::new(None, vec![resource_logs]);

                match logs_output.send(payload_msg).await {
                    Ok(_) => {
                        self.accepted_counter.add(count, &self.tags);
                    }
                    Err(e) => {
                        self.refused_counter.add(count, &self.tags);
                        error!("Failed to send logs: {}", e);
                    }
                }
            }
        }

        // Save state
        if let Err(e) = self.save_state() {
            error!("Failed to save state: {}", e);
        }

        Ok(())
    }

    /// Process a single file
    async fn process_file(&mut self, path: &PathBuf) -> Result<Option<(Vec<u8>, FileReader, Vec<Entry>)>> {
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
}

// SPDX-License-Identifier: Apache-2.0

//! Offset committer for file receiver at-least-once delivery.
//!
//! This module handles acknowledgement processing and state persistence separately
//! from the main file receiver loop. This allows the committer to continue receiving
//! acks after the receiver stops reading, ensuring all in-flight messages are
//! acknowledged before shutdown.

use std::time::{Duration, Instant};

use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::bounded_channel::BoundedReceiver;
use crate::receivers::file::error::Result;
use crate::receivers::file::persistence::{JsonFilePersister, Persister};
use crate::receivers::file::receiver::SharedOffsetTracker;
use crate::topology::payload::FileAcknowledgement;

use super::persistence::{
    KNOWN_FILES_KEY, PERSISTED_STATE_VERSION, PersistedFileEntryV1, PersistedStateV1,
};

/// Configuration for the offset committer
pub struct OffsetCommitterConfig {
    /// Interval between periodic checkpoints
    pub checkpoint_interval: Duration,
    /// Maximum time to wait when draining acks during shutdown
    pub drain_timeout: Duration,
    /// Maximum duration of consecutive checkpoint failures before returning error
    pub max_checkpoint_failure_duration: Duration,
}

impl Default for OffsetCommitterConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: Duration::from_secs(1),
            drain_timeout: Duration::from_secs(2),
            max_checkpoint_failure_duration: Duration::from_secs(60),
        }
    }
}

/// File metadata needed for persistence
#[derive(Debug, Clone)]
pub struct TrackedFileInfo {
    pub file_id: crate::receivers::file::input::FileId,
    pub path: String,
    pub filename: String,
}

/// Handles acknowledgement processing and state persistence for the file receiver.
///
/// Runs as a separate task that survives after the main receiver loop stops,
/// allowing it to receive acks from exporters before final shutdown.
pub struct FileOffsetCommitter {
    /// Channel for receiving acknowledgements from the pipeline
    ack_receiver: BoundedReceiver<FileAcknowledgement>,
    /// Shared offset tracker (also used by receiver for tracking sends)
    offset_tracker: SharedOffsetTracker,
    /// Persister for saving state to disk
    persister: JsonFilePersister,
    /// Configuration
    config: OffsetCommitterConfig,
    /// Interval timer for periodic checkpoints
    checkpoint_interval: tokio::time::Interval,
    /// Channel for receiving file info updates from the receiver
    file_info_rx: BoundedReceiver<TrackedFileInfo>,
    /// Current tracked files (path/filename info for persistence)
    tracked_files:
        std::collections::HashMap<crate::receivers::file::input::FileId, TrackedFileInfo>,
    /// Tracks when checkpoint failures started (for threshold-based exit)
    checkpoint_first_failure: Option<Instant>,
}

impl FileOffsetCommitter {
    /// Create a new FileOffsetCommitter
    pub fn new(
        ack_receiver: BoundedReceiver<FileAcknowledgement>,
        offset_tracker: SharedOffsetTracker,
        persister: JsonFilePersister,
        config: OffsetCommitterConfig,
        file_info_rx: BoundedReceiver<TrackedFileInfo>,
    ) -> Self {
        let checkpoint_interval = tokio::time::interval(config.checkpoint_interval);

        Self {
            ack_receiver,
            offset_tracker,
            persister,
            config,
            checkpoint_interval,
            file_info_rx,
            tracked_files: std::collections::HashMap::new(),
            checkpoint_first_failure: None,
        }
    }

    /// Run the main committer loop
    ///
    /// Processes acknowledgements and performs periodic checkpoints until cancelled.
    /// On cancellation, drains remaining acks and performs final checkpoint.
    /// Returns error if checkpoint failures persist beyond the configured threshold.
    pub async fn run(&mut self, cancel_token: CancellationToken) -> Result<()> {
        info!("File offset committer started");

        let mut fatal_error = None;

        loop {
            select! {
                biased;

                // Periodic checkpoint
                _ = self.checkpoint_interval.tick() => {
                    if let Err(e) = self.maybe_checkpoint() {
                        error!("Checkpoint failures persisted beyond threshold, exiting: {}", e);
                        fatal_error = Some(e);
                        break;
                    }
                }

                // Receive file info updates from receiver
                Some(file_info) = self.file_info_rx.next() => {
                    self.tracked_files.insert(file_info.file_id, file_info);
                }

                // Process acknowledgements
                ack_result = self.ack_receiver.next() => {
                    match ack_result {
                        Some(ack) => {
                            self.process_ack(ack);
                        }
                        None => {
                            // Ack channel closed - receiver has shut down
                            debug!("Ack channel closed, exiting offset committer run loop");
                            break;
                        }
                    }
                }

                // Handle cancellation
                _ = cancel_token.cancelled() => {
                    debug!("FileOffsetCommitter cancelled, draining pending acknowledgements");
                    break;
                }
            }
        }

        // Drain remaining acks and perform final checkpoint
        self.drain().await;

        match fatal_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Process a single acknowledgement
    fn process_ack(&self, ack: FileAcknowledgement) {
        let mut tracker = self.offset_tracker.lock().unwrap();
        match ack {
            FileAcknowledgement::Ack(file_ack) => {
                debug!(
                    file_id = %file_ack.file_id,
                    offsets = ?file_ack.offsets.len(),
                    "Processing file ack"
                );
                tracker.acknowledge_batch(file_ack.file_id, &file_ack.offsets);
            }
            FileAcknowledgement::Nack(file_nack) => {
                debug!(
                    file_id = %file_nack.file_id,
                    offsets = ?file_nack.offsets.len(),
                    "Processing file nack"
                );
                // nack() will panic if finite_retry_enabled is false
                tracker.nack_batch(file_nack.file_id, &file_nack.offsets);
            }
        }
    }

    /// Drain remaining acknowledgements with timeout and perform final checkpoint
    async fn drain(&mut self) {
        let drain_start = tokio::time::Instant::now();
        let drain_deadline = drain_start + self.config.drain_timeout;
        let mut ack_count = 0;

        debug!("Draining remaining file acknowledgements");

        // Drain loop with deadline
        loop {
            if tokio::time::Instant::now() >= drain_deadline {
                debug!(
                    "Drain deadline reached after processing {} acknowledgements",
                    ack_count
                );
                break;
            }

            // Also drain any pending file info updates
            while let Ok(Some(file_info)) =
                tokio::time::timeout(Duration::from_millis(1), self.file_info_rx.next()).await
            {
                self.tracked_files.insert(file_info.file_id, file_info);
            }

            let poll_res =
                tokio::time::timeout(Duration::from_millis(100), self.ack_receiver.next()).await;

            match poll_res {
                Ok(Some(ack)) => {
                    self.process_ack(ack);
                    ack_count += 1;
                }
                Ok(None) => {
                    debug!("Ack channel closed during drain after {} acks", ack_count);
                    break;
                }
                Err(_) => {
                    // Timeout - check if we should continue
                    // If no acks received in 100ms and we have some, we're probably done
                    if ack_count > 0 {
                        debug!(
                            "No acks received in 100ms, finishing drain after {} acks",
                            ack_count
                        );
                        break;
                    }
                }
            }
        }

        if ack_count > 0 {
            info!("Drained {} pending acknowledgements", ack_count);
        }

        // Perform final checkpoint
        debug!("Performing final checkpoint after draining");
        if let Err(e) = self.checkpoint() {
            warn!("Failed to perform final checkpoint: {}", e);
        } else {
            info!("Final checkpoint completed successfully");
        }
    }

    /// Conditionally checkpoint and track failures.
    /// Returns Ok on success or if failure is within threshold.
    /// Returns Err only when failure duration threshold is breached.
    fn maybe_checkpoint(&mut self) -> Result<()> {
        match self.checkpoint() {
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
                    // Threshold breached - return error to trigger shutdown
                    Err(e)
                } else {
                    // Within threshold - log warning and continue
                    warn!(
                        "Checkpoint failed (failures started {:?} ago): {}",
                        failure_duration, e
                    );
                    Ok(())
                }
            }
        }
    }

    /// Perform a checkpoint - persist current state to disk
    fn checkpoint(&mut self) -> Result<()> {
        // Get persistable offsets from the offset tracker
        let persistable_offsets = {
            let tracker = self.offset_tracker.lock().unwrap();
            tracker.get_all_persistable_offsets()
        };

        // Build state to persist
        let mut files = std::collections::HashMap::new();

        for (file_id, offset) in persistable_offsets {
            // Get file info if we have it
            let (path, filename) = self
                .tracked_files
                .get(&file_id)
                .map(|info| (info.path.clone(), info.filename.clone()))
                .unwrap_or_else(|| (String::new(), String::new()));

            let entry = PersistedFileEntryV1 {
                path,
                filename,
                dev: file_id.dev(),
                ino: file_id.ino(),
                offset,
            };
            let key = entry.key();
            files.insert(key, entry);
        }

        let state = PersistedStateV1 {
            version: PERSISTED_STATE_VERSION,
            files,
        };

        self.persister.set_raw_json(KNOWN_FILES_KEY, &state)?;
        self.persister.sync()?;

        debug!("Checkpoint completed with {} files", state.files.len());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_channel;
    use crate::receivers::file::input::FileId;
    use crate::receivers::file::offset_tracker::FileOffsetTracker;
    use crate::receivers::file::persistence::{
        JsonFileDatabase, KNOWN_FILES_KEY, PersistedStateV1, file_id_to_key,
    };
    use std::sync::{Arc, Mutex as StdMutex};
    use tempfile::TempDir;

    /// Create a test committer with the given config
    fn create_test_committer(
        temp_dir: &TempDir,
        config: OffsetCommitterConfig,
    ) -> (FileOffsetCommitter, SharedOffsetTracker) {
        let db = JsonFileDatabase::open(temp_dir.path().join("offsets.json")).unwrap();
        let persister = db.persister("test");

        let (_ack_tx, ack_rx) = bounded_channel::bounded(10);
        let (_file_info_tx, file_info_rx) = bounded_channel::bounded(10);
        let offset_tracker: SharedOffsetTracker =
            Arc::new(StdMutex::new(FileOffsetTracker::new(false)));

        let committer = FileOffsetCommitter::new(
            ack_rx,
            offset_tracker.clone(),
            persister,
            config,
            file_info_rx,
        );

        (committer, offset_tracker)
    }

    #[tokio::test]
    async fn test_checkpoint_writes_v1_format() {
        let temp_dir = TempDir::new().unwrap();
        let offsets_path = temp_dir.path().join("offsets.json");

        let (mut committer, offset_tracker) =
            create_test_committer(&temp_dir, OffsetCommitterConfig::default());

        // Add a tracked file with file info
        let file_id = FileId::new(1, 100);
        committer.tracked_files.insert(
            file_id,
            TrackedFileInfo {
                file_id,
                path: "/var/log/test.log".to_string(),
                filename: "test.log".to_string(),
            },
        );

        // Register the file in the offset tracker with an offset
        {
            let mut tracker = offset_tracker.lock().unwrap();
            // Track some offsets as sent
            tracker.track(
                file_id,
                crate::receivers::file::offset_tracker::LineOffset::new(0, 100),
            );
            tracker.track(
                file_id,
                crate::receivers::file::offset_tracker::LineOffset::new(100, 50),
            );
            // Acknowledge them so they're persistable
            tracker.acknowledge(
                file_id,
                crate::receivers::file::offset_tracker::LineOffset::new(0, 100),
            );
            tracker.acknowledge(
                file_id,
                crate::receivers::file::offset_tracker::LineOffset::new(100, 50),
            );
        }

        // Perform checkpoint
        let result = committer.checkpoint();
        assert!(result.is_ok(), "checkpoint should succeed");

        // Read back the persisted state and verify v1 format
        let db = JsonFileDatabase::open(&offsets_path).unwrap();
        let mut persister = db.persister("test");
        persister.load().unwrap();

        let state: PersistedStateV1 = persister.get_raw_json(KNOWN_FILES_KEY).unwrap();
        assert_eq!(state.version, 1, "Should be version 1");
        assert_eq!(state.files.len(), 1, "Should have 1 file entry");

        // Verify the entry contents
        let key = file_id_to_key(file_id.dev(), file_id.ino());
        let entry = state.files.get(&key).unwrap();
        assert_eq!(entry.dev, 1);
        assert_eq!(entry.ino, 100);
        assert_eq!(
            entry.offset, 150,
            "Offset should be high water mark (0+100 + 100+50 = 150)"
        );
        assert_eq!(entry.path, "/var/log/test.log");
        assert_eq!(entry.filename, "test.log");
    }

    #[tokio::test]
    async fn test_checkpoint_failure_threshold() {
        let temp_dir = TempDir::new().unwrap();

        // Create config with short failure threshold for testing
        let config = OffsetCommitterConfig {
            max_checkpoint_failure_duration: Duration::from_millis(50),
            ..Default::default()
        };

        let (mut committer, _offset_tracker) = create_test_committer(&temp_dir, config);

        // Make the persister directory read-only to cause sync failures
        let readonly_dir = temp_dir.path().join("readonly");
        std::fs::create_dir_all(&readonly_dir).unwrap();

        let readonly_db = JsonFileDatabase::open(readonly_dir.join("offsets.json")).unwrap();
        committer.persister = readonly_db.persister("test");

        // Make directory read-only (this will cause sync to fail)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&readonly_dir, std::fs::Permissions::from_mode(0o444))
                .unwrap();
        }

        // First checkpoint failure should return Ok (threshold not reached)
        let result = committer.maybe_checkpoint();
        #[cfg(unix)]
        assert!(
            result.is_ok(),
            "First checkpoint failure should return Ok (threshold not reached)"
        );

        // Wait for threshold to be exceeded
        std::thread::sleep(Duration::from_millis(60));

        // Next checkpoint should return Err (threshold breached)
        let result = committer.maybe_checkpoint();
        #[cfg(unix)]
        assert!(
            result.is_err(),
            "Checkpoint should return Err after threshold duration"
        );

        // Cleanup - restore permissions
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&readonly_dir, std::fs::Permissions::from_mode(0o755))
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_checkpoint_failure_resets_on_success() {
        let temp_dir = TempDir::new().unwrap();

        let config = OffsetCommitterConfig {
            max_checkpoint_failure_duration: Duration::from_millis(100),
            ..Default::default()
        };

        let (mut committer, _offset_tracker) = create_test_committer(&temp_dir, config);

        // Simulate a failure by setting checkpoint_first_failure
        committer.checkpoint_first_failure = Some(Instant::now());

        // Successful checkpoint should clear the failure tracking
        let result = committer.maybe_checkpoint();
        assert!(result.is_ok(), "checkpoint should succeed");
        assert!(
            committer.checkpoint_first_failure.is_none(),
            "checkpoint_first_failure should be cleared after success"
        );
    }
}

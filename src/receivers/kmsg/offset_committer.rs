// SPDX-License-Identifier: Apache-2.0

//! Offset committer for kmsg receiver at-least-once delivery.
//!
//! This module handles acknowledgement processing and state persistence separately
//! from the main kmsg receiver loop. This allows the committer to continue receiving
//! acks after the receiver stops reading, ensuring all in-flight messages are
//! acknowledged before shutdown.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::bounded_channel::BoundedReceiver;
use crate::receivers::kmsg::error::Result;
use crate::receivers::kmsg::offset_tracker::KmsgOffsetTracker;
use crate::receivers::kmsg::persistence::{self, PersistedKmsgState};
use crate::topology::payload::KmsgAcknowledgement;

/// Shared offset tracker type
pub type SharedOffsetTracker = Arc<Mutex<KmsgOffsetTracker>>;

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

/// Handles acknowledgement processing and state persistence for the kmsg receiver.
///
/// Runs as a separate task that survives after the main receiver loop stops,
/// allowing it to receive acks from exporters before final shutdown.
pub struct KmsgOffsetCommitter {
    /// Channel for receiving acknowledgements from the pipeline
    ack_receiver: BoundedReceiver<KmsgAcknowledgement>,
    /// Shared offset tracker (also used by receiver for tracking sends)
    offset_tracker: SharedOffsetTracker,
    /// Path to the persistence file
    offsets_path: std::path::PathBuf,
    /// Current boot ID for persistence
    boot_id: String,
    /// Configuration
    config: OffsetCommitterConfig,
    /// Interval timer for periodic checkpoints
    checkpoint_interval: tokio::time::Interval,
    /// Last checkpointed sequence (to avoid redundant writes)
    last_checkpointed_sequence: Option<u64>,
    /// Tracks when checkpoint failures started (for threshold-based exit)
    checkpoint_first_failure: Option<Instant>,
}

impl KmsgOffsetCommitter {
    /// Create a new KmsgOffsetCommitter
    pub fn new(
        ack_receiver: BoundedReceiver<KmsgAcknowledgement>,
        offset_tracker: SharedOffsetTracker,
        offsets_path: std::path::PathBuf,
        boot_id: String,
        config: OffsetCommitterConfig,
    ) -> Self {
        let checkpoint_interval = tokio::time::interval(config.checkpoint_interval);

        Self {
            ack_receiver,
            offset_tracker,
            offsets_path,
            boot_id,
            config,
            checkpoint_interval,
            last_checkpointed_sequence: None,
            checkpoint_first_failure: None,
        }
    }

    /// Run the main committer loop
    ///
    /// Processes acknowledgements and performs periodic checkpoints until cancelled.
    /// On cancellation, drains remaining acks and performs final checkpoint.
    /// Returns error if checkpoint failures persist beyond the configured threshold.
    pub async fn run(&mut self, cancel_token: CancellationToken) -> Result<()> {
        info!("Kmsg offset committer started");

        // Skip the first immediate tick
        self.checkpoint_interval.tick().await;

        let mut fatal_error = None;

        loop {
            select! {
                biased;

                // Periodic checkpoint
                _ = self.checkpoint_interval.tick() => {
                    if let Err(e) = self.maybe_checkpoint(false) {
                        error!("Checkpoint failures persisted beyond threshold, exiting: {}", e);
                        fatal_error = Some(e);
                        break;
                    }
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
                    debug!("KmsgOffsetCommitter cancelled, draining pending acknowledgements");
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
    fn process_ack(&self, ack: KmsgAcknowledgement) {
        let mut tracker = self
            .offset_tracker
            .lock()
            .expect("offset tracker mutex poisoned - this is a bug");
        match ack {
            KmsgAcknowledgement::Ack(kmsg_ack) => {
                debug!(
                    sequences = ?kmsg_ack.sequences.len(),
                    "Processing kmsg ack"
                );
                tracker.acknowledge_batch(&kmsg_ack.sequences);
            }
            KmsgAcknowledgement::Nack(kmsg_nack) => {
                debug!(
                    sequences = ?kmsg_nack.sequences.len(),
                    "Processing kmsg nack"
                );
                // nack() will panic if finite_retry_enabled is false
                tracker.nack_batch(&kmsg_nack.sequences);
            }
        }
    }

    /// Drain remaining acknowledgements with timeout and perform final checkpoint
    async fn drain(&mut self) {
        let drain_start = tokio::time::Instant::now();
        let drain_deadline = drain_start + self.config.drain_timeout;
        let mut ack_count = 0;

        debug!("Draining remaining kmsg acknowledgements");

        // Drain loop with deadline - always wait until deadline or channel closed
        loop {
            let remaining = drain_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                debug!(
                    "Drain deadline reached after processing {} acknowledgements",
                    ack_count
                );
                break;
            }

            let poll_res = tokio::time::timeout(remaining, self.ack_receiver.next()).await;

            match poll_res {
                Ok(Some(ack)) => {
                    self.process_ack(ack);
                    ack_count += 1;
                }
                Ok(None) => {
                    // Channel closed - all senders dropped, no more acks coming
                    debug!("Ack channel closed during drain after {} acks", ack_count);
                    break;
                }
                Err(_) => {
                    // Timeout - deadline reached
                    debug!(
                        "Drain timeout reached after processing {} acknowledgements",
                        ack_count
                    );
                    break;
                }
            }
        }

        if ack_count > 0 {
            info!("Drained {} pending acknowledgements", ack_count);
        }

        // Perform final checkpoint with sync_dir=true for durability
        debug!("Performing final checkpoint after draining");
        if let Err(e) = self.checkpoint(true) {
            warn!("Failed to perform final checkpoint: {}", e);
        } else {
            info!("Final checkpoint completed successfully");
        }
    }

    /// Conditionally checkpoint and track failures.
    /// Returns Ok on success or if failure is within threshold.
    /// Returns Err only when failure duration threshold is breached.
    fn maybe_checkpoint(&mut self, sync_dir: bool) -> Result<()> {
        match self.checkpoint(sync_dir) {
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
    fn checkpoint(&mut self, sync_dir: bool) -> Result<()> {
        // Get persistable sequence from the offset tracker
        let persistable_sequence = {
            let tracker = self
                .offset_tracker
                .lock()
                .expect("offset tracker mutex poisoned - this is a bug");
            tracker.get_persistable_sequence()
        };

        let Some(seq) = persistable_sequence else {
            // No state to persist yet
            return Ok(());
        };

        // Skip if unchanged
        if Some(seq) == self.last_checkpointed_sequence {
            return Ok(());
        }

        let state = PersistedKmsgState::new(self.boot_id.clone(), seq);
        persistence::save_state(&self.offsets_path, &state, sync_dir)?;

        debug!(sequence = seq, "Checkpointed kmsg offset");
        self.last_checkpointed_sequence = Some(seq);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_channel;
    use crate::topology::payload::KmsgAck;
    use tempfile::TempDir;

    /// Create a test committer with the given config
    fn create_test_committer(
        temp_dir: &TempDir,
        config: OffsetCommitterConfig,
    ) -> (KmsgOffsetCommitter, SharedOffsetTracker) {
        let offsets_path = temp_dir.path().join("kmsg_offsets.json");

        let (_ack_tx, ack_rx) = bounded_channel::bounded(10);
        let offset_tracker: SharedOffsetTracker =
            Arc::new(Mutex::new(KmsgOffsetTracker::new(false)));

        let committer = KmsgOffsetCommitter::new(
            ack_rx,
            offset_tracker.clone(),
            offsets_path,
            "test-boot-id".to_string(),
            config,
        );

        (committer, offset_tracker)
    }

    #[tokio::test]
    async fn test_checkpoint_writes_state() {
        let temp_dir = TempDir::new().unwrap();
        let offsets_path = temp_dir.path().join("kmsg_offsets.json");

        let (mut committer, offset_tracker) =
            create_test_committer(&temp_dir, OffsetCommitterConfig::default());

        // Register some sequences in the offset tracker
        {
            let mut tracker = offset_tracker.lock().expect("test mutex");
            tracker.track_batch(&[100, 101, 102]);
            tracker.acknowledge_batch(&[100, 101, 102]);
        }

        // Perform checkpoint
        let result = committer.checkpoint(true);
        assert!(result.is_ok(), "checkpoint should succeed");

        // Read back the persisted state and verify
        let state = persistence::load_state(&offsets_path).unwrap();
        assert_eq!(state.boot_id, "test-boot-id");
        assert_eq!(state.sequence, 103); // hwm (102) + 1
    }

    #[tokio::test]
    async fn test_checkpoint_skips_when_unchanged() {
        let temp_dir = TempDir::new().unwrap();

        let (mut committer, offset_tracker) =
            create_test_committer(&temp_dir, OffsetCommitterConfig::default());

        // Track and ack some sequences
        {
            let mut tracker = offset_tracker.lock().expect("test mutex");
            tracker.track(100);
            tracker.acknowledge(100);
        }

        // First checkpoint - hwm=100, no pending, so persistable = hwm+1 = 101
        committer.checkpoint(false).unwrap();
        assert_eq!(committer.last_checkpointed_sequence, Some(101));

        // Second checkpoint with same sequence should succeed without writing
        committer.checkpoint(false).unwrap();
        assert_eq!(committer.last_checkpointed_sequence, Some(101));
    }

    #[tokio::test]
    async fn test_checkpoint_failure_threshold() {
        let temp_dir = TempDir::new().unwrap();

        // Create config with short failure threshold for testing
        let config = OffsetCommitterConfig {
            max_checkpoint_failure_duration: Duration::from_millis(50),
            ..Default::default()
        };

        let (mut committer, offset_tracker) = create_test_committer(&temp_dir, config);

        // Track some sequences so there's something to persist
        {
            let mut tracker = offset_tracker.lock().expect("test mutex");
            tracker.track(100);
            tracker.acknowledge(100);
        }

        // Make the persister directory read-only to cause sync failures
        let readonly_dir = temp_dir.path().join("readonly");
        std::fs::create_dir_all(&readonly_dir).unwrap();

        // Point committer to invalid path
        let blocker_file = readonly_dir.join("blocker");
        std::fs::write(&blocker_file, "I am a file").unwrap();
        committer.offsets_path = blocker_file.join("offsets.json");

        // First checkpoint failure should return Ok (threshold not reached)
        let result = committer.maybe_checkpoint(false);
        assert!(
            result.is_ok(),
            "First checkpoint failure should return Ok (threshold not reached)"
        );

        // Wait for threshold to be exceeded
        std::thread::sleep(Duration::from_millis(60));

        // Next checkpoint should return Err (threshold breached)
        let result = committer.maybe_checkpoint(false);
        assert!(
            result.is_err(),
            "Checkpoint should return Err after threshold duration"
        );
    }

    #[tokio::test]
    async fn test_process_ack() {
        let temp_dir = TempDir::new().unwrap();

        let (committer, offset_tracker) =
            create_test_committer(&temp_dir, OffsetCommitterConfig::default());

        // Track some sequences
        {
            let mut tracker = offset_tracker.lock().expect("test mutex");
            tracker.track_batch(&[100, 101, 102]);
        }

        // Process an ack
        let ack = KmsgAcknowledgement::Ack(KmsgAck {
            sequences: vec![100, 101],
        });
        committer.process_ack(ack);

        // Verify tracker state
        {
            let tracker = offset_tracker.lock().expect("test mutex");
            assert_eq!(tracker.pending_count(), 1);
            assert_eq!(tracker.lowest_pending(), Some(102));
            assert_eq!(tracker.high_water_mark(), Some(101));
        }
    }

    #[tokio::test]
    async fn test_checkpoint_with_pending_returns_lowest() {
        let temp_dir = TempDir::new().unwrap();
        let offsets_path = temp_dir.path().join("kmsg_offsets.json");

        let (mut committer, offset_tracker) =
            create_test_committer(&temp_dir, OffsetCommitterConfig::default());

        // Track sequences, only ack some (out of order)
        {
            let mut tracker = offset_tracker.lock().expect("test mutex");
            tracker.track_batch(&[100, 101, 102, 103]);
            // Ack 102 and 103 but not 100 and 101
            tracker.acknowledge(102);
            tracker.acknowledge(103);
        }

        // Perform checkpoint
        committer.checkpoint(true).unwrap();

        // Read back - should have lowest_pending (100), not hwm + 1 (104)
        // On resume we skip seq < 100, so we re-read from sequence 100
        let state = persistence::load_state(&offsets_path).unwrap();
        assert_eq!(state.sequence, 100);
    }

    /// End-to-end test: receiver tracks → channel acks → committer checkpoints
    ///
    /// This test verifies the full at-least-once delivery flow:
    /// 1. Receiver tracks sequences in offset tracker when sending batches
    /// 2. Exporter sends acks back via the ack channel
    /// 3. Committer processes acks and updates tracker
    /// 4. Checkpoint persists the correct sequence
    #[tokio::test]
    async fn test_end_to_end_ack_flow() {
        let temp_dir = TempDir::new().unwrap();
        let offsets_path = temp_dir.path().join("kmsg_offsets.json");

        // Create ack channel (simulating receiver → committer communication)
        let (ack_tx, ack_rx) = bounded_channel::bounded::<KmsgAcknowledgement>(10);

        // Create shared offset tracker
        let offset_tracker: SharedOffsetTracker =
            Arc::new(Mutex::new(KmsgOffsetTracker::new(false)));

        // Create committer
        let config = OffsetCommitterConfig {
            checkpoint_interval: Duration::from_millis(50),
            drain_timeout: Duration::from_millis(500),
            ..Default::default()
        };

        let mut committer = KmsgOffsetCommitter::new(
            ack_rx,
            offset_tracker.clone(),
            offsets_path.clone(),
            "test-boot-id".to_string(),
            config,
        );

        let cancel = CancellationToken::new();
        let committer_cancel = cancel.clone();

        // Start committer in background
        let committer_handle = tokio::spawn(async move { committer.run(committer_cancel).await });

        // Simulate receiver: track sequences (as BatchSender would do)
        {
            let mut tracker = offset_tracker.lock().expect("test mutex");
            tracker.track_batch(&[100, 101, 102]);
        }

        // Simulate exporter: send ack for first two sequences
        ack_tx
            .send(KmsgAcknowledgement::Ack(KmsgAck {
                sequences: vec![100, 101],
            }))
            .await
            .unwrap();

        // Give committer time to process ack and checkpoint
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify tracker state (102 still pending)
        {
            let tracker = offset_tracker.lock().expect("test mutex");
            assert_eq!(tracker.pending_count(), 1, "Should have 1 pending");
            assert_eq!(tracker.lowest_pending(), Some(102));
            assert_eq!(tracker.high_water_mark(), Some(101));
        }

        // Send ack for remaining sequence
        ack_tx
            .send(KmsgAcknowledgement::Ack(KmsgAck {
                sequences: vec![102],
            }))
            .await
            .unwrap();

        // Give committer time to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify all acked
        {
            let tracker = offset_tracker.lock().expect("test mutex");
            assert_eq!(tracker.pending_count(), 0, "Should have no pending");
            assert_eq!(tracker.high_water_mark(), Some(102));
        }

        // Close channel to trigger drain and shutdown
        drop(ack_tx);

        // Wait for committer to finish
        let result = tokio::time::timeout(Duration::from_secs(2), committer_handle)
            .await
            .expect("Committer should finish within timeout")
            .expect("Committer task should not panic");
        assert!(result.is_ok(), "Committer should exit successfully");

        // Verify final checkpoint was written with hwm+1 (103)
        let state = persistence::load_state(&offsets_path).unwrap();
        assert_eq!(state.boot_id, "test-boot-id");
        assert_eq!(state.sequence, 103, "Should persist hwm+1 when all acked");
    }

    /// Test drain behavior waits for acks until deadline
    #[tokio::test]
    async fn test_drain_waits_for_slow_acks() {
        let temp_dir = TempDir::new().unwrap();
        let offsets_path = temp_dir.path().join("kmsg_offsets.json");

        let (ack_tx, ack_rx) = bounded_channel::bounded::<KmsgAcknowledgement>(10);
        let offset_tracker: SharedOffsetTracker =
            Arc::new(Mutex::new(KmsgOffsetTracker::new(false)));

        let config = OffsetCommitterConfig {
            checkpoint_interval: Duration::from_secs(60), // Won't tick during test
            drain_timeout: Duration::from_millis(500),
            ..Default::default()
        };

        let mut committer = KmsgOffsetCommitter::new(
            ack_rx,
            offset_tracker.clone(),
            offsets_path.clone(),
            "test-boot-id".to_string(),
            config,
        );

        // Track sequences
        {
            let mut tracker = offset_tracker.lock().expect("test mutex");
            tracker.track_batch(&[100, 101, 102]);
        }

        let cancel = CancellationToken::new();
        let committer_cancel = cancel.clone();

        // Start committer
        let committer_handle = tokio::spawn(async move { committer.run(committer_cancel).await });

        // Send first ack
        ack_tx
            .send(KmsgAcknowledgement::Ack(KmsgAck {
                sequences: vec![100],
            }))
            .await
            .unwrap();

        // Close channel to trigger drain
        drop(ack_tx);

        // Wait for committer - it should drain remaining acks (none) and checkpoint
        let result = tokio::time::timeout(Duration::from_secs(2), committer_handle)
            .await
            .expect("Committer should finish within timeout")
            .expect("Committer task should not panic");
        assert!(result.is_ok());

        // Final checkpoint should have lowest_pending (101) since 101, 102 weren't acked
        let state = persistence::load_state(&offsets_path).unwrap();
        assert_eq!(state.sequence, 101, "Should persist lowest_pending");
    }
}

// SPDX-License-Identifier: Apache-2.0

//! Offset tracking for at-least-once delivery in the kmsg receiver.
//!
//! This module tracks in-flight sequence numbers, ensuring that we only persist
//! the sequence we want to resume from. This prevents data loss if the process
//! crashes between reading and exporting.
//!
//! ## Sequence Semantics
//!
//! The kernel ring buffer assigns a monotonically increasing sequence number to
//! each message. We track these sequences as "pending" until acknowledged by
//! exporters. The persistable sequence is:
//! - If pending sequences exist: the lowest pending (to re-read unacked messages)
//! - If all acknowledged: high_water_mark + 1 (the next sequence to read)

use std::collections::BTreeSet;

/// Tracks pending sequence numbers for at-least-once delivery.
///
/// Uses a BTreeSet to maintain sorted sequences, allowing O(1) access
/// to the minimum pending sequence. The persistable sequence is the lowest
/// pending sequence, or hwm+1 if all sequences are acknowledged.
pub struct KmsgOffsetTracker {
    /// Set of pending (unacknowledged) sequence numbers
    pending: BTreeSet<u64>,
    /// High water mark: highest acknowledged sequence number
    high_water_mark: Option<u64>,
    /// Whether finite retry is enabled (when false, nacks should panic)
    finite_retry_enabled: bool,
}

impl KmsgOffsetTracker {
    /// Create a new KmsgOffsetTracker
    pub fn new(finite_retry_enabled: bool) -> Self {
        Self {
            pending: BTreeSet::new(),
            high_water_mark: None,
            finite_retry_enabled,
        }
    }

    /// Track a sequence number as in-flight.
    /// Call this when dispatching a batch to the pipeline.
    pub fn track(&mut self, sequence: u64) {
        self.pending.insert(sequence);
    }

    /// Track multiple sequence numbers as in-flight.
    /// Call this when dispatching a batch with multiple messages.
    pub fn track_batch(&mut self, sequences: &[u64]) {
        for &seq in sequences {
            self.pending.insert(seq);
        }
    }

    /// Acknowledge a sequence number.
    /// Call this when the exporter successfully exports the batch.
    pub fn acknowledge(&mut self, sequence: u64) {
        // Update high water mark if this sequence is higher
        self.high_water_mark = Some(
            self.high_water_mark
                .map_or(sequence, |hwm| hwm.max(sequence)),
        );

        // Remove from pending
        self.pending.remove(&sequence);
    }

    /// Acknowledge multiple sequence numbers.
    /// Call this when the exporter successfully exports a batch.
    pub fn acknowledge_batch(&mut self, sequences: &[u64]) {
        for &seq in sequences {
            self.acknowledge(seq);
        }
    }

    /// Handle a nacked message.
    /// With indefinite retry enabled (default), this panics to prevent data loss.
    /// With finite retry enabled, this behaves like acknowledge to prevent blocking.
    pub fn nack(&mut self, sequence: u64) {
        if !self.finite_retry_enabled {
            panic!(
                "CRITICAL BUG: nack() called on kmsg offset tracker with indefinite retry enabled. \
                 This indicates exporter retry logic failure with infinite retries configured. \
                 Cannot safely acknowledge nacked sequence - would cause data loss. \
                 System must abort to prevent silent data corruption. \
                 sequence={}",
                sequence
            );
        }

        tracing::debug!(
            sequence = sequence,
            "Received nack for message, treating as acknowledge to prevent blocking"
        );

        // When finite retry is enabled, nack behaves exactly like ack
        self.acknowledge(sequence);
    }

    /// Handle nacks for multiple sequence numbers.
    pub fn nack_batch(&mut self, sequences: &[u64]) {
        for &seq in sequences {
            self.nack(seq);
        }
    }

    /// Get the lowest pending sequence number, or None if no sequences are pending.
    pub fn lowest_pending(&self) -> Option<u64> {
        self.pending.first().copied()
    }

    /// Get the high water mark (highest acknowledged sequence).
    pub fn high_water_mark(&self) -> Option<u64> {
        self.high_water_mark
    }

    /// Get the sequence to persist.
    ///
    /// Returns the safe restart point:
    /// - If there are pending sequences: returns the lowest pending sequence
    ///   (on resume, we skip sequences *before* this and re-read from this one)
    /// - If no pending sequences: returns high_water_mark + 1
    ///   (the next sequence to read, since all up to hwm are acked)
    /// - None if no state has been tracked yet
    ///
    /// The resume logic skips sequences `< persisted_sequence`, so we persist
    /// the exact sequence we want to resume from.
    pub fn get_persistable_sequence(&self) -> Option<u64> {
        // If there are pending sequences, return the lowest one.
        // The resume logic skips seq < persisted, so we re-read from lowest_pending.
        if let Some(lowest_pending) = self.lowest_pending() {
            return Some(lowest_pending);
        }

        // No pending sequences - return hwm + 1 (next sequence to read)
        // This correctly handles the case where hwm is at u64::MAX (saturates)
        self.high_water_mark.map(|hwm| hwm.saturating_add(1))
    }

    /// Get the number of pending (unacknowledged) sequences.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Check if there are any pending sequences.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Clear all tracking state.
    ///
    /// Called on ring buffer overflow when messages have been lost and
    /// pending sequences may reference evicted messages. Resets to a
    /// clean state to avoid checkpointing stale/invalid sequences.
    pub fn clear(&mut self) {
        self.pending.clear();
        self.high_water_mark = None;
    }
}

impl Default for KmsgOffsetTracker {
    fn default() -> Self {
        Self::new(false) // Default to indefinite retry (safer default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_track_and_ack() {
        let mut tracker = KmsgOffsetTracker::new(false);

        // Track some sequences
        tracker.track(100);
        tracker.track(101);
        tracker.track(102);

        // Verify pending count
        assert_eq!(tracker.pending_count(), 3);
        assert!(tracker.has_pending());

        // Get lowest pending - should be 100
        assert_eq!(tracker.lowest_pending(), Some(100));

        // Acknowledge first sequence
        tracker.acknowledge(100);
        assert_eq!(tracker.pending_count(), 2);

        // Lowest pending should now be 101
        assert_eq!(tracker.lowest_pending(), Some(101));

        // High water mark should be 100
        assert_eq!(tracker.high_water_mark(), Some(100));
    }

    #[test]
    fn test_batch_track_and_ack() {
        let mut tracker = KmsgOffsetTracker::new(false);

        // Track batch of sequences
        tracker.track_batch(&[100, 101, 102, 103, 104]);

        assert_eq!(tracker.pending_count(), 5);
        assert_eq!(tracker.lowest_pending(), Some(100));

        // Acknowledge batch
        tracker.acknowledge_batch(&[100, 101, 102]);
        assert_eq!(tracker.pending_count(), 2);
        assert_eq!(tracker.lowest_pending(), Some(103));
        assert_eq!(tracker.high_water_mark(), Some(102));
    }

    #[test]
    fn test_out_of_order_acks() {
        let mut tracker = KmsgOffsetTracker::new(false);

        // Track sequential sequences
        tracker.track_batch(&[100, 101, 102, 103, 104]);

        // Ack out of order: 102, 104
        tracker.acknowledge(102);
        tracker.acknowledge(104);

        // Lowest pending should still be 100
        assert_eq!(tracker.lowest_pending(), Some(100));
        // High water mark should be 104
        assert_eq!(tracker.high_water_mark(), Some(104));

        // Ack 100
        tracker.acknowledge(100);

        // Lowest pending should now be 101
        assert_eq!(tracker.lowest_pending(), Some(101));

        // Ack 101
        tracker.acknowledge(101);

        // Lowest pending should now be 103 (102 was already acked)
        assert_eq!(tracker.lowest_pending(), Some(103));
    }

    #[test]
    fn test_empty_state() {
        let mut tracker = KmsgOffsetTracker::new(false);

        // Should return None when no sequences tracked
        assert_eq!(tracker.lowest_pending(), None);
        assert_eq!(tracker.high_water_mark(), None);
        assert_eq!(tracker.get_persistable_sequence(), None);
        assert!(!tracker.has_pending());

        // Track and then ack all sequences
        tracker.track_batch(&[100, 101]);
        tracker.acknowledge(100);
        tracker.acknowledge(101);

        // Should return None for lowest_pending when all acked
        assert_eq!(tracker.lowest_pending(), None);
        assert_eq!(tracker.pending_count(), 0);
        // High water mark should be 101
        assert_eq!(tracker.high_water_mark(), Some(101));
    }

    #[test]
    fn test_get_persistable_sequence_with_pending() {
        let mut tracker = KmsgOffsetTracker::new(false);

        // No state yet
        assert_eq!(tracker.get_persistable_sequence(), None);

        // Track sequences
        tracker.track_batch(&[100, 101, 102]);

        // Should return lowest_pending (resume from seq 100)
        assert_eq!(tracker.get_persistable_sequence(), Some(100));

        // Ack first sequence
        tracker.acknowledge(100);

        // Should return new lowest_pending (resume from seq 101)
        assert_eq!(tracker.get_persistable_sequence(), Some(101));
    }

    #[test]
    fn test_get_persistable_sequence_all_acked() {
        let mut tracker = KmsgOffsetTracker::new(false);

        // Track and ack all sequences
        tracker.track_batch(&[100, 101, 102]);
        tracker.acknowledge_batch(&[100, 101, 102]);

        // No pending - should return hwm + 1 (next sequence to read)
        assert_eq!(tracker.get_persistable_sequence(), Some(103));
    }

    #[test]
    fn test_get_persistable_sequence_out_of_order_acks() {
        let mut tracker = KmsgOffsetTracker::new(false);

        tracker.track_batch(&[100, 101, 102]);

        // Ack out of order: 102 first
        tracker.acknowledge(102);

        // Should return lowest_pending (resume from 100)
        assert_eq!(tracker.get_persistable_sequence(), Some(100));
        // hwm should be 102
        assert_eq!(tracker.high_water_mark(), Some(102));

        // Ack 100
        tracker.acknowledge(100);

        // Should return lowest_pending (resume from 101)
        assert_eq!(tracker.get_persistable_sequence(), Some(101));

        // Ack 101
        tracker.acknowledge(101);

        // All acked - should return hwm + 1 (103)
        assert_eq!(tracker.get_persistable_sequence(), Some(103));
    }

    #[test]
    fn test_nack_with_finite_retry() {
        let mut tracker = KmsgOffsetTracker::new(true); // finite retry enabled

        // Track some sequences
        tracker.track_batch(&[100, 101, 102]);

        // Nack first sequence - should behave like ack
        tracker.nack(100);
        assert_eq!(tracker.pending_count(), 2);
        assert_eq!(tracker.lowest_pending(), Some(101));
        assert_eq!(tracker.high_water_mark(), Some(100));
    }

    #[test]
    #[should_panic(
        expected = "CRITICAL BUG: nack() called on kmsg offset tracker with indefinite retry enabled"
    )]
    fn test_nack_panics_with_indefinite_retry() {
        let mut tracker = KmsgOffsetTracker::new(false); // indefinite retry

        tracker.track(100);
        // This should panic
        tracker.nack(100);
    }

    #[test]
    fn test_duplicate_tracking() {
        let mut tracker = KmsgOffsetTracker::new(false);

        // Track same sequence multiple times (idempotent)
        tracker.track(100);
        tracker.track(100);
        tracker.track(100);

        // Should only count once
        assert_eq!(tracker.pending_count(), 1);

        // Single ack should clear it
        tracker.acknowledge(100);
        assert_eq!(tracker.pending_count(), 0);
    }

    #[test]
    fn test_ack_non_existent() {
        let mut tracker = KmsgOffsetTracker::new(false);

        // Acking non-existent sequence should be safe no-op
        tracker.acknowledge(999);

        // Track some sequences
        tracker.track(100);

        // Ack wrong sequence - should be no-op for pending set
        tracker.acknowledge(999);
        assert_eq!(tracker.pending_count(), 1);

        // But hwm should be updated
        assert_eq!(tracker.high_water_mark(), Some(999));
    }

    #[test]
    fn test_clear() {
        let mut tracker = KmsgOffsetTracker::new(false);

        // Build up some state
        tracker.track_batch(&[100, 101, 102]);
        tracker.acknowledge(100);
        assert_eq!(tracker.pending_count(), 2);
        assert_eq!(tracker.high_water_mark(), Some(100));

        // Clear all state
        tracker.clear();

        // Verify everything is reset
        assert_eq!(tracker.pending_count(), 0);
        assert!(!tracker.has_pending());
        assert_eq!(tracker.high_water_mark(), None);
        assert_eq!(tracker.lowest_pending(), None);
        assert_eq!(tracker.get_persistable_sequence(), None);
    }

    #[test]
    fn test_persistable_sequence_scenario() {
        // This test validates the exact at-least-once delivery scenario:
        // Seq 100, 101, 102 are tracked.
        // If we crash, we want to restart and re-read from lowest pending.
        // Resume skips seq < persisted, so we persist exactly the seq we want to resume from.

        let mut tracker = KmsgOffsetTracker::new(false);

        tracker.track_batch(&[100, 101, 102]);

        // Initial state: all pending, persist 100 to resume from there
        assert_eq!(tracker.get_persistable_sequence(), Some(100));

        // 102 acks first (out of order)
        tracker.acknowledge(102);
        assert_eq!(tracker.get_persistable_sequence(), Some(100)); // Still 100 (100 pending)
        assert_eq!(tracker.high_water_mark(), Some(102));

        // 100 acks
        tracker.acknowledge(100);
        assert_eq!(tracker.get_persistable_sequence(), Some(101)); // Now 101 (101 pending)
        assert_eq!(tracker.high_water_mark(), Some(102)); // hwm unchanged

        // 101 acks
        tracker.acknowledge(101);
        assert_eq!(tracker.get_persistable_sequence(), Some(103)); // All acked, return hwm + 1
    }

    #[test]
    fn test_persistable_sequence_with_seq_zero() {
        // Edge case: sequence 0 is the lowest pending
        let mut tracker = KmsgOffsetTracker::new(false);

        tracker.track_batch(&[0, 1, 2]);

        // Should return 0 (resume from seq 0)
        assert_eq!(tracker.get_persistable_sequence(), Some(0));

        // Ack seq 0
        tracker.acknowledge(0);
        assert_eq!(tracker.get_persistable_sequence(), Some(1));

        // Ack all
        tracker.acknowledge_batch(&[1, 2]);
        assert_eq!(tracker.get_persistable_sequence(), Some(3)); // hwm + 1
    }
}

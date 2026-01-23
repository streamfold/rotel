// SPDX-License-Identifier: Apache-2.0

//! Offset tracking for at-least-once delivery in the file receiver.
//!
//! This module tracks in-flight offsets per file, ensuring that we only commit
//! the lowest pending offset when exporters acknowledge batches. This prevents
//! data loss if the process crashes between reading and exporting.
//!
//! Similar to the Kafka offset tracker, but keyed by FileId instead of topic/partition.
//!
//! ## Offset Semantics
//!
//! Offsets are tracked as `LineOffset { offset, len }` pairs where:
//! - `offset` is the byte position where a line BEGINS in the file
//! - `len` is the length of the line in bytes (including newline)
//!
//! This allows the log record's offset to represent "where this record starts"
//! while still being able to compute the resume position (offset + len).

use std::collections::{BTreeMap, HashMap};

use crate::receivers::file::input::FileId;

/// Represents a line's position in a file.
///
/// The offset is the byte position where the line BEGINS, and len is the
/// total bytes consumed (including newline). This allows computing the
/// end position as `offset + len`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineOffset {
    /// Byte position where the line begins
    pub offset: u64,
    /// Length of the line in bytes (including newline)
    pub len: u32,
}

impl LineOffset {
    /// Create a new LineOffset
    pub fn new(offset: u64, len: u32) -> Self {
        Self { offset, len }
    }

    /// Get the ending byte position (offset + len)
    pub fn end_offset(&self) -> u64 {
        self.offset + self.len as u64
    }
}

/// Tracks pending offsets per file for at-least-once delivery.
///
/// Uses a BTreeMap per file to maintain sorted offsets, allowing O(1) access
/// to the minimum offset. The committable offset is the lowest pending offset
/// across all tracked offsets for a file.
///
/// Offsets are stored as (begin_offset -> len) pairs, where begin_offset is
/// the byte position where a line starts and len is the line's byte length.
pub struct FileOffsetTracker {
    /// Maps FileId to a sorted map of pending offsets (begin_offset -> len)
    files: HashMap<FileId, BTreeMap<u64, u32>>,
    /// High water marks per file (highest acknowledged offset with its length)
    high_water_marks: HashMap<FileId, LineOffset>,
    /// Whether finite retry is enabled (when false, nacks should panic)
    finite_retry_enabled: bool,
}

impl FileOffsetTracker {
    /// Create a new FileOffsetTracker
    pub fn new(finite_retry_enabled: bool) -> Self {
        Self {
            files: HashMap::new(),
            high_water_marks: HashMap::new(),
            finite_retry_enabled,
        }
    }

    /// Track an offset for a specific file.
    /// Call this when dispatching a batch to the pipeline.
    pub fn track(&mut self, file_id: FileId, line_offset: LineOffset) {
        let file_offsets = self.files.entry(file_id).or_default();
        file_offsets.insert(line_offset.offset, line_offset.len);
    }

    /// Track multiple offsets for a specific file.
    /// Call this when dispatching a batch with multiple lines.
    pub fn track_batch(&mut self, file_id: FileId, offsets: &[LineOffset]) {
        let file_offsets = self.files.entry(file_id).or_default();
        for line_offset in offsets {
            file_offsets.insert(line_offset.offset, line_offset.len);
        }
    }

    /// Acknowledge an offset for a specific file.
    /// Call this when the exporter successfully exports the batch.
    ///
    /// The LineOffset contains both the begin offset and length, which is needed
    /// to properly track the high water mark for computing the resume position.
    pub fn acknowledge(&mut self, file_id: FileId, line_offset: LineOffset) {
        // Update high water mark if this offset is higher
        let hwm = self
            .high_water_marks
            .entry(file_id)
            .or_insert(LineOffset::new(0, 0));
        if line_offset.offset > hwm.offset {
            *hwm = line_offset;
        }

        // Remove from pending (file entry stays - FileCoordinator manages file lifecycle)
        if let Some(file_offsets) = self.files.get_mut(&file_id) {
            file_offsets.remove(&line_offset.offset);
        }
    }

    /// Acknowledge multiple offsets for a specific file.
    /// Call this when the exporter successfully exports a batch.
    pub fn acknowledge_batch(&mut self, file_id: FileId, offsets: &[LineOffset]) {
        for &line_offset in offsets {
            self.acknowledge(file_id, line_offset);
        }
    }

    /// Handle a nacked message for a specific file.
    /// With indefinite retry enabled (default), this panics to prevent data loss.
    /// With finite retry enabled, this behaves like acknowledge to prevent blocking.
    pub fn nack(&mut self, file_id: FileId, line_offset: LineOffset) {
        if !self.finite_retry_enabled {
            panic!(
                "CRITICAL BUG: nack() called on file offset tracker with indefinite retry enabled. \
                 This indicates exporter retry logic failure with infinite retries configured. \
                 Cannot safely acknowledge nacked offset - would cause data loss. \
                 System must abort to prevent silent data corruption. \
                 file_id={}, offset={}",
                file_id, line_offset.offset
            );
        }

        tracing::debug!(
            file_id = %file_id,
            offset = line_offset.offset,
            len = line_offset.len,
            "Received nack for message, treating as acknowledge to prevent blocking"
        );

        // When finite retry is enabled, nack behaves exactly like ack
        self.acknowledge(file_id, line_offset);
    }

    /// Handle nacks for multiple offsets for a specific file.
    pub fn nack_batch(&mut self, file_id: FileId, offsets: &[LineOffset]) {
        for &line_offset in offsets {
            self.nack(file_id, line_offset);
        }
    }

    /// Get the committable offset for a specific file.
    /// This is the lowest pending begin offset, or None if no offsets are pending.
    pub fn get_committable_offset(&self, file_id: FileId) -> Option<u64> {
        self.files
            .get(&file_id)
            .and_then(|offsets| offsets.first_key_value().map(|(offset, _)| *offset))
    }

    /// Get committable offsets for all tracked files.
    /// Returns a map from FileId to the lowest pending begin offset.
    pub fn get_all_committable_offsets(&self) -> HashMap<FileId, u64> {
        let mut committable = HashMap::new();
        for (file_id, offsets) in &self.files {
            if let Some((min_offset, _)) = offsets.first_key_value() {
                committable.insert(*file_id, *min_offset);
            }
        }
        committable
    }

    /// Get the high water mark for a specific file (highest acknowledged offset with length).
    pub fn get_high_water_mark(&self, file_id: FileId) -> Option<LineOffset> {
        self.high_water_marks.get(&file_id).copied()
    }

    /// Get the number of pending offsets for a specific file.
    pub fn pending_count(&self, file_id: FileId) -> usize {
        self.files
            .get(&file_id)
            .map(|offsets| offsets.len())
            .unwrap_or(0)
    }

    /// Get the total number of pending offsets across all files.
    pub fn total_pending(&self) -> usize {
        self.files.values().map(|offsets| offsets.len()).sum()
    }

    /// Check if a file has any pending offsets.
    pub fn has_pending(&self, file_id: FileId) -> bool {
        self.files
            .get(&file_id)
            .map(|offsets| !offsets.is_empty())
            .unwrap_or(false)
    }

    /// Get the lowest pending offset for a specific file.
    pub fn lowest_pending_offset(&self, file_id: FileId) -> Option<u64> {
        self.get_committable_offset(file_id)
    }

    /// Get the offset to persist for a specific file.
    ///
    /// Returns the byte position to resume reading from:
    /// - If there are pending offsets: returns the lowest pending begin offset
    ///   (safe restart point - will re-read unacked lines)
    /// - If no pending offsets: returns hwm.offset + hwm.len (end of last acked line)
    /// - None if no state for this file
    pub fn get_persistable_offset(&self, file_id: FileId) -> Option<u64> {
        // If there are pending offsets, return the lowest one
        // This ensures at-least-once delivery: on restart we re-read from the
        // lowest unacked offset
        if let Some(lowest_pending) = self.get_committable_offset(file_id) {
            return Some(lowest_pending);
        }

        // No pending offsets - return end of high water mark (hwm.offset + hwm.len)
        // This is the position after the last fully acknowledged line
        self.high_water_marks
            .get(&file_id)
            .map(|hwm| hwm.end_offset())
    }

    /// Get persistable offsets for all files that have state.
    /// Returns a map from FileId to the offset to persist.
    pub fn get_all_persistable_offsets(&self) -> HashMap<FileId, u64> {
        let mut result = HashMap::new();

        // Collect all file IDs from both pending and high water marks
        let mut all_file_ids: std::collections::HashSet<FileId> =
            self.files.keys().copied().collect();
        all_file_ids.extend(self.high_water_marks.keys().copied());

        for file_id in all_file_ids {
            if let Some(offset) = self.get_persistable_offset(file_id) {
                result.insert(file_id, offset);
            }
        }

        result
    }

    /// Remove all tracking for a specific file.
    /// Call this when a file is closed/removed.
    pub fn remove_file(&mut self, file_id: FileId) {
        self.files.remove(&file_id);
        self.high_water_marks.remove(&file_id);
    }

    /// Get all currently tracked file IDs.
    pub fn tracked_files(&self) -> Vec<FileId> {
        self.files.keys().copied().collect()
    }
}

impl Default for FileOffsetTracker {
    fn default() -> Self {
        Self::new(false) // Default to indefinite retry (safer default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_file_id(dev: u64, ino: u64) -> FileId {
        FileId::new(dev, ino)
    }

    /// Helper to create a LineOffset with a default length of 10
    fn lo(offset: u64) -> LineOffset {
        LineOffset::new(offset, 10)
    }

    /// Helper to create a LineOffset with specific length
    fn lo_len(offset: u64, len: u32) -> LineOffset {
        LineOffset::new(offset, len)
    }

    #[test]
    fn test_line_offset_end_offset() {
        let lo = LineOffset::new(100, 25);
        assert_eq!(lo.end_offset(), 125);
    }

    #[test]
    fn test_basic_track_and_ack() {
        let mut tracker = FileOffsetTracker::new(false);
        let file_id = test_file_id(1, 100);

        // Track some offsets
        tracker.track(file_id, lo(100));
        tracker.track(file_id, lo(200));
        tracker.track(file_id, lo(300));

        // Verify pending count
        assert_eq!(tracker.pending_count(file_id), 3);
        assert_eq!(tracker.total_pending(), 3);

        // Get committable - should be lowest offset
        assert_eq!(tracker.get_committable_offset(file_id), Some(100));

        // Acknowledge first offset
        tracker.acknowledge(file_id, lo(100));
        assert_eq!(tracker.pending_count(file_id), 2);

        // Committable should now be 200
        assert_eq!(tracker.get_committable_offset(file_id), Some(200));
    }

    #[test]
    fn test_batch_track_and_ack() {
        let mut tracker = FileOffsetTracker::new(false);
        let file_id = test_file_id(1, 100);

        // Track batch of offsets
        let offsets = vec![lo(100), lo(200), lo(300), lo(400), lo(500)];
        tracker.track_batch(file_id, &offsets);

        assert_eq!(tracker.pending_count(file_id), 5);
        assert_eq!(tracker.get_committable_offset(file_id), Some(100));

        // Acknowledge batch
        tracker.acknowledge_batch(file_id, &[lo(100), lo(200), lo(300)]);
        assert_eq!(tracker.pending_count(file_id), 2);
        assert_eq!(tracker.get_committable_offset(file_id), Some(400));
    }

    #[test]
    fn test_out_of_order_acks() {
        let mut tracker = FileOffsetTracker::new(false);
        let file_id = test_file_id(1, 100);

        // Track sequential offsets
        tracker.track_batch(file_id, &[lo(100), lo(200), lo(300), lo(400), lo(500)]);

        // Ack out of order: 300, 500
        tracker.acknowledge(file_id, lo(300));
        tracker.acknowledge(file_id, lo(500));

        // Committable should still be 100 (lowest pending)
        assert_eq!(tracker.get_committable_offset(file_id), Some(100));

        // Ack 100
        tracker.acknowledge(file_id, lo(100));

        // Committable should now be 200
        assert_eq!(tracker.get_committable_offset(file_id), Some(200));

        // Ack 200
        tracker.acknowledge(file_id, lo(200));

        // Committable should now be 400 (300 was already acked)
        assert_eq!(tracker.get_committable_offset(file_id), Some(400));
    }

    #[test]
    fn test_multiple_files() {
        let mut tracker = FileOffsetTracker::new(false);
        let file1 = test_file_id(1, 100);
        let file2 = test_file_id(1, 200);
        let file3 = test_file_id(2, 100);

        // Track offsets for different files
        tracker.track(file1, lo(100));
        tracker.track(file1, lo(200));
        tracker.track(file2, lo(300));
        tracker.track(file2, lo(400));
        tracker.track(file3, lo(500));

        // Verify counts
        assert_eq!(tracker.pending_count(file1), 2);
        assert_eq!(tracker.pending_count(file2), 2);
        assert_eq!(tracker.pending_count(file3), 1);
        assert_eq!(tracker.total_pending(), 5);

        // Get all committable offsets
        let committable = tracker.get_all_committable_offsets();
        assert_eq!(committable.get(&file1), Some(&100));
        assert_eq!(committable.get(&file2), Some(&300));
        assert_eq!(committable.get(&file3), Some(&500));

        // Ack some offsets
        tracker.acknowledge(file1, lo(100));
        tracker.acknowledge(file2, lo(300));

        // Verify updated committable
        let committable = tracker.get_all_committable_offsets();
        assert_eq!(committable.get(&file1), Some(&200));
        assert_eq!(committable.get(&file2), Some(&400));
        assert_eq!(committable.get(&file3), Some(&500));
    }

    #[test]
    fn test_empty_state() {
        let mut tracker = FileOffsetTracker::new(false);
        let file_id = test_file_id(1, 100);

        // Should return None when no offsets tracked
        assert_eq!(tracker.get_committable_offset(file_id), None);
        assert!(tracker.get_all_committable_offsets().is_empty());

        // Track and then ack all offsets
        tracker.track_batch(file_id, &[lo(100), lo(200)]);
        tracker.acknowledge(file_id, lo(100));
        tracker.acknowledge(file_id, lo(200));

        // Should return None when all offsets acked
        assert_eq!(tracker.get_committable_offset(file_id), None);
        assert_eq!(tracker.pending_count(file_id), 0);
    }

    #[test]
    fn test_high_water_mark() {
        let mut tracker = FileOffsetTracker::new(false);
        let file_id = test_file_id(1, 100);

        // Initially no high water mark
        assert_eq!(tracker.get_high_water_mark(file_id), None);

        // Track and ack offsets
        tracker.track_batch(file_id, &[lo(100), lo(200), lo(300), lo(400), lo(500)]);

        tracker.acknowledge(file_id, lo(100));
        assert_eq!(tracker.get_high_water_mark(file_id), Some(lo(100)));

        tracker.acknowledge(file_id, lo(300)); // Out of order
        assert_eq!(tracker.get_high_water_mark(file_id), Some(lo(300)));

        tracker.acknowledge(file_id, lo(200));
        assert_eq!(tracker.get_high_water_mark(file_id), Some(lo(300))); // Still 300

        tracker.acknowledge(file_id, lo(500));
        assert_eq!(tracker.get_high_water_mark(file_id), Some(lo(500)));
    }

    #[test]
    fn test_duplicate_tracking() {
        let mut tracker = FileOffsetTracker::new(false);
        let file_id = test_file_id(1, 100);

        // Track same offset multiple times (idempotent)
        tracker.track(file_id, lo(100));
        tracker.track(file_id, lo(100));
        tracker.track(file_id, lo(100));

        // Should only count once
        assert_eq!(tracker.pending_count(file_id), 1);

        // Single ack should clear it
        tracker.acknowledge(file_id, lo(100));
        assert_eq!(tracker.pending_count(file_id), 0);
    }

    #[test]
    fn test_ack_non_existent() {
        let mut tracker = FileOffsetTracker::new(false);
        let file_id = test_file_id(1, 100);
        let other_file = test_file_id(1, 999);

        // Acking non-existent file should be safe no-op (logs warning)
        tracker.acknowledge(other_file, lo(123));

        // Track some offsets
        tracker.track(file_id, lo(100));

        // Ack wrong offset - should be no-op
        tracker.acknowledge(file_id, lo(999));
        assert_eq!(tracker.pending_count(file_id), 1);
    }

    #[test]
    fn test_nack_with_finite_retry() {
        let mut tracker = FileOffsetTracker::new(true); // finite retry enabled
        let file_id = test_file_id(1, 100);

        // Track some offsets
        tracker.track_batch(file_id, &[lo(100), lo(200), lo(300)]);

        // Nack first offset - should behave like ack
        tracker.nack(file_id, lo(100));
        assert_eq!(tracker.pending_count(file_id), 2);
        assert_eq!(tracker.get_committable_offset(file_id), Some(200));
        assert_eq!(tracker.get_high_water_mark(file_id), Some(lo(100)));
    }

    #[test]
    #[should_panic(
        expected = "CRITICAL BUG: nack() called on file offset tracker with indefinite retry enabled"
    )]
    fn test_nack_panics_with_indefinite_retry() {
        let mut tracker = FileOffsetTracker::new(false); // indefinite retry
        let file_id = test_file_id(1, 100);

        tracker.track(file_id, lo(100));
        // This should panic
        tracker.nack(file_id, lo(100));
    }

    #[test]
    fn test_remove_file() {
        let mut tracker = FileOffsetTracker::new(false);
        let file1 = test_file_id(1, 100);
        let file2 = test_file_id(1, 200);

        // Track offsets for both files
        tracker.track_batch(file1, &[lo(100), lo(200)]);
        tracker.track_batch(file2, &[lo(300), lo(400)]);

        // Acknowledge some to set high water marks
        tracker.acknowledge(file1, lo(100));
        tracker.acknowledge(file2, lo(300));

        assert_eq!(tracker.total_pending(), 2);
        assert!(tracker.get_high_water_mark(file1).is_some());

        // Remove file1
        tracker.remove_file(file1);

        // file1 should be gone
        assert_eq!(tracker.pending_count(file1), 0);
        assert_eq!(tracker.get_high_water_mark(file1), None);
        assert!(!tracker.has_pending(file1));

        // file2 should still exist
        assert_eq!(tracker.pending_count(file2), 1);
        assert_eq!(tracker.get_high_water_mark(file2), Some(lo(300)));
    }

    #[test]
    fn test_tracked_files() {
        let mut tracker = FileOffsetTracker::new(false);
        let file1 = test_file_id(1, 100);
        let file2 = test_file_id(1, 200);

        assert!(tracker.tracked_files().is_empty());

        tracker.track(file1, lo(100));
        tracker.track(file2, lo(200));

        let tracked = tracker.tracked_files();
        assert_eq!(tracked.len(), 2);
        assert!(tracked.contains(&file1));
        assert!(tracked.contains(&file2));
    }

    #[test]
    fn test_has_pending() {
        let mut tracker = FileOffsetTracker::new(false);
        let file_id = test_file_id(1, 100);

        assert!(!tracker.has_pending(file_id));

        tracker.track(file_id, lo(100));
        assert!(tracker.has_pending(file_id));

        tracker.acknowledge(file_id, lo(100));
        assert!(!tracker.has_pending(file_id));
    }

    #[test]
    fn test_get_persistable_offset_with_pending() {
        let mut tracker = FileOffsetTracker::new(false);
        let file_id = test_file_id(1, 100);

        // No state yet
        assert_eq!(tracker.get_persistable_offset(file_id), None);

        // Track offsets
        tracker.track_batch(file_id, &[lo(100), lo(200), lo(300)]);

        // Should return lowest pending
        assert_eq!(tracker.get_persistable_offset(file_id), Some(100));

        // Ack first offset
        tracker.acknowledge(file_id, lo(100));

        // Should return new lowest pending
        assert_eq!(tracker.get_persistable_offset(file_id), Some(200));
    }

    #[test]
    fn test_get_persistable_offset_all_acked() {
        let mut tracker = FileOffsetTracker::new(false);
        let file_id = test_file_id(1, 100);

        // Track and ack all offsets with specific lengths
        // Line at 100 with len 50 -> ends at 150
        // Line at 150 with len 30 -> ends at 180
        // Line at 180 with len 20 -> ends at 200
        tracker.track_batch(
            file_id,
            &[lo_len(100, 50), lo_len(150, 30), lo_len(180, 20)],
        );
        tracker.acknowledge_batch(
            file_id,
            &[lo_len(100, 50), lo_len(150, 30), lo_len(180, 20)],
        );

        // No pending - should return hwm.offset + hwm.len = 180 + 20 = 200
        assert_eq!(tracker.get_persistable_offset(file_id), Some(200));
    }

    #[test]
    fn test_get_persistable_offset_out_of_order_acks() {
        let mut tracker = FileOffsetTracker::new(false);
        let file_id = test_file_id(1, 100);

        // Track offsets with realistic lengths
        // Line A: offset=0, len=10 (ends at 10)
        // Line B: offset=10, len=20 (ends at 30)
        // Line C: offset=30, len=5 (ends at 35)
        tracker.track_batch(file_id, &[lo_len(0, 10), lo_len(10, 20), lo_len(30, 5)]);

        // Ack out of order: C first (offset 30)
        tracker.acknowledge(file_id, lo_len(30, 5));

        // Should still return lowest pending (0)
        assert_eq!(tracker.get_persistable_offset(file_id), Some(0));
        // hwm should be (30, 5)
        assert_eq!(tracker.get_high_water_mark(file_id), Some(lo_len(30, 5)));

        // Ack A (offset 0)
        tracker.acknowledge(file_id, lo_len(0, 10));

        // Should return 10 (B still pending)
        assert_eq!(tracker.get_persistable_offset(file_id), Some(10));
        // hwm unchanged (30 > 0)
        assert_eq!(tracker.get_high_water_mark(file_id), Some(lo_len(30, 5)));

        // Ack B (offset 10)
        tracker.acknowledge(file_id, lo_len(10, 20));

        // All acked - should return hwm.end_offset() = 30 + 5 = 35
        assert_eq!(tracker.get_persistable_offset(file_id), Some(35));
    }

    #[test]
    fn test_get_all_persistable_offsets() {
        let mut tracker = FileOffsetTracker::new(false);
        let file1 = test_file_id(1, 100);
        let file2 = test_file_id(1, 200);

        // Track offsets for both files
        tracker.track_batch(file1, &[lo_len(100, 50), lo_len(150, 50)]);
        tracker.track_batch(file2, &[lo_len(300, 30), lo_len(330, 40), lo_len(370, 30)]);

        // Ack all of file1, partial for file2
        tracker.acknowledge_batch(file1, &[lo_len(100, 50), lo_len(150, 50)]);
        tracker.acknowledge(file2, lo_len(300, 30));

        let persistable = tracker.get_all_persistable_offsets();

        // file1: all acked, should be hwm.end_offset() = 150 + 50 = 200
        assert_eq!(persistable.get(&file1), Some(&200));

        // file2: has pending [330, 370], should be lowest pending (330)
        assert_eq!(persistable.get(&file2), Some(&330));
    }

    #[test]
    fn test_persistable_offset_scenario_from_design() {
        // This test validates the exact scenario from the design discussion:
        // Line A: offset=0, len=10 (ends at 10)
        // Line B: offset=10, len=20 (ends at 30)
        // Line C: offset=30, len=5 (ends at 35)

        let mut tracker = FileOffsetTracker::new(false);
        let file_id = test_file_id(1, 100);

        let line_a = lo_len(0, 10);
        let line_b = lo_len(10, 20);
        let line_c = lo_len(30, 5);

        tracker.track_batch(file_id, &[line_a, line_b, line_c]);

        // Initial state: all pending
        assert_eq!(tracker.get_persistable_offset(file_id), Some(0));

        // C acks first
        tracker.acknowledge(file_id, line_c);
        assert_eq!(tracker.get_persistable_offset(file_id), Some(0)); // Still 0 (A pending)
        assert_eq!(tracker.get_high_water_mark(file_id), Some(line_c));

        // A acks
        tracker.acknowledge(file_id, line_a);
        assert_eq!(tracker.get_persistable_offset(file_id), Some(10)); // Now 10 (B pending)
        assert_eq!(tracker.get_high_water_mark(file_id), Some(line_c)); // hwm unchanged (30 > 0)

        // B acks
        tracker.acknowledge(file_id, line_b);
        assert_eq!(tracker.get_persistable_offset(file_id), Some(35)); // 30 + 5 = 35
        assert_eq!(tracker.get_high_water_mark(file_id), Some(line_c)); // hwm still line_c
    }
}

// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};

#[rustfmt::skip]
type PartitionMap = RwLock<HashMap<i32, Arc<Mutex<BTreeMap<i64, ()>>>>>;
/// BTreeMap-based implementation of OffsetTracker.
///
/// Uses a BTreeMap per partition to maintain sorted offsets,
/// allowing O(1) access to the minimum offset.
/// Uses per-partition locks to allow concurrent access to different partitions.
pub struct BTreeMapOffsetTracker {
    /// Maps partition to a mutex-protected sorted set of pending offsets
    /// Using () as value since we only need the keys
    partitions: PartitionMap,
    high_water_marks: HashMap<i32, i64>,
}

impl Default for BTreeMapOffsetTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl BTreeMapOffsetTracker {
    pub fn new() -> Self {
        Self {
            partitions: RwLock::new(HashMap::new()),
            high_water_marks: HashMap::new(), // This is only going to be updated by a single thread,
        }
    }

    fn track(&self, partition: i32, offset: i64) {
        // Get or create the partition's BTreeMap with read lock first
        let partition_lock = {
            let partitions = self.partitions.read().unwrap();
            if let Some(partition_map) = partitions.get(&partition) {
                partition_map.clone()
            } else {
                // Need to create new partition, upgrade to write lock
                drop(partitions);
                let mut partitions = self.partitions.write().unwrap();
                // Double-check in case another thread created it
                partitions
                    .entry(partition)
                    .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())))
                    .clone()
            }
        };

        // Now acquire the partition-specific lock and insert
        let mut offsets = partition_lock.lock().unwrap();
        offsets.insert(offset, ());
    }

    fn acknowledge(&mut self, partition: i32, offset: i64) {
        let hwm = self.high_water_marks.get(&partition);
        match hwm {
            None => {
                self.high_water_marks.insert(partition, offset);
            }
            Some(h) => {
                if *h < offset {
                    self.high_water_marks.insert(partition, offset);
                }
            }
        }
        // Get the partition's BTreeMap
        let partition_lock = {
            let partitions = self.partitions.read().unwrap();
            partitions.get(&partition).cloned()
        };

        if let Some(partition_lock) = partition_lock {
            let should_remove = {
                let mut offsets = partition_lock.lock().unwrap();
                offsets.remove(&offset);
                offsets.is_empty()
            };

            // Clean up empty partition entries to prevent memory leak
            if should_remove {
                let mut partitions = self.partitions.write().unwrap();
                // Double-check it's still empty (another thread might have added)
                if let Some(partition_map) = partitions.get(&partition) {
                    let offsets = partition_map.lock().unwrap();
                    if offsets.is_empty() {
                        drop(offsets);
                        partitions.remove(&partition);
                    }
                }
            }
        } else {
            tracing::warn!(
                partition = partition,
                offset = offset,
                "Received acknowledgment for unknown partition"
            );
        }
    }

    // Returns the highest acknowledged offset for a partition (high water mark)
    pub fn get_high_water_mark(&self, partition: i32) -> Option<i64> {
        self.high_water_marks.get(&partition).copied()
    }

    fn get_committable_offsets(&self) -> HashMap<i32, i64> {
        let partitions = self.partitions.read().unwrap();
        let mut committable = HashMap::new();

        for (partition, partition_lock) in partitions.iter() {
            let offsets = partition_lock.lock().unwrap();
            // Get the minimum offset for this partition
            if let Some((min_offset, _)) = offsets.first_key_value() {
                committable.insert(*partition, *min_offset);
            }
        }
        committable
    }

    fn lowest_pending_offset(&self, partition: i32) -> Option<i64> {
        let partitions = self.partitions.read().unwrap();
        partitions.get(&partition).and_then(|partition_lock| {
            let offsets = partition_lock.lock().unwrap();
            offsets.first_key_value().map(|(offset, _)| *offset)
        })
    }

    fn pending_count(&self, partition: i32) -> usize {
        let partitions = self.partitions.read().unwrap();
        partitions
            .get(&partition)
            .map(|partition_lock| {
                let offsets = partition_lock.lock().unwrap();
                offsets.len()
            })
            .unwrap_or(0)
    }

    fn total_pending(&self) -> usize {
        let partitions = self.partitions.read().unwrap();
        partitions
            .values()
            .map(|partition_lock| {
                let offsets = partition_lock.lock().unwrap();
                offsets.len()
            })
            .sum()
    }
}

/// Wrapper that manages per-topic offset trackers to reduce lock contention.
///
/// Instead of having one monolithic tracker for all topics and partitions,
/// this maintains separate trackers for each topic. This reduces lock contention
/// when processing messages from different topics in parallel.
pub struct TopicTrackers {
    /// Maps topic_id to its dedicated offset tracker
    trackers: RwLock<HashMap<u8, BTreeMapOffsetTracker>>,
}

impl TopicTrackers {
    /// Create a new TopicTrackers using BTreeMap implementation as default
    pub fn new() -> Self {
        Self {
            trackers: RwLock::new(HashMap::new()),
        }
    }

    /// Track an offset for a specific topic and partition
    pub fn track(&self, topic_id: u8, partition: i32, offset: i64) {
        // Try to get existing tracker with read lock first
        {
            let trackers = self.trackers.read().unwrap();
            if let Some(tracker) = trackers.get(&topic_id) {
                tracker.track(partition, offset);
                return;
            }
        }

        // Need to create a new tracker, acquire write lock
        let mut trackers = self.trackers.write().unwrap();
        // Double-check in case another thread created it
        let tracker = trackers.entry(topic_id).or_default();
        tracker.track(partition, offset);
    }

    /// Acknowledge an offset for a specific topic and partition
    pub fn acknowledge(&self, topic_id: u8, partition: i32, offset: i64) {
        let mut trackers = self.trackers.write().unwrap();
        if let Some(tracker) = trackers.get_mut(&topic_id) {
            tracker.acknowledge(partition, offset);
        } else {
            tracing::warn!(
                topic_id = topic_id,
                partition = partition,
                offset = offset,
                "Received acknowledgment for unknown topic"
            );
        }
    }

    /// Get committable offsets for a specific topic
    pub fn get_committable_offsets(&self, topic_id: u8) -> HashMap<i32, i64> {
        let trackers = self.trackers.read().unwrap();
        trackers
            .get(&topic_id)
            .map(|tracker| tracker.get_committable_offsets())
            .unwrap_or_default()
    }

    /// Get committable offsets for all topics
    pub fn get_all_committable_offsets(&self) -> HashMap<(u8, i32), i64> {
        let trackers = self.trackers.read().unwrap();
        let mut all_committable = HashMap::new();

        for (topic_id, tracker) in trackers.iter() {
            let topic_committable = tracker.get_committable_offsets();
            for (partition, offset) in topic_committable {
                all_committable.insert((*topic_id, partition), offset);
            }
        }

        all_committable
    }

    /// Get pending count for a specific topic and partition
    pub fn pending_count(&self, topic_id: u8, partition: i32) -> usize {
        let trackers = self.trackers.read().unwrap();
        trackers
            .get(&topic_id)
            .map(|tracker| tracker.pending_count(partition))
            .unwrap_or(0)
    }

    /// Get total pending count across all topics and partitions
    pub fn total_pending(&self) -> usize {
        let trackers = self.trackers.read().unwrap();
        trackers
            .values()
            .map(|tracker| tracker.total_pending())
            .sum()
    }

    /// Get total pending count for a specific topic
    pub fn topic_pending(&self, topic_id: u8) -> usize {
        let trackers = self.trackers.read().unwrap();
        trackers
            .get(&topic_id)
            .map(|tracker| tracker.total_pending())
            .unwrap_or(0)
    }

    /// Get the lowest pending offset for a specific topic and partition
    pub fn lowest_pending_offset(&self, topic_id: u8, partition: i32) -> Option<i64> {
        let trackers = self.trackers.read().unwrap();
        trackers
            .get(&topic_id)
            .and_then(|tracker| tracker.lowest_pending_offset(partition))
    }

    /// Get the high water mark for a specific topic and partition
    pub fn high_water_mark(&self, topic_id: u8, partition: i32) -> Option<i64> {
        let trackers = self.trackers.read().unwrap();
        trackers
            .get(&topic_id)
            .and_then(|tracker| tracker.get_high_water_mark(partition))
    }
}

impl Default for TopicTrackers {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_track_and_ack() {
        let mut tracker = BTreeMapOffsetTracker::new();

        // Track some offsets
        tracker.track(0, 100);
        tracker.track(0, 101);
        tracker.track(0, 102);

        // Verify pending count
        assert_eq!(tracker.pending_count(0), 3);

        // Get committable - should be lowest offset
        let committable = tracker.get_committable_offsets();
        assert_eq!(committable.get(&0), Some(&100));

        // Acknowledge first offset
        tracker.acknowledge(0, 100);
        assert_eq!(tracker.pending_count(0), 2);

        // Committable should now be 101
        let committable = tracker.get_committable_offsets();
        assert_eq!(committable.get(&0), Some(&101));
    }

    #[test]
    fn test_lowest_pending_offset() {
        let mut tracker = BTreeMapOffsetTracker::new();

        // Initially empty
        assert_eq!(tracker.lowest_pending_offset(0), None);

        // Track some offsets out of order
        tracker.track(0, 105);
        tracker.track(0, 102);
        tracker.track(0, 110);
        tracker.track(0, 100);

        // Should return the lowest offset
        assert_eq!(tracker.lowest_pending_offset(0), Some(100));

        // Acknowledge the lowest
        tracker.acknowledge(0, 100);
        assert_eq!(tracker.lowest_pending_offset(0), Some(102));

        // Acknowledge middle offset
        tracker.acknowledge(0, 105);
        assert_eq!(tracker.lowest_pending_offset(0), Some(102));

        // Acknowledge 102
        tracker.acknowledge(0, 102);
        assert_eq!(tracker.lowest_pending_offset(0), Some(110));

        // Acknowledge last offset
        tracker.acknowledge(0, 110);
        assert_eq!(tracker.lowest_pending_offset(0), None);

        // Test non-existent partition
        assert_eq!(tracker.lowest_pending_offset(99), None);
    }

    #[test]
    fn test_out_of_order_acks() {
        let mut tracker = BTreeMapOffsetTracker::new();

        // Track sequential offsets
        tracker.track(0, 100);
        tracker.track(0, 101);
        tracker.track(0, 102);
        tracker.track(0, 103);
        tracker.track(0, 104);

        // Ack out of order: 102, 104
        tracker.acknowledge(0, 102);
        tracker.acknowledge(0, 104);

        // Committable should still be 100 (lowest pending)
        let committable = tracker.get_committable_offsets();
        assert_eq!(committable.get(&0), Some(&100));

        // Ack 100
        tracker.acknowledge(0, 100);

        // Committable should now be 101
        let committable = tracker.get_committable_offsets();
        assert_eq!(committable.get(&0), Some(&101));

        // Ack 101
        tracker.acknowledge(0, 101);

        // Committable should now be 103 (102 was already acked)
        let committable = tracker.get_committable_offsets();
        assert_eq!(committable.get(&0), Some(&103));
    }

    #[test]
    fn test_multiple_partitions() {
        let mut tracker = BTreeMapOffsetTracker::new();

        // Track offsets for different partitions
        tracker.track(0, 100);
        tracker.track(0, 101);
        tracker.track(1, 200);
        tracker.track(1, 201);
        tracker.track(2, 300);

        // Verify counts
        assert_eq!(tracker.pending_count(0), 2);
        assert_eq!(tracker.pending_count(1), 2);
        assert_eq!(tracker.pending_count(2), 1);
        assert_eq!(tracker.total_pending(), 5);

        // Get committable for all partitions
        let committable = tracker.get_committable_offsets();
        assert_eq!(committable.get(&0), Some(&100));
        assert_eq!(committable.get(&1), Some(&200));
        assert_eq!(committable.get(&2), Some(&300));

        // Ack some offsets
        tracker.acknowledge(0, 100);
        tracker.acknowledge(1, 200);

        // Verify updated committable
        let committable = tracker.get_committable_offsets();
        assert_eq!(committable.get(&0), Some(&101));
        assert_eq!(committable.get(&1), Some(&201));
        assert_eq!(committable.get(&2), Some(&300));
    }

    #[test]
    fn test_empty_state() {
        let mut tracker = BTreeMapOffsetTracker::new();

        // Should return empty map when no offsets tracked
        let committable = tracker.get_committable_offsets();
        assert!(committable.is_empty());

        // Track and then ack all offsets
        tracker.track(0, 100);
        tracker.track(0, 101);
        tracker.acknowledge(0, 100);
        tracker.acknowledge(0, 101);

        // Should return empty map when all offsets acked
        let committable = tracker.get_committable_offsets();
        assert!(committable.is_empty());
        assert_eq!(tracker.pending_count(0), 0);
    }

    #[test]
    fn test_duplicate_tracking() {
        let mut tracker = BTreeMapOffsetTracker::new();

        // Track same offset multiple times (idempotent)
        tracker.track(0, 100);
        tracker.track(0, 100);
        tracker.track(0, 100);

        // Should only count once
        assert_eq!(tracker.pending_count(0), 1);

        // Single ack should clear it
        tracker.acknowledge(0, 100);
        assert_eq!(tracker.pending_count(0), 0);
    }

    #[test]
    fn test_ack_non_existent() {
        let mut tracker = BTreeMapOffsetTracker::new();

        // Acking non-existent offset should be safe no-op
        tracker.acknowledge(0, 999);

        // Track some offsets
        tracker.track(0, 100);

        // Ack wrong partition - should be no-op
        tracker.acknowledge(1, 100);
        assert_eq!(tracker.pending_count(0), 1);

        // Ack wrong offset - should be no-op
        tracker.acknowledge(0, 999);
        assert_eq!(tracker.pending_count(0), 1);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let tracker = Arc::new(Mutex::new(BTreeMapOffsetTracker::new()));
        let mut handles = vec![];

        // Use barrier to ensure all tracking is done before acking
        let barrier = Arc::new(Barrier::new(20)); // 10 track threads + 10 ack threads

        // Spawn threads that track offsets
        for i in 0..10 {
            let tracker_clone = tracker.clone();
            let barrier_clone = barrier.clone();
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    tracker_clone.lock().unwrap().track(i, j);
                }
                barrier_clone.wait(); // Wait for all threads to finish tracking
            });
            handles.push(handle);
        }

        // Spawn threads that acknowledge offsets - but wait for tracking to complete
        for i in 0..10 {
            let tracker_clone = tracker.clone();
            let barrier_clone = barrier.clone();
            let handle = thread::spawn(move || {
                barrier_clone.wait(); // Wait for all tracking to complete first
                for j in 0..50 {
                    tracker_clone.lock().unwrap().acknowledge(i, j);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        let tracker = tracker.lock().unwrap();

        // Each partition should have 50 pending offsets (100 tracked - 50 acked)
        for i in 0..10 {
            assert_eq!(tracker.pending_count(i as i32), 50);
        }

        // Total should be 500
        assert_eq!(tracker.total_pending(), 500);

        // Each partition's committable should be 50 (first unacked)
        let committable = tracker.get_committable_offsets();
        for i in 0..10 {
            assert_eq!(committable.get(&(i as i32)), Some(&50));
        }
    }

    #[test]
    fn test_btree_implementation() {
        // Basic track and ack
        let mut tracker = BTreeMapOffsetTracker::default();
        tracker.track(0, 100);
        tracker.track(0, 101);
        assert_eq!(tracker.pending_count(0), 2);
        assert_eq!(tracker.get_committable_offsets().get(&0), Some(&100));
        assert_eq!(tracker.lowest_pending_offset(0), Some(100));

        tracker.acknowledge(0, 100);
        assert_eq!(tracker.get_committable_offsets().get(&0), Some(&101));
        assert_eq!(tracker.lowest_pending_offset(0), Some(101));

        // Out of order acks
        let mut tracker = BTreeMapOffsetTracker::default();
        tracker.track(0, 100);
        tracker.track(0, 101);
        tracker.track(0, 102);

        tracker.acknowledge(0, 102);
        assert_eq!(tracker.get_committable_offsets().get(&0), Some(&100));
        assert_eq!(tracker.lowest_pending_offset(0), Some(100));

        tracker.acknowledge(0, 100);
        assert_eq!(tracker.get_committable_offsets().get(&0), Some(&101));
        assert_eq!(tracker.lowest_pending_offset(0), Some(101));

        // Test empty partition
        assert_eq!(tracker.lowest_pending_offset(99), None);
    }

    #[test]
    fn test_high_water_mark_after_complete_ack() {
        let mut tracker = BTreeMapOffsetTracker::new();

        // Track 5 offsets for partition 0
        tracker.track(0, 100);
        tracker.track(0, 101);
        tracker.track(0, 102);
        tracker.track(0, 103);
        tracker.track(0, 104);

        // Verify they are tracked
        assert_eq!(tracker.pending_count(0), 5);
        assert_eq!(tracker.lowest_pending_offset(0), Some(100));

        // Acknowledge all offsets
        tracker.acknowledge(0, 100);
        tracker.acknowledge(0, 101);
        tracker.acknowledge(0, 102);
        tracker.acknowledge(0, 103);
        tracker.acknowledge(0, 104);

        // Verify all are acknowledged - lowest pending should be None
        assert_eq!(tracker.lowest_pending_offset(0), None);
        assert_eq!(tracker.pending_count(0), 0);

        // Check high water mark - should be the highest acknowledged offset
        assert_eq!(tracker.get_high_water_mark(0), Some(104));
    }

    #[test]
    fn test_topic_trackers_basic() {
        let trackers = TopicTrackers::new();

        // Track offsets for multiple topics
        trackers.track(1, 0, 100);
        trackers.track(1, 0, 101);
        trackers.track(2, 0, 200);
        trackers.track(2, 1, 300);

        // Verify per-topic counts
        assert_eq!(trackers.topic_pending(1), 2);
        assert_eq!(trackers.topic_pending(2), 2);
        assert_eq!(trackers.total_pending(), 4);

        // Verify per-partition counts
        assert_eq!(trackers.pending_count(1, 0), 2);
        assert_eq!(trackers.pending_count(2, 0), 1);
        assert_eq!(trackers.pending_count(2, 1), 1);

        // Get committable offsets per topic
        let topic1_committable = trackers.get_committable_offsets(1);
        assert_eq!(topic1_committable.get(&0), Some(&100));

        let topic2_committable = trackers.get_committable_offsets(2);
        assert_eq!(topic2_committable.get(&0), Some(&200));
        assert_eq!(topic2_committable.get(&1), Some(&300));

        // Get all committable offsets
        let all_committable = trackers.get_all_committable_offsets();
        assert_eq!(all_committable.get(&(1, 0)), Some(&100));
        assert_eq!(all_committable.get(&(2, 0)), Some(&200));
        assert_eq!(all_committable.get(&(2, 1)), Some(&300));
    }

    #[test]
    fn test_topic_trackers_acknowledge() {
        let trackers = TopicTrackers::new();

        // Track some offsets
        trackers.track(1, 0, 100);
        trackers.track(1, 0, 101);
        trackers.track(2, 0, 200);

        // Acknowledge some offsets
        trackers.acknowledge(1, 0, 100);
        trackers.acknowledge(2, 0, 200);

        // Verify updated state
        assert_eq!(trackers.pending_count(1, 0), 1);
        assert_eq!(trackers.pending_count(2, 0), 0);
        assert_eq!(trackers.total_pending(), 1);

        let topic1_committable = trackers.get_committable_offsets(1);
        assert_eq!(topic1_committable.get(&0), Some(&101));

        let topic2_committable = trackers.get_committable_offsets(2);
        assert!(topic2_committable.is_empty());
    }

    #[test]
    fn test_topic_trackers_lowest_pending_offset() {
        let trackers = TopicTrackers::new();

        // Initially empty
        assert_eq!(trackers.lowest_pending_offset(1, 0), None);

        // Track some offsets
        trackers.track(1, 0, 105);
        trackers.track(1, 0, 100);
        trackers.track(1, 0, 102);
        trackers.track(2, 1, 200);

        // Should return lowest for each topic/partition
        assert_eq!(trackers.lowest_pending_offset(1, 0), Some(100));
        assert_eq!(trackers.lowest_pending_offset(2, 1), Some(200));
        assert_eq!(trackers.lowest_pending_offset(99, 0), None); // Unknown topic

        // Acknowledge lowest offset
        trackers.acknowledge(1, 0, 100);
        assert_eq!(trackers.lowest_pending_offset(1, 0), Some(102));
    }

    #[test]
    fn test_topic_trackers_unknown_topic() {
        let trackers = TopicTrackers::new();

        // Query unknown topic should return defaults
        assert_eq!(trackers.topic_pending(99), 0);
        assert_eq!(trackers.pending_count(99, 0), 0);
        assert!(trackers.get_committable_offsets(99).is_empty());

        // Acknowledge unknown topic should be safe no-op
        trackers.acknowledge(99, 0, 123);
    }

    #[test]
    fn test_topic_trackers_concurrent() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let trackers = Arc::new(TopicTrackers::new());
        let mut handles = vec![];
        let barrier = Arc::new(Barrier::new(10));

        // Spawn threads for different topics to verify reduced contention
        for topic_id in 0..10 {
            let trackers_clone = trackers.clone();
            let barrier_clone = barrier.clone();
            let handle = thread::spawn(move || {
                // Each topic works on its own partition 0
                for offset in 0..100 {
                    trackers_clone.track(topic_id as u8, 0, offset);
                }
                barrier_clone.wait();

                // Acknowledge half of them
                for offset in 0..50 {
                    trackers_clone.acknowledge(topic_id as u8, 0, offset);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final state
        assert_eq!(trackers.total_pending(), 500); // 10 topics * 50 pending each

        // Each topic should have 50 pending and committable offset 50
        for topic_id in 0..10 {
            assert_eq!(trackers.topic_pending(topic_id), 50);
            assert_eq!(trackers.pending_count(topic_id, 0), 50);
            let committable = trackers.get_committable_offsets(topic_id);
            assert_eq!(committable.get(&0), Some(&50));
        }

        // Verify all committable works
        let all_committable = trackers.get_all_committable_offsets();
        assert_eq!(all_committable.len(), 10);
        for topic_id in 0..10 {
            assert_eq!(all_committable.get(&(topic_id, 0)), Some(&50));
        }
    }
}

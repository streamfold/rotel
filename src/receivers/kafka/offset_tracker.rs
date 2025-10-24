// SPDX-License-Identifier: Apache-2.0

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::sync::RwLock;

/// Tracks Kafka offsets for at-least-once processing semantics.
///
/// Each tracker is dedicated to a single topic and maintains pending offsets per partition.
/// When an offset is acknowledged, it's removed from tracking.
/// The lowest pending offset for each partition represents the safe commit point.
pub trait OffsetTracker: Send + Sync {
    /// Track a new offset when a message is consumed from Kafka
    fn track(&self, partition: i32, offset: i64);

    /// Remove an offset when acknowledgment is received from downstream processors
    fn acknowledge(&self, partition: i32, offset: i64);

    /// Get the lowest pending offset for each partition.
    /// Returns the offset that should be committed to Kafka.
    /// If no offsets are pending for a partition, it won't be in the returned map.
    fn get_committable_offsets(&self) -> HashMap<i32, i64>;

    /// Get count of pending offsets for a specific partition (for monitoring)
    fn pending_count(&self, partition: i32) -> usize;

    /// Get total count of all pending offsets across all partitions
    fn total_pending(&self) -> usize;
}

/// BTreeMap-based implementation of OffsetTracker.
///
/// Uses a BTreeMap per partition to maintain sorted offsets,
/// allowing O(1) access to the minimum offset.
pub struct BTreeMapOffsetTracker {
    /// Maps partition to a sorted set of pending offsets
    /// Using () as value since we only need the keys
    pending: RwLock<HashMap<i32, BTreeMap<i64, ()>>>,
}

impl BTreeMapOffsetTracker {
    pub fn new() -> Self {
        Self {
            pending: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for BTreeMapOffsetTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl OffsetTracker for BTreeMapOffsetTracker {
    fn track(&self, partition: i32, offset: i64) {
        let mut pending = self.pending.write().unwrap();
        pending
            .entry(partition)
            .or_insert_with(BTreeMap::new)
            .insert(offset, ());
    }

    fn acknowledge(&self, partition: i32, offset: i64) {
        let mut pending = self.pending.write().unwrap();
        if let Some(partition_offsets) = pending.get_mut(&partition) {
            partition_offsets.remove(&offset);

            // Clean up empty partition entries to prevent memory leak
            if partition_offsets.is_empty() {
                pending.remove(&partition);
            }
        }
    }

    fn get_committable_offsets(&self) -> HashMap<i32, i64> {
        let pending = self.pending.read().unwrap();
        let mut committable = HashMap::new();

        for (partition, offsets) in pending.iter() {
            // Get the minimum offset for this partition
            if let Some((min_offset, _)) = offsets.first_key_value() {
                committable.insert(*partition, *min_offset);
            }
        }

        committable
    }

    fn pending_count(&self, partition: i32) -> usize {
        let pending = self.pending.read().unwrap();
        pending
            .get(&partition)
            .map(|offsets| offsets.len())
            .unwrap_or(0)
    }

    fn total_pending(&self) -> usize {
        let pending = self.pending.read().unwrap();
        pending.values().map(|offsets| offsets.len()).sum()
    }
}

/// MinHeap-based implementation of OffsetTracker.
///
/// Uses a BinaryHeap (min-heap) per partition to track offsets.
/// Additionally maintains a HashSet for O(1) membership checks.
pub struct MinHeapOffsetTracker {
    /// Maps partition to heap and set of pending offsets
    /// The heap gives us O(1) access to minimum, set gives O(1) membership check
    pending: RwLock<HashMap<i32, PartitionTracker>>,
}

struct PartitionTracker {
    /// Min-heap of offsets (using Reverse for min-heap behavior)
    heap: BinaryHeap<Reverse<i64>>,
    /// Set for O(1) membership check and removal tracking
    offsets: HashSet<i64>,
}

impl PartitionTracker {
    fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            offsets: HashSet::new(),
        }
    }

    fn track(&mut self, offset: i64) {
        if self.offsets.insert(offset) {
            self.heap.push(Reverse(offset));
        }
    }

    fn acknowledge(&mut self, offset: i64) {
        self.offsets.remove(&offset);
        // We don't remove from heap immediately - we handle it lazily in get_min()
    }

    fn get_min(&mut self) -> Option<i64> {
        // Clean the heap by removing acknowledged offsets from the top
        while let Some(Reverse(min_offset)) = self.heap.peek() {
            if self.offsets.contains(min_offset) {
                return Some(*min_offset);
            }
            // This offset was acknowledged, remove it from heap
            self.heap.pop();
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }
}

impl MinHeapOffsetTracker {
    pub fn new() -> Self {
        Self {
            pending: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MinHeapOffsetTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl OffsetTracker for MinHeapOffsetTracker {
    fn track(&self, partition: i32, offset: i64) {
        let mut pending = self.pending.write().unwrap();
        pending
            .entry(partition)
            .or_insert_with(PartitionTracker::new)
            .track(offset);
    }

    fn acknowledge(&self, partition: i32, offset: i64) {
        let mut pending = self.pending.write().unwrap();
        if let Some(partition_tracker) = pending.get_mut(&partition) {
            partition_tracker.acknowledge(offset);

            // Clean up empty partition entries
            if partition_tracker.is_empty() {
                pending.remove(&partition);
            }
        }
    }

    fn get_committable_offsets(&self) -> HashMap<i32, i64> {
        let mut pending = self.pending.write().unwrap();
        let mut committable = HashMap::new();

        for (partition, tracker) in pending.iter_mut() {
            if let Some(min_offset) = tracker.get_min() {
                committable.insert(*partition, min_offset);
            }
        }

        committable
    }

    fn pending_count(&self, partition: i32) -> usize {
        let pending = self.pending.read().unwrap();
        pending
            .get(&partition)
            .map(|tracker| tracker.len())
            .unwrap_or(0)
    }

    fn total_pending(&self) -> usize {
        let pending = self.pending.read().unwrap();
        pending.values().map(|tracker| tracker.len()).sum()
    }
}

/// Wrapper that manages per-topic offset trackers to reduce lock contention.
///
/// Instead of having one monolithic tracker for all topics and partitions,
/// this maintains separate trackers for each topic. This reduces lock contention
/// when processing messages from different topics in parallel.
pub struct TopicTrackers {
    /// Maps topic_id to its dedicated offset tracker
    trackers: RwLock<HashMap<u8, Box<dyn OffsetTracker>>>,
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
        let tracker = trackers
            .entry(topic_id)
            .or_insert_with(|| Box::new(BTreeMapOffsetTracker::new()));
        tracker.track(partition, offset);
    }

    /// Acknowledge an offset for a specific topic and partition
    pub fn acknowledge(&self, topic_id: u8, partition: i32, offset: i64) {
        let trackers = self.trackers.read().unwrap();
        if let Some(tracker) = trackers.get(&topic_id) {
            tracker.acknowledge(partition, offset);
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
        let tracker = BTreeMapOffsetTracker::new();

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
    fn test_out_of_order_acks() {
        let tracker = BTreeMapOffsetTracker::new();

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
        let tracker = BTreeMapOffsetTracker::new();

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
        let tracker = BTreeMapOffsetTracker::new();

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
        let tracker = BTreeMapOffsetTracker::new();

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
        let tracker = BTreeMapOffsetTracker::new();

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

        let tracker = Arc::new(BTreeMapOffsetTracker::new());
        let mut handles = vec![];

        // Use barrier to ensure all tracking is done before acking
        let barrier = Arc::new(Barrier::new(20)); // 10 track threads + 10 ack threads

        // Spawn threads that track offsets
        for i in 0..10 {
            let tracker_clone = tracker.clone();
            let barrier_clone = barrier.clone();
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    tracker_clone.track(i, j);
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
                    tracker_clone.acknowledge(i, j);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

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

    // Test both implementations with the same test cases
    fn run_tracker_tests<T: OffsetTracker + Default>() {
        // Basic track and ack
        let tracker = T::default();
        tracker.track(0, 100);
        tracker.track(0, 101);
        assert_eq!(tracker.pending_count(0), 2);
        assert_eq!(tracker.get_committable_offsets().get(&0), Some(&100));

        tracker.acknowledge(0, 100);
        assert_eq!(tracker.get_committable_offsets().get(&0), Some(&101));

        // Out of order acks
        let tracker = T::default();
        tracker.track(0, 100);
        tracker.track(0, 101);
        tracker.track(0, 102);

        tracker.acknowledge(0, 102);
        assert_eq!(tracker.get_committable_offsets().get(&0), Some(&100));

        tracker.acknowledge(0, 100);
        assert_eq!(tracker.get_committable_offsets().get(&0), Some(&101));
    }

    #[test]
    fn test_btree_implementation() {
        run_tracker_tests::<BTreeMapOffsetTracker>();
    }

    #[test]
    fn test_minheap_implementation() {
        run_tracker_tests::<MinHeapOffsetTracker>();
    }

    #[test]
    fn test_minheap_lazy_cleanup() {
        let tracker = MinHeapOffsetTracker::new();

        // Add many offsets
        for i in 0..1000 {
            tracker.track(0, i);
        }

        // Acknowledge most of them out of order
        for i in (0..950).rev() {
            tracker.acknowledge(0, i);
        }

        // The min should still be found efficiently despite heap cleanup
        let committable = tracker.get_committable_offsets();
        assert_eq!(committable.get(&0), Some(&950));
        assert_eq!(tracker.pending_count(0), 50);
    }

    #[test]
    fn test_minheap_edge_cases() {
        let tracker = MinHeapOffsetTracker::new();

        // Acknowledge non-existent offset
        tracker.acknowledge(0, 999);
        assert_eq!(tracker.pending_count(0), 0);

        // Track, ack all, then get committable
        tracker.track(0, 100);
        tracker.acknowledge(0, 100);
        assert!(tracker.get_committable_offsets().is_empty());

        // Duplicate tracking should be idempotent
        tracker.track(0, 200);
        tracker.track(0, 200);
        assert_eq!(tracker.pending_count(0), 1);
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

#[cfg(test)]
mod benches {
    use super::*;
    use std::time::Instant;

    /// Simple benchmark helper
    fn benchmark<F>(name: &str, f: F) -> std::time::Duration
    where
        F: FnOnce(),
    {
        let start = Instant::now();
        f();
        let duration = start.elapsed();
        println!("{}: {:?}", name, duration);
        duration
    }

    #[test]
    #[ignore] // Run with `cargo test bench_ -- --ignored`
    fn bench_track_performance() {
        const N: i64 = 10_000;

        let btree_time = benchmark("BTreeMap track 10k sequential", || {
            let tracker = BTreeMapOffsetTracker::new();
            for i in 0..N {
                tracker.track(0, i);
            }
        });

        let heap_time = benchmark("MinHeap track 10k sequential", || {
            let tracker = MinHeapOffsetTracker::new();
            for i in 0..N {
                tracker.track(0, i);
            }
        });

        println!(
            "BTreeMap vs MinHeap track ratio: {:.2}",
            btree_time.as_nanos() as f64 / heap_time.as_nanos() as f64
        );
    }

    #[test]
    #[ignore]
    fn bench_ack_performance() {
        const N: i64 = 10_000;

        let btree_time = benchmark("BTreeMap ack 10k sequential", || {
            let tracker = BTreeMapOffsetTracker::new();
            for i in 0..N {
                tracker.track(0, i);
            }
            for i in 0..N {
                tracker.acknowledge(0, i);
            }
        });

        let heap_time = benchmark("MinHeap ack 10k sequential", || {
            let tracker = MinHeapOffsetTracker::new();
            for i in 0..N {
                tracker.track(0, i);
            }
            for i in 0..N {
                tracker.acknowledge(0, i);
            }
        });

        println!(
            "BTreeMap vs MinHeap ack ratio: {:.2}",
            btree_time.as_nanos() as f64 / heap_time.as_nanos() as f64
        );
    }

    #[test]
    #[ignore]
    fn bench_get_committable_performance() {
        const N: i64 = 10_000;
        const QUERIES: usize = 1_000;

        let btree_time = benchmark("BTreeMap get_committable 1k queries", || {
            let tracker = BTreeMapOffsetTracker::new();
            for i in 0..N {
                tracker.track(0, i);
            }
            for _ in 0..QUERIES {
                let _ = tracker.get_committable_offsets();
            }
        });

        let heap_time = benchmark("MinHeap get_committable 1k queries", || {
            let tracker = MinHeapOffsetTracker::new();
            for i in 0..N {
                tracker.track(0, i);
            }
            for _ in 0..QUERIES {
                let _ = tracker.get_committable_offsets();
            }
        });

        println!(
            "BTreeMap vs MinHeap get_committable ratio: {:.2}",
            btree_time.as_nanos() as f64 / heap_time.as_nanos() as f64
        );
    }

    #[test]
    #[ignore]
    fn bench_mixed_workload() {
        let btree_time = benchmark("BTreeMap mixed workload", || {
            let tracker = BTreeMapOffsetTracker::new();

            // Mixed pattern: track in batches, ack some out of order
            for batch in 0..10 {
                let start = batch * 1000;
                // Track 1000 offsets
                for i in start..start + 1000 {
                    tracker.track(0, i);
                }
                // Ack 800 of them out of order
                for i in (start + 200..start + 1000).step_by(2) {
                    tracker.acknowledge(0, i);
                }
                // Check committable a few times
                for _ in 0..10 {
                    let _ = tracker.get_committable_offsets();
                }
            }
        });

        let heap_time = benchmark("MinHeap mixed workload", || {
            let tracker = MinHeapOffsetTracker::new();

            // Same mixed pattern
            for batch in 0..10 {
                let start = batch * 1000;
                for i in start..start + 1000 {
                    tracker.track(0, i);
                }
                for i in (start + 200..start + 1000).step_by(2) {
                    tracker.acknowledge(0, i);
                }
                for _ in 0..10 {
                    let _ = tracker.get_committable_offsets();
                }
            }
        });

        println!(
            "BTreeMap vs MinHeap mixed workload ratio: {:.2}",
            btree_time.as_nanos() as f64 / heap_time.as_nanos() as f64
        );
    }
}

// SPDX-License-Identifier: Apache-2.0

use crate::topology::payload::Message;
use std::fmt;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone)]
pub struct BatchConfig {
    pub max_size: usize,
    pub timeout: Duration,
    pub disabled: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: 8192,
            timeout: Duration::from_millis(200),
            disabled: false,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TooManyItemsError;

impl fmt::Display for TooManyItemsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "too many items to batch")
    }
}

pub(crate) struct NestedBatch<T: BatchSizer + BatchSplittable> {
    items: Vec<T>,
    item_count: usize, // We need this because there are nested items in items.
    max_size: usize,
    last_flush: Instant,
    batch_timeout: Duration,
    disabled: bool,
}

impl<T: BatchSizer + BatchSplittable> NestedBatch<T>
// where
//     OTLPPayload: From<Vec<T>>,
{
    /// Creates a new NestedBatch. NestedBatches are vectors of type T but contain
    /// additional items per instance of T in the vector. We batch based on the total number
    /// of nested items in Vec<T>, rather than Vec<T>.len().
    ///
    /// # Arguments
    /// - `max_size`: The maximum size that will be sent to the batch handler per flush cycle
    /// - `batch_timeout`: How long to wait before flushing if we've not reached max_size
    pub(crate) fn new(max_size: usize, batch_timeout: Duration, disabled: bool) -> NestedBatch<T> {
        Self {
            items: Vec::with_capacity(max_size),
            item_count: 0,
            max_size,
            last_flush: Instant::now(),
            batch_timeout,
            disabled,
        }
    }

    pub(crate) fn get_timeout(&self) -> Duration {
        self.batch_timeout
    }

    pub(crate) fn take_batch(&mut self) -> Vec<T> {
        let items = std::mem::take(&mut self.items);
        self.item_count = 0;
        self.last_flush = Instant::now();
        items
    }

    pub(crate) fn should_flush(&self, now: Instant) -> bool {
        self.item_count > 0 && (self.last_flush + self.batch_timeout) < now
    }

    pub(crate) fn offer(
        &mut self,
        mut new_items: Vec<T>,
    ) -> Result<Option<Vec<T>>, TooManyItemsError> {
        if self.disabled {
            return Ok(Some(new_items));
        }

        let new_items_count = new_items.iter().map(|n| n.size_of()).sum::<usize>();
        if self.item_count + new_items_count < self.max_size {
            self.items.append(&mut new_items);
            self.item_count += new_items_count;
            return Ok(None);
        }

        // We can't just use all the new_items, as we'd overflow the batch. So we need to carefully
        // take enough from new_items to fill our current batch and push the remaining self.items.
        while !new_items.is_empty() && self.item_count < self.max_size {
            let s = new_items[0].size_of();
            if s < self.max_size - self.item_count {
                self.items.push(new_items.remove(0));
                self.item_count += s;
            } else {
                // We'll need to split this T
                let res = new_items[0].split(self.max_size - self.item_count);
                let split_size = res.size_of();
                self.items.push(res);
                self.item_count += split_size;
            }
        }
        let harvested = Some(self.take_batch());
        // Final error condition we need to make sure remaining isn't larger than max_size;
        let final_new_item_count = new_items.iter().map(|n| n.size_of()).sum::<usize>();
        if final_new_item_count > self.max_size {
            return Err(TooManyItemsError);
        }
        self.item_count += final_new_item_count;
        self.items.append(&mut new_items);
        Ok(harvested)
    }
}

pub trait BatchSizer {
    fn size_of(&self) -> usize;
}

pub trait BatchSplittable {
    fn split(&mut self, split_n: usize) -> Self
    where
        Self: Sized;
}

// Implement BatchSizer for Message<T> where T implements BatchSizer
impl<T: BatchSizer> BatchSizer for Message<T> {
    fn size_of(&self) -> usize {
        self.payload.iter().map(|item| item.size_of()).sum()
    }
}

// Implement BatchSizer for slice of Message<T>
impl<T: BatchSizer> BatchSizer for [Message<T>] {
    fn size_of(&self) -> usize {
        self.iter().map(|msg| msg.size_of()).sum()
    }
}

// Implement BatchSplittable for Message<T> where T implements BatchSplittable
impl<T> BatchSplittable for Message<T>
where
    T: BatchSplittable + BatchSizer,
{
    fn split(&mut self, split_n: usize) -> Self {
        let mut split_payload = Vec::new();
        let mut count_moved = 0;

        while !self.payload.is_empty() && count_moved < split_n {
            let item_size = self.payload[0].size_of();
            if item_size + count_moved <= split_n {
                // Move the entire item
                split_payload.push(self.payload.remove(0));
                count_moved += item_size;
            } else if count_moved < split_n {
                // Need to split this item
                let remaining_size = split_n - count_moved;
                let split_item = self.payload[0].split(remaining_size);
                split_payload.push(split_item);
                count_moved = split_n; // We've reached our target
            }
        }

        // Clone metadata for both parts - future reference counting will handle proper ack semantics
        Message {
            metadata: self.metadata.clone(),
            payload: split_payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::payload::{KafkaMetadata, Message, MessageMetadata};
    use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
    use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
    use utilities::otlp::FakeOTLP;

    #[test]
    fn test_trace_batch_splitting() {
        let mut batch = NestedBatch::<ResourceSpans>::new(10, Duration::from_secs(1), false);
        let first_request = FakeOTLP::trace_service_request_with_spans(1, 5);
        let resp = batch.offer(first_request.resource_spans);
        assert!(resp.is_ok());
        assert_eq!(resp.unwrap(), None);
        let second_request = FakeOTLP::trace_service_request_with_spans(1, 7);
        let resp = batch.offer(second_request.resource_spans);
        assert!(resp.is_ok());
        let spans = resp.unwrap().unwrap();
        assert_eq!(10, spans.size_of());
        for rs in spans {
            assert!(rs.resource.is_some())
        }
        // Grab what's left in the batch
        let leftover = batch.take_batch();
        assert_eq!(2, leftover.size_of());
    }

    #[test]
    fn test_trace_batch_split_too_large() {
        let mut batch = NestedBatch::<ResourceSpans>::new(10, Duration::from_secs(1), false);
        let first_request = FakeOTLP::trace_service_request_with_spans(1, 21);
        let resp = batch.offer(first_request.resource_spans);
        assert!(resp.is_err());
    }

    #[test]
    fn test_metrics_batch_splitting() {
        let mut batch = NestedBatch::<ResourceMetrics>::new(10, Duration::from_secs(1), false);
        let first_request = FakeOTLP::metrics_service_request_with_metrics(1, 5);
        let resp = batch.offer(first_request.resource_metrics);
        assert!(resp.is_ok());
        assert_eq!(resp.unwrap(), None);
        let second_request = FakeOTLP::metrics_service_request_with_metrics(1, 7);
        let resp = batch.offer(second_request.resource_metrics);
        assert!(resp.is_ok());
        let metrics = resp.unwrap().unwrap();
        assert_eq!(10, metrics.size_of());
        for rm in metrics {
            assert!(rm.resource.is_some())
        }
        // Grab what's left in the batch
        let leftover = batch.take_batch();
        assert_eq!(2, leftover.size_of());
    }

    #[test]
    fn test_metrics_batch_split_too_large() {
        let mut batch = NestedBatch::<ResourceMetrics>::new(10, Duration::from_secs(1), false);
        let first_request = FakeOTLP::metrics_service_request_with_metrics(1, 21);
        let resp = batch.offer(first_request.resource_metrics);
        assert!(resp.is_err());
    }

    #[test]
    fn test_logs_batch_splitting() {
        let mut batch = NestedBatch::<ResourceLogs>::new(10, Duration::from_secs(1), false);
        let first_request = FakeOTLP::logs_service_request_with_logs(1, 5);
        let resp = batch.offer(first_request.resource_logs);
        assert!(resp.is_ok());
        assert_eq!(resp.unwrap(), None);
        let second_request = FakeOTLP::logs_service_request_with_logs(1, 7);
        let resp = batch.offer(second_request.resource_logs);
        assert!(resp.is_ok());
        let logs = resp.unwrap().unwrap();
        assert_eq!(10, logs.size_of());
        for rl in logs {
            assert!(rl.resource.is_some())
        }
        // Grab what's left in the batch
        let leftover = batch.take_batch();
        assert_eq!(2, leftover.size_of());
    }

    #[test]
    fn test_logs_batch_split_too_large() {
        let mut batch = NestedBatch::<ResourceLogs>::new(10, Duration::from_secs(1), false);
        let first_request = FakeOTLP::logs_service_request_with_logs(1, 21);
        let resp = batch.offer(first_request.resource_logs);
        assert!(resp.is_err());
    }

    #[test]
    fn test_disabled_batching() {
        // Enabled, it should not return Some
        let mut batch = NestedBatch::<ResourceMetrics>::new(10, Duration::from_secs(1), false);
        let first_request = FakeOTLP::metrics_service_request_with_metrics(1, 5);
        let resp = batch.offer(first_request.resource_metrics);
        assert!(resp.is_ok());
        assert!(resp.unwrap().is_none());

        // When disabled, should immediately return
        let mut batch = NestedBatch::<ResourceMetrics>::new(10, Duration::from_secs(1), true);
        let first_request = FakeOTLP::metrics_service_request_with_metrics(1, 5);
        let resp = batch.offer(first_request.resource_metrics);
        assert!(resp.is_ok());
        let resp = resp.unwrap();
        assert!(resp.is_some());
        assert_eq!(resp.unwrap().size_of(), 5);
    }

    #[test]
    fn test_message_batch_without_kafka_metadata() {
        let mut batch =
            NestedBatch::<Message<ResourceSpans>>::new(10, Duration::from_secs(1), false);

        // Create a message without metadata
        let first_request = FakeOTLP::trace_service_request_with_spans(1, 5);
        let message = Message {
            metadata: None,
            payload: first_request.resource_spans,
        };

        let resp = batch.offer(vec![message]);
        assert!(resp.is_ok());
        assert_eq!(resp.unwrap(), None);

        // Add more that causes a split
        let second_request = FakeOTLP::trace_service_request_with_spans(1, 7);
        let message2 = Message {
            metadata: None,
            payload: second_request.resource_spans,
        };

        let resp = batch.offer(vec![message2]);
        assert!(resp.is_ok());
        let messages = resp.unwrap().unwrap();
        assert_eq!(messages.size_of(), 10);

        // Grab what's left in the batch
        let leftover = batch.take_batch();
        assert_eq!(leftover.size_of(), 2);
    }

    #[test]
    fn test_message_batch_with_kafka_metadata_no_split() {
        let mut batch =
            NestedBatch::<Message<ResourceSpans>>::new(10, Duration::from_secs(1), false);

        // Create a message with Kafka metadata that fits entirely in batch
        let first_request = FakeOTLP::trace_service_request_with_spans(1, 5);
        let kafka_metadata = KafkaMetadata {
            offset: 100,
            partition: 0,
            topic_id: 1,
            ack_chan: None,
        };
        let message = Message {
            metadata: Some(MessageMetadata::kafka(kafka_metadata.clone())),
            payload: first_request.resource_spans,
        };

        let resp = batch.offer(vec![message]);
        assert!(resp.is_ok());
        assert_eq!(resp.unwrap(), None);

        // Add another message that fits without splitting
        let second_request = FakeOTLP::trace_service_request_with_spans(1, 3);
        let kafka_metadata2 = KafkaMetadata {
            offset: 101,
            partition: 0,
            topic_id: 1,
            ack_chan: None,
        };
        let message2 = Message {
            metadata: Some(MessageMetadata::kafka(kafka_metadata2)),
            payload: second_request.resource_spans,
        };

        let resp = batch.offer(vec![message2]);
        assert!(resp.is_ok());
        assert_eq!(resp.unwrap(), None);

        // Both messages should be in the batch with their metadata intact
        let batch_content = batch.take_batch();
        assert_eq!(batch_content.len(), 2);
        assert_eq!(batch_content.size_of(), 8);

        // Verify metadata is preserved
        if let Some(metadata) = &batch_content[0].metadata {
            if let Some(meta) = metadata.as_kafka() {
                assert_eq!(meta.offset, 100);
            } else {
                panic!("Expected Kafka metadata in first message");
            }
        }

        if let Some(metadata) = &batch_content[1].metadata {
            if let Some(meta) = metadata.as_kafka() {
                assert_eq!(meta.offset, 101);
            } else {
                panic!("Expected Kafka metadata in second message");
            }
        }
    }

    #[test]
    fn test_message_batch_with_kafka_metadata_split() {
        let mut batch =
            NestedBatch::<Message<ResourceSpans>>::new(10, Duration::from_secs(1), false);

        // Create first message with Kafka metadata
        let first_request = FakeOTLP::trace_service_request_with_spans(1, 5);
        let kafka_metadata = KafkaMetadata {
            offset: 100,
            partition: 0,
            topic_id: 1,
            ack_chan: None,
        };
        let message = Message {
            metadata: Some(MessageMetadata::kafka(kafka_metadata.clone())),
            payload: first_request.resource_spans,
        };

        let resp = batch.offer(vec![message]);
        assert!(resp.is_ok());
        assert_eq!(resp.unwrap(), None);

        // Add a message that needs to be split
        let second_request = FakeOTLP::trace_service_request_with_spans(1, 7);
        let kafka_metadata2 = KafkaMetadata {
            offset: 101,
            partition: 0,
            topic_id: 1,
            ack_chan: None,
        };
        let message2 = Message {
            metadata: Some(MessageMetadata::kafka(kafka_metadata2.clone())),
            payload: second_request.resource_spans,
        };

        let resp = batch.offer(vec![message2]);
        assert!(resp.is_ok());
        let flushed_batch = resp.unwrap().unwrap();

        // The flushed batch should have 10 items
        assert_eq!(flushed_batch.size_of(), 10);
        // It should have 2 messages: the first complete one and part of the second
        assert_eq!(flushed_batch.len(), 2);

        // First message should have its original metadata
        if let Some(metadata) = &flushed_batch[0].metadata {
            if let Some(meta) = metadata.as_kafka() {
                assert_eq!(meta.offset, 100);
            } else {
                panic!("Expected Kafka metadata in first message");
            }
        }

        // Second message in flushed batch should ALSO have Kafka metadata (cloned for both parts)
        if let Some(metadata) = &flushed_batch[1].metadata {
            if let Some(meta) = metadata.as_kafka() {
                assert_eq!(meta.offset, 101);
                assert_eq!(meta.partition, 0);
                assert_eq!(meta.topic_id, 1);
            } else {
                panic!("Expected Kafka metadata in split message first part");
            }
        }

        // Get remaining batch
        let remaining = batch.take_batch();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining.size_of(), 2);

        // The remaining part should also have the Kafka metadata (cloned)
        if let Some(metadata) = &remaining[0].metadata {
            if let Some(meta) = metadata.as_kafka() {
                assert_eq!(meta.offset, 101);
                assert_eq!(meta.partition, 0);
                assert_eq!(meta.topic_id, 1);
            } else {
                panic!("Expected Kafka metadata in remaining message");
            }
        }
    }

    #[test]
    fn test_message_batch_multiple_splits_kafka_metadata() {
        let mut batch =
            NestedBatch::<Message<ResourceSpans>>::new(10, Duration::from_secs(1), false);

        // Add a message with 15 spans that will need multiple splits
        let request = FakeOTLP::trace_service_request_with_spans(1, 15);
        let kafka_metadata = KafkaMetadata {
            offset: 200,
            partition: 1,
            topic_id: 2,
            ack_chan: None,
        };
        let message = Message {
            metadata: Some(MessageMetadata::kafka(kafka_metadata.clone())),
            payload: request.resource_spans,
        };

        let resp = batch.offer(vec![message]);
        assert!(resp.is_ok());
        let flushed = resp.unwrap().unwrap();

        // First batch should have 10 items in one message WITH metadata (cloned)
        assert_eq!(flushed.size_of(), 10);
        assert_eq!(flushed.len(), 1);
        if let Some(metadata) = &flushed[0].metadata {
            if let Some(meta) = metadata.as_kafka() {
                assert_eq!(meta.offset, 200);
                assert_eq!(meta.partition, 1);
                assert_eq!(meta.topic_id, 2);
            } else {
                panic!("Expected Kafka metadata in first split");
            }
        }

        // Remaining should have 5 items with the same metadata (cloned)
        let remaining = batch.take_batch();
        assert_eq!(remaining.size_of(), 5);
        assert_eq!(remaining.len(), 1);
        if let Some(metadata) = &remaining[0].metadata {
            if let Some(meta) = metadata.as_kafka() {
                assert_eq!(meta.offset, 200);
                assert_eq!(meta.partition, 1);
                assert_eq!(meta.topic_id, 2);
            } else {
                panic!("Expected Kafka metadata in remaining message");
            }
        }
    }

    #[test]
    fn test_message_batch_split_too_large() {
        let mut batch =
            NestedBatch::<Message<ResourceSpans>>::new(10, Duration::from_secs(1), false);

        // Create a message with 21 spans (too large for max_size of 10)
        let request = FakeOTLP::trace_service_request_with_spans(1, 21);
        let message = Message {
            metadata: None,
            payload: request.resource_spans,
        };

        let resp = batch.offer(vec![message]);
        assert!(
            resp.is_err(),
            "Should error when single message is too large"
        );
    }

    #[test]
    fn test_message_batch_split_too_large_with_kafka_metadata() {
        let mut batch =
            NestedBatch::<Message<ResourceSpans>>::new(10, Duration::from_secs(1), false);

        // Create a message with 21 spans (too large) with Kafka metadata
        let request = FakeOTLP::trace_service_request_with_spans(1, 21);
        let kafka_metadata = KafkaMetadata {
            offset: 500,
            partition: 0,
            topic_id: 1,
            ack_chan: None,
        };
        let message = Message {
            metadata: Some(MessageMetadata::kafka(kafka_metadata)),
            payload: request.resource_spans,
        };

        let resp = batch.offer(vec![message]);
        assert!(
            resp.is_err(),
            "Should error when single message with Kafka metadata is too large"
        );
    }

    #[test]
    fn test_message_batch_disabled_batching() {
        // When enabled, should batch normally
        let mut batch =
            NestedBatch::<Message<ResourceSpans>>::new(10, Duration::from_secs(1), false);
        let request = FakeOTLP::trace_service_request_with_spans(1, 5);
        let message = Message {
            metadata: None,
            payload: request.resource_spans,
        };
        let resp = batch.offer(vec![message]);
        assert!(resp.is_ok());
        assert!(
            resp.unwrap().is_none(),
            "Enabled batching should not immediately return"
        );

        // When disabled, should immediately return
        let mut batch =
            NestedBatch::<Message<ResourceSpans>>::new(10, Duration::from_secs(1), true);
        let request = FakeOTLP::trace_service_request_with_spans(1, 5);
        let message = Message {
            metadata: None,
            payload: request.resource_spans,
        };
        let resp = batch.offer(vec![message]);
        assert!(resp.is_ok());
        let returned = resp.unwrap();
        assert!(
            returned.is_some(),
            "Disabled batching should immediately return"
        );
        assert_eq!(returned.unwrap().size_of(), 5);
    }

    #[test]
    fn test_message_batch_disabled_batching_with_kafka_metadata() {
        // When disabled with Kafka metadata, should still immediately return with metadata intact
        let mut batch =
            NestedBatch::<Message<ResourceSpans>>::new(10, Duration::from_secs(1), true);
        let request = FakeOTLP::trace_service_request_with_spans(1, 5);
        let kafka_metadata = KafkaMetadata {
            offset: 600,
            partition: 1,
            topic_id: 2,
            ack_chan: None,
        };
        let message = Message {
            metadata: Some(MessageMetadata::kafka(kafka_metadata)),
            payload: request.resource_spans,
        };

        let resp = batch.offer(vec![message]);
        assert!(resp.is_ok());
        let returned = resp.unwrap();
        assert!(
            returned.is_some(),
            "Disabled batching should immediately return"
        );

        let messages = returned.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages.size_of(), 5);

        // Verify metadata is preserved when batching is disabled
        if let Some(metadata) = &messages[0].metadata {
            if let Some(meta) = metadata.as_kafka() {
                assert_eq!(meta.offset, 600);
                assert_eq!(meta.partition, 1);
                assert_eq!(meta.topic_id, 2);
            } else {
                panic!("Expected Kafka metadata to be preserved");
            }
        }
    }

    #[test]
    fn test_message_batch_metrics_splitting() {
        let mut batch =
            NestedBatch::<Message<ResourceMetrics>>::new(10, Duration::from_secs(1), false);

        // First message with 5 metrics
        let first_request = FakeOTLP::metrics_service_request_with_metrics(1, 5);
        let message1 = Message {
            metadata: None,
            payload: first_request.resource_metrics,
        };

        let resp = batch.offer(vec![message1]);
        assert!(resp.is_ok());
        assert_eq!(resp.unwrap(), None);

        // Second message with 7 metrics that causes split
        let second_request = FakeOTLP::metrics_service_request_with_metrics(1, 7);
        let kafka_metadata = KafkaMetadata {
            offset: 700,
            partition: 0,
            topic_id: 3,
            ack_chan: None,
        };
        let message2 = Message {
            metadata: Some(MessageMetadata::kafka(kafka_metadata)),
            payload: second_request.resource_metrics,
        };

        let resp = batch.offer(vec![message2]);
        assert!(resp.is_ok());
        let flushed = resp.unwrap().unwrap();

        assert_eq!(flushed.size_of(), 10);
        // First message should have no metadata (it wasn't given any)
        assert!(flushed[0].metadata.is_none());
        // Second message (split part) should have Kafka metadata (cloned)
        if flushed.len() > 1 {
            if let Some(metadata) = &flushed[1].metadata {
                if let Some(meta) = metadata.as_kafka() {
                    assert_eq!(meta.offset, 700);
                    assert_eq!(meta.partition, 0);
                    assert_eq!(meta.topic_id, 3);
                } else {
                    panic!("Expected Kafka metadata in split part");
                }
            }
        }

        // Remaining should also have the Kafka metadata (cloned)
        let leftover = batch.take_batch();
        assert_eq!(leftover.size_of(), 2);
        if let Some(metadata) = &leftover[0].metadata {
            if let Some(meta) = metadata.as_kafka() {
                assert_eq!(meta.offset, 700);
            } else {
                panic!("Expected Kafka metadata in remaining metrics");
            }
        }
    }

    #[test]
    fn test_message_batch_logs_splitting() {
        let mut batch =
            NestedBatch::<Message<ResourceLogs>>::new(10, Duration::from_secs(1), false);

        // First message with 5 logs
        let first_request = FakeOTLP::logs_service_request_with_logs(1, 5);
        let message1 = Message {
            metadata: None,
            payload: first_request.resource_logs,
        };

        let resp = batch.offer(vec![message1]);
        assert!(resp.is_ok());
        assert_eq!(resp.unwrap(), None);

        // Second message with 7 logs that causes split
        let second_request = FakeOTLP::logs_service_request_with_logs(1, 7);
        let kafka_metadata = KafkaMetadata {
            offset: 800,
            partition: 2,
            topic_id: 4,
            ack_chan: None,
        };
        let message2 = Message {
            metadata: Some(MessageMetadata::kafka(kafka_metadata)),
            payload: second_request.resource_logs,
        };

        let resp = batch.offer(vec![message2]);
        assert!(resp.is_ok());
        let flushed = resp.unwrap().unwrap();

        assert_eq!(flushed.size_of(), 10);
        // Split part should have Kafka metadata (cloned)
        if flushed.len() > 1 {
            if let Some(metadata) = &flushed[1].metadata {
                if let Some(meta) = metadata.as_kafka() {
                    assert_eq!(meta.offset, 800);
                    assert_eq!(meta.partition, 2);
                    assert_eq!(meta.topic_id, 4);
                } else {
                    panic!("Expected Kafka metadata in split part");
                }
            }
        }

        // Remaining should also have the Kafka metadata (cloned)
        let leftover = batch.take_batch();
        assert_eq!(leftover.size_of(), 2);
        if let Some(metadata) = &leftover[0].metadata {
            if let Some(meta) = metadata.as_kafka() {
                assert_eq!(meta.offset, 800);
                assert_eq!(meta.partition, 2);
                assert_eq!(meta.topic_id, 4);
            } else {
                panic!("Expected Kafka metadata in remaining logs");
            }
        }
    }

    #[test]
    fn test_message_batch_mixed_metadata_types() {
        let mut batch =
            NestedBatch::<Message<ResourceSpans>>::new(10, Duration::from_secs(1), false);

        // Message with Kafka metadata
        let request1 = FakeOTLP::trace_service_request_with_spans(1, 3);
        let kafka_metadata = KafkaMetadata {
            offset: 300,
            partition: 2,
            topic_id: 3,
            ack_chan: None,
        };
        let message1 = Message {
            metadata: Some(MessageMetadata::kafka(kafka_metadata)),
            payload: request1.resource_spans,
        };

        // Message without metadata
        let request2 = FakeOTLP::trace_service_request_with_spans(1, 4);
        let message2 = Message {
            metadata: None,
            payload: request2.resource_spans,
        };

        // Another message with Kafka metadata that will cause a split
        let request3 = FakeOTLP::trace_service_request_with_spans(1, 5);
        let kafka_metadata2 = KafkaMetadata {
            offset: 301,
            partition: 2,
            topic_id: 3,
            ack_chan: None,
        };
        let message3 = Message {
            metadata: Some(MessageMetadata::kafka(kafka_metadata2)),
            payload: request3.resource_spans,
        };

        // Add first two messages
        let resp = batch.offer(vec![message1, message2]);
        assert!(resp.is_ok());
        assert!(resp.unwrap().is_none());

        // Add third message that causes split
        let resp = batch.offer(vec![message3]);
        assert!(resp.is_ok());
        let flushed = resp.unwrap().unwrap();

        // Flushed batch should have 10 items in 3 messages
        assert_eq!(flushed.size_of(), 10);
        assert_eq!(flushed.len(), 3);

        // First message should have its metadata
        if let Some(metadata) = &flushed[0].metadata {
            if let Some(meta) = metadata.as_kafka() {
                assert_eq!(meta.offset, 300);
            } else {
                panic!("Expected Kafka metadata in first message");
            }
        }

        // Second message should have no metadata (originally had none)
        assert!(flushed[1].metadata.is_none());

        // Third message is split, first part should have metadata (cloned)
        if let Some(metadata) = &flushed[2].metadata {
            if let Some(meta) = metadata.as_kafka() {
                assert_eq!(meta.offset, 301);
                assert_eq!(meta.partition, 2);
                assert_eq!(meta.topic_id, 3);
            } else {
                panic!("Expected Kafka metadata in split part");
            }
        }

        // Remaining batch should have the rest with metadata (cloned)
        let remaining = batch.take_batch();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining.size_of(), 2);
        if let Some(metadata) = &remaining[0].metadata {
            if let Some(meta) = metadata.as_kafka() {
                assert_eq!(meta.offset, 301);
            } else {
                panic!("Expected Kafka metadata in remaining message");
            }
        }
    }
}

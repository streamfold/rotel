// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::{BoundedSender, SendError};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Clone, Debug, PartialEq)]
pub struct Message<T> {
    pub metadata: Option<MessageMetadata>,
    pub payload: Vec<T>,
}

impl<T> Message<T> {
    // Used in testing
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.payload.len()
    }
}

pub struct MessageMetadata {
    data: MessageMetadataInner,
    ref_count: Arc<AtomicU32>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MessageMetadataInner {
    Kafka(KafkaMetadata),
}

impl MessageMetadata {
    /// Create new MessageMetadata with Kafka variant, starting with ref_count = 1
    pub fn kafka(metadata: KafkaMetadata) -> Self {
        Self {
            data: MessageMetadataInner::Kafka(metadata),
            ref_count: Arc::new(AtomicU32::new(1)),
        }
    }

    /// Get the inner metadata data
    pub fn inner(&self) -> &MessageMetadataInner {
        &self.data
    }

    /// Get reference count for debugging/testing
    pub fn ref_count(&self) -> u32 {
        self.ref_count.load(Ordering::Acquire)
    }

    /// Helper method to get Kafka metadata if available
    pub fn as_kafka(&self) -> Option<&KafkaMetadata> {
        match &self.data {
            MessageMetadataInner::Kafka(km) => Some(km),
        }
    }

    /// Create a shallow clone that shares the same reference count Arc
    /// Used for retry scenarios where we don't want to increment the ref count
    pub fn shallow_clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            ref_count: self.ref_count.clone(), // Share the same Arc, no increment
        }
    }
}

impl Clone for MessageMetadata {
    fn clone(&self) -> Self {
        // Increment the reference count atomically
        self.ref_count.fetch_add(1, Ordering::AcqRel);
        Self {
            data: self.data.clone(),
            ref_count: self.ref_count.clone(),
        }
    }
}

/// Debug implementation for MessageMetadata
impl std::fmt::Debug for MessageMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageMetadata")
            .field("data", &self.data)
            .field("ref_count", &self.ref_count.load(Ordering::Acquire))
            .finish()
    }
}

/// PartialEq implementation for MessageMetadata
impl PartialEq for MessageMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

#[derive(Clone)]
pub struct KafkaMetadata {
    pub offset: i64,
    pub partition: i32,
    pub topic_id: u8,
    pub ack_chan: Option<BoundedSender<KafkaAcknowledgement>>,
}

// Manual Debug for KafkaMetadata since BoundedSender doesn't implement Debug
impl std::fmt::Debug for KafkaMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaMetadata")
            .field("offset", &self.offset)
            .field("partition", &self.partition)
            .field("topic_id", &self.topic_id)
            .field("ack_chan", &self.ack_chan.is_some())
            .finish()
    }
}

// Manual PartialEq for KafkaMetadata since BoundedSender can't be compared
impl PartialEq for KafkaMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
            && self.partition == other.partition
            && self.topic_id == other.topic_id
        // We don't compare ack_chan as it's not meaningful for equality
    }
}

// TODO: consider whether we want a generic reason or enum.
pub trait Ack {
    #[allow(async_fn_in_trait)]
    async fn ack(&self) -> Result<(), SendError>;
    fn nack(&self);
}

impl Ack for MessageMetadata {
    async fn ack(&self) -> Result<(), SendError> {
        // Decrement reference count atomically
        let prev_count = self.ref_count.fetch_sub(1, Ordering::AcqRel);

        // Only send acknowledgment when count reaches 0
        if prev_count == 1 {
            match &self.data {
                MessageMetadataInner::Kafka(km) => {
                    if let Some(ack_chan) = &km.ack_chan {
                        ack_chan
                            .send(KafkaAcknowledgement::Ack(KafkaAck {
                                offset: km.offset,
                                partition: km.partition,
                                topic_id: km.topic_id,
                            }))
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    fn nack(&self) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_reference_counting_single_ack() {
        let (ack_tx, mut ack_rx) = crate::bounded_channel::bounded(1);

        let kafka_metadata = KafkaMetadata {
            offset: 123,
            partition: 0,
            topic_id: 1,
            ack_chan: Some(ack_tx),
        };

        let metadata = MessageMetadata::kafka(kafka_metadata);

        // Should start with ref count 1
        assert_eq!(metadata.ref_count(), 1);

        // Call ack() once - should send acknowledgment since count goes to 0
        metadata.ack().await.unwrap();

        // Should receive the acknowledgment
        let ack = ack_rx.next().await.unwrap();
        match ack {
            KafkaAcknowledgement::Ack(kafka_ack) => {
                assert_eq!(kafka_ack.offset, 123);
                assert_eq!(kafka_ack.partition, 0);
                assert_eq!(kafka_ack.topic_id, 1);
            }
            _ => panic!("Expected Ack"),
        }
    }

    #[tokio::test]
    async fn test_reference_counting_clone_and_ack() {
        let (ack_tx, mut ack_rx) = crate::bounded_channel::bounded(1);

        let kafka_metadata = KafkaMetadata {
            offset: 456,
            partition: 2,
            topic_id: 3,
            ack_chan: Some(ack_tx),
        };

        let metadata1 = MessageMetadata::kafka(kafka_metadata);
        assert_eq!(metadata1.ref_count(), 1);

        // Clone should increment ref count
        let metadata2 = metadata1.clone();
        assert_eq!(metadata1.ref_count(), 2);
        assert_eq!(metadata2.ref_count(), 2);

        // First ack() should not send acknowledgment (count goes from 2 to 1)
        metadata1.ack().await.unwrap();
        assert_eq!(metadata2.ref_count(), 1);

        // Should not have received acknowledgment yet
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), ack_rx.next()).await;
        assert!(result.is_err(), "Should not receive ack yet");

        // Second ack() should send acknowledgment (count goes from 1 to 0)
        metadata2.ack().await.unwrap();

        // Should receive the acknowledgment now
        let ack = ack_rx.next().await.unwrap();
        match ack {
            KafkaAcknowledgement::Ack(kafka_ack) => {
                assert_eq!(kafka_ack.offset, 456);
                assert_eq!(kafka_ack.partition, 2);
                assert_eq!(kafka_ack.topic_id, 3);
            }
            _ => panic!("Expected Ack"),
        }
    }

    #[tokio::test]
    async fn test_shallow_clone_does_not_increment_ref_count() {
        let (ack_tx, mut ack_rx) = crate::bounded_channel::bounded(1);

        let kafka_metadata = KafkaMetadata {
            offset: 789,
            partition: 1,
            topic_id: 2,
            ack_chan: Some(ack_tx),
        };

        let metadata1 = MessageMetadata::kafka(kafka_metadata);
        assert_eq!(metadata1.ref_count(), 1);

        // Shallow clone should NOT increment ref count (used for retry scenarios)
        let metadata2 = metadata1.shallow_clone();
        assert_eq!(metadata1.ref_count(), 1);
        assert_eq!(metadata2.ref_count(), 1);

        // First ack() should send acknowledgment immediately (count goes from 1 to 0)
        metadata1.ack().await.unwrap();

        // Should receive acknowledgment
        let ack = ack_rx.next().await.unwrap();
        match ack {
            KafkaAcknowledgement::Ack(kafka_ack) => {
                assert_eq!(kafka_ack.offset, 789);
                assert_eq!(kafka_ack.partition, 1);
                assert_eq!(kafka_ack.topic_id, 2);
            }
            _ => panic!("Expected Ack"),
        }

        // Second ack() on shallow clone should not send another acknowledgment
        // (since the first ack already sent it when count reached 0)
        metadata2.ack().await.unwrap();

        // Should not receive another acknowledgment
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), ack_rx.next()).await;
        assert!(result.is_err(), "Should not receive second ack");
    }
}
pub enum KafkaAcknowledgement {
    Ack(KafkaAck),
    Nack(KafkaNack),
}

pub struct KafkaAck {
    pub offset: i64,
    pub partition: i32,
    pub topic_id: u8,
}

pub struct KafkaNack {
    pub offset: i64,
    pub partition: i32,
    pub topic_id: u8,
    pub reason: ExporterError,
}

// We'll likely want to have a single shared ExporterError type, perhaps residing outside of both
// the payload and exporter modules that both can share. For now this is just serving as a placeholder
pub enum ExporterError {}

/// Trait for converting telemetry data into OTLP protocol format
pub trait OTLPFrom<T> {
    fn otlp_from(value: T) -> Self;
}

/// Trait for converting telemetry data into OTLP protocol format
pub trait OTLPInto<T> {
    fn otlp_into(self) -> T;
}

impl OTLPFrom<Vec<ResourceSpans>> for ExportTraceServiceRequest {
    fn otlp_from(value: Vec<ResourceSpans>) -> Self {
        ExportTraceServiceRequest {
            resource_spans: value,
        }
    }
}

impl OTLPFrom<Vec<Message<ResourceSpans>>> for ExportTraceServiceRequest {
    fn otlp_from(value: Vec<Message<ResourceSpans>>) -> Self {
        ExportTraceServiceRequest {
            resource_spans: value.into_iter().flat_map(|m| m.payload).collect(),
        }
    }
}

impl OTLPFrom<Vec<ResourceMetrics>> for ExportMetricsServiceRequest {
    fn otlp_from(value: Vec<ResourceMetrics>) -> Self {
        ExportMetricsServiceRequest {
            resource_metrics: value,
        }
    }
}

impl OTLPFrom<Vec<Message<ResourceMetrics>>> for ExportMetricsServiceRequest {
    fn otlp_from(value: Vec<Message<ResourceMetrics>>) -> Self {
        ExportMetricsServiceRequest {
            resource_metrics: value.into_iter().flat_map(|m| m.payload).collect(),
        }
    }
}

impl OTLPFrom<Vec<ResourceLogs>> for ExportLogsServiceRequest {
    fn otlp_from(value: Vec<ResourceLogs>) -> Self {
        ExportLogsServiceRequest {
            resource_logs: value,
        }
    }
}

impl OTLPFrom<Vec<Message<ResourceLogs>>> for ExportLogsServiceRequest {
    fn otlp_from(value: Vec<Message<ResourceLogs>>) -> Self {
        ExportLogsServiceRequest {
            resource_logs: value.into_iter().flat_map(|m| m.payload).collect(),
        }
    }
}

impl OTLPInto<Vec<ResourceSpans>> for ExportTraceServiceRequest {
    fn otlp_into(self) -> Vec<ResourceSpans> {
        self.resource_spans
    }
}

impl OTLPInto<Vec<ResourceMetrics>> for ExportMetricsServiceRequest {
    fn otlp_into(self) -> Vec<ResourceMetrics> {
        self.resource_metrics
    }
}

impl OTLPInto<Vec<ResourceLogs>> for ExportLogsServiceRequest {
    fn otlp_into(self) -> Vec<ResourceLogs> {
        self.resource_logs
    }
}

#[derive(Clone, Debug)]
pub enum OTLPPayload {
    Traces(Vec<ResourceSpans>),
    Metrics(Vec<ResourceMetrics>),
    Logs(Vec<ResourceLogs>),
}

impl From<Vec<ResourceMetrics>> for OTLPPayload {
    fn from(value: Vec<ResourceMetrics>) -> Self {
        OTLPPayload::Metrics(value)
    }
}

impl From<Vec<ResourceSpans>> for OTLPPayload {
    fn from(value: Vec<ResourceSpans>) -> Self {
        OTLPPayload::Traces(value)
    }
}

impl From<Vec<ResourceLogs>> for OTLPPayload {
    fn from(value: Vec<ResourceLogs>) -> Self {
        OTLPPayload::Logs(value)
    }
}

impl TryFrom<OTLPPayload> for Vec<ResourceSpans> {
    type Error = &'static str;

    fn try_from(value: OTLPPayload) -> Result<Self, Self::Error> {
        match value {
            OTLPPayload::Traces(v) => Ok(v),
            OTLPPayload::Metrics(_) => Err("Cannot convert metrics payload to traces"),
            OTLPPayload::Logs(_) => Err("Cannot convert logs payload to traces"),
        }
    }
}

impl TryFrom<OTLPPayload> for Vec<ResourceMetrics> {
    type Error = &'static str;

    fn try_from(value: OTLPPayload) -> Result<Self, Self::Error> {
        match value {
            OTLPPayload::Metrics(v) => Ok(v),
            OTLPPayload::Traces(_) => Err("Cannot convert traces payload to metrics"),
            OTLPPayload::Logs(_) => Err("Cannot convert logs payload to metrics"),
        }
    }
}

impl TryFrom<OTLPPayload> for Vec<ResourceLogs> {
    type Error = &'static str;

    fn try_from(value: OTLPPayload) -> Result<Self, Self::Error> {
        match value {
            OTLPPayload::Logs(v) => Ok(v),
            OTLPPayload::Traces(_) => Err("Cannot convert traces payload to logs"),
            OTLPPayload::Metrics(_) => Err("Cannot convert metrics payload to logs"),
        }
    }
}

impl TryFrom<OTLPPayload> for ExportTraceServiceRequest {
    type Error = &'static str;

    fn try_from(value: OTLPPayload) -> Result<Self, Self::Error> {
        match value {
            OTLPPayload::Traces(spans) => Ok(ExportTraceServiceRequest {
                resource_spans: spans,
            }),
            OTLPPayload::Metrics(_) => {
                Err("Cannot convert metrics payload to trace service request")
            }
            OTLPPayload::Logs(_) => Err("Cannot convert logs payload to trace service request"),
        }
    }
}

impl TryFrom<OTLPPayload> for ExportMetricsServiceRequest {
    type Error = &'static str;

    fn try_from(value: OTLPPayload) -> Result<Self, Self::Error> {
        match value {
            OTLPPayload::Metrics(metrics) => Ok(ExportMetricsServiceRequest {
                resource_metrics: metrics,
            }),
            OTLPPayload::Traces(_) => {
                Err("Cannot convert traces payload to metrics service request")
            }
            OTLPPayload::Logs(_) => Err("Cannot convert logs payload to metrics service request"),
        }
    }
}

impl TryFrom<OTLPPayload> for ExportLogsServiceRequest {
    type Error = &'static str;

    fn try_from(value: OTLPPayload) -> Result<Self, Self::Error> {
        match value {
            OTLPPayload::Logs(logs) => Ok(ExportLogsServiceRequest {
                resource_logs: logs,
            }),
            OTLPPayload::Traces(_) => Err("Cannot convert traces payload to logs service request"),
            OTLPPayload::Metrics(_) => {
                Err("Cannot convert metrics payload to logs service request")
            }
        }
    }
}

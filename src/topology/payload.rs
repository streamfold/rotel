// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::{BoundedSender, SendError};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

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

#[derive(Clone, Debug, PartialEq)]
pub enum MessageMetadata {
    Kafka(KafkaMetadata),
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
        match self {
            MessageMetadata::Kafka(km) => {
                if let Some(ack_chan) = &km.ack_chan {
                    ack_chan
                        .send(KafkaAcknowledgement::Ack(KafkaAck {
                            offset: km.offset,
                            partition: km.partition,
                            topic_id: km.topic_id,
                        }))
                        .await?;
                }
                Ok(())
            }
        }
    }

    fn nack(&self) {
        todo!()
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

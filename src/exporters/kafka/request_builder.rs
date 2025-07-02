// SPDX-License-Identifier: Apache-2.0

use crate::exporters::kafka::config::SerializationFormat;
use crate::exporters::kafka::errors::Result;
use crate::topology::payload::OTLPFrom;
use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use prost::Message;
use serde::Serialize;

/// Generic builder for creating Kafka messages from telemetry data
#[derive(Clone)]
pub struct KafkaRequestBuilder<Resource, Request> {
    serialization_format: SerializationFormat,
    _phantom: std::marker::PhantomData<(Resource, Request)>,
}

impl<Resource, Request> KafkaRequestBuilder<Resource, Request>
where
    Resource: Message + Serialize + Clone,
    Request: Message + OTLPFrom<Vec<Resource>> + Serialize,
{
    /// Create a new request builder
    pub fn new(format: SerializationFormat) -> Self {
        Self {
            serialization_format: format,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Build message from telemetry resources
    pub fn build_message(&self, resources: &[Resource]) -> Result<Bytes> {
        let payload = match self.serialization_format {
            SerializationFormat::Json => self.serialize_json(resources)?,
            SerializationFormat::Protobuf => self.serialize_protobuf(resources)?,
        };
        Ok(payload)
    }

    /// Serialize data as JSON
    fn serialize_json(&self, data: &[Resource]) -> Result<Bytes> {
        let json = serde_json::to_vec(data)?;
        Ok(Bytes::from(json))
    }

    /// Serialize resources as protobuf using OTLP service request format
    fn serialize_protobuf(&self, resources: &[Resource]) -> Result<Bytes> {
        // Create OTLP service request using OTLPFrom trait
        let request = Request::otlp_from(resources.to_vec());

        let mut buf = Vec::new();
        request.encode(&mut buf)?;
        Ok(Bytes::from(buf))
    }
}

// // Type aliases for backward compatibility and convenience
pub type KafkaTraceRequestBuilder = KafkaRequestBuilder<ResourceSpans, ExportTraceServiceRequest>;
pub type KafkaMetricsRequestBuilder =
    KafkaRequestBuilder<ResourceMetrics, ExportMetricsServiceRequest>;
pub type KafkaLogsRequestBuilder = KafkaRequestBuilder<ResourceLogs, ExportLogsServiceRequest>;

// Convenience methods for specific telemetry types
impl KafkaTraceRequestBuilder {
    /// Build message from trace spans
    pub fn build_trace_message(&self, spans: &[ResourceSpans]) -> Result<Bytes> {
        self.build_message(spans)
    }
}

impl KafkaMetricsRequestBuilder {
    /// Build message from metrics
    pub fn build_metrics_message(&self, metrics: &[ResourceMetrics]) -> Result<Bytes> {
        self.build_message(metrics)
    }
}

impl KafkaLogsRequestBuilder {
    /// Build message from logs
    pub fn build_logs_message(&self, logs: &[ResourceLogs]) -> Result<Bytes> {
        self.build_message(logs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::kafka::config::SerializationFormat;

    #[test]
    fn test_json_serialization() {
        let builder = KafkaTraceRequestBuilder::new(SerializationFormat::Json);
        let spans: Vec<ResourceSpans> = vec![];

        let result = builder.build_trace_message(&spans);
        assert!(result.is_ok());

        let payload = result.unwrap();
        assert!(!payload.is_empty());
    }
}

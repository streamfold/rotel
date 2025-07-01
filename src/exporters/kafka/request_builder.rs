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

/// Builder for creating Kafka messages from telemetry data
#[derive(Clone)]
pub struct KafkaRequestBuilder {
    serialization_format: SerializationFormat,
}

impl KafkaRequestBuilder {
    /// Create a new request builder
    pub fn new(format: SerializationFormat) -> Self {
        Self {
            serialization_format: format,
        }
    }

    /// Build message from trace spans
    pub fn build_trace_message(&self, spans: &[ResourceSpans]) -> Result<Bytes> {
        let payload = match self.serialization_format {
            SerializationFormat::Json => self.serialize_json(&spans)?,
            SerializationFormat::Protobuf => self.serialize_protobuf_traces(spans)?,
        };
        Ok(payload)
    }

    /// Build message from metrics
    pub fn build_metrics_message(&self, metrics: &[ResourceMetrics]) -> Result<Bytes> {
        let payload = match self.serialization_format {
            SerializationFormat::Json => self.serialize_json(&metrics)?,
            SerializationFormat::Protobuf => self.serialize_protobuf_metrics(metrics)?,
        };
        Ok(payload)
    }

    /// Build message from logs
    pub fn build_logs_message(&self, logs: &[ResourceLogs]) -> Result<Bytes> {
        let payload = match self.serialization_format {
            SerializationFormat::Json => self.serialize_json(&logs)?,
            SerializationFormat::Protobuf => self.serialize_protobuf_logs(logs)?,
        };
        Ok(payload)
    }

    /// Serialize data as JSON
    fn serialize_json<T: Serialize>(&self, data: &T) -> Result<Bytes> {
        let json = serde_json::to_vec(data)?;
        Ok(Bytes::from(json))
    }

    /// Serialize traces as protobuf
    fn serialize_protobuf_traces(&self, spans: &[ResourceSpans]) -> Result<Bytes> {
        // Create a TracesData message (from OTLP spec) using OTLPFrom trait
        let traces_data = ExportTraceServiceRequest::otlp_from(spans.to_vec());

        let mut buf = Vec::new();
        traces_data.encode(&mut buf)?;
        Ok(Bytes::from(buf))
    }

    /// Serialize metrics as protobuf
    fn serialize_protobuf_metrics(&self, metrics: &[ResourceMetrics]) -> Result<Bytes> {
        // Create a MetricsData message (from OTLP spec) using OTLPFrom trait
        let metrics_data = ExportMetricsServiceRequest::otlp_from(metrics.to_vec());

        let mut buf = Vec::new();
        metrics_data.encode(&mut buf)?;
        Ok(Bytes::from(buf))
    }

    /// Serialize logs as protobuf
    fn serialize_protobuf_logs(&self, logs: &[ResourceLogs]) -> Result<Bytes> {
        // Create a LogsData message (from OTLP spec) using OTLPFrom trait
        let logs_data = ExportLogsServiceRequest::otlp_from(logs.to_vec());

        let mut buf = Vec::new();
        logs_data.encode(&mut buf)?;
        Ok(Bytes::from(buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::kafka::config::SerializationFormat;

    #[test]
    fn test_json_serialization() {
        let builder = KafkaRequestBuilder::new(SerializationFormat::Json);
        let spans: Vec<ResourceSpans> = vec![];

        let result = builder.build_trace_message(&spans);
        assert!(result.is_ok());

        let payload = result.unwrap();
        assert!(!payload.is_empty());
    }
}

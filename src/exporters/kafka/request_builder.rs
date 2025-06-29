// SPDX-License-Identifier: Apache-2.0

use crate::exporters::kafka::config::SerializationFormat;
use crate::exporters::kafka::errors::Result;
use bytes::Bytes;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use prost::Message;
use serde::Serialize;

/// Key for Kafka messages
#[derive(Clone, Debug)]
pub struct MessageKey {
    pub telemetry_type: String,
    pub resource_id: Option<String>,
}

impl MessageKey {
    /// Create a new message key
    pub fn new(telemetry_type: &str) -> Self {
        Self {
            telemetry_type: telemetry_type.to_string(),
            resource_id: None,
        }
    }

    /// Set the resource ID
    pub fn with_resource_id(mut self, id: String) -> Self {
        self.resource_id = Some(id);
        self
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        match &self.resource_id {
            Some(id) => format!("{}:{}", self.telemetry_type, id),
            None => self.telemetry_type.clone(),
        }
    }
}

/// Builder for creating Kafka messages from telemetry data
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
    pub fn build_trace_message(&self, spans: &[ResourceSpans]) -> Result<(MessageKey, Bytes)> {
        let key = MessageKey::new("traces");
        let payload = match self.serialization_format {
            SerializationFormat::Json => self.serialize_json(&spans)?,
            SerializationFormat::Protobuf => self.serialize_protobuf_traces(spans)?,
        };
        Ok((key, payload))
    }

    /// Build message from metrics
    pub fn build_metrics_message(
        &self,
        metrics: &[ResourceMetrics],
    ) -> Result<(MessageKey, Bytes)> {
        let key = MessageKey::new("metrics");
        let payload = match self.serialization_format {
            SerializationFormat::Json => self.serialize_json(&metrics)?,
            SerializationFormat::Protobuf => self.serialize_protobuf_metrics(metrics)?,
        };
        Ok((key, payload))
    }

    /// Build message from logs
    pub fn build_logs_message(&self, logs: &[ResourceLogs]) -> Result<(MessageKey, Bytes)> {
        let key = MessageKey::new("logs");
        let payload = match self.serialization_format {
            SerializationFormat::Json => self.serialize_json(&logs)?,
            SerializationFormat::Protobuf => self.serialize_protobuf_logs(logs)?,
        };
        Ok((key, payload))
    }

    /// Serialize data as JSON
    fn serialize_json<T: Serialize>(&self, data: &T) -> Result<Bytes> {
        let json = serde_json::to_vec(data)?;
        Ok(Bytes::from(json))
    }

    /// Serialize traces as protobuf
    fn serialize_protobuf_traces(&self, spans: &[ResourceSpans]) -> Result<Bytes> {
        // Create a TracesData message (from OTLP spec)
        let traces_data =
            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
                resource_spans: spans.to_vec(),
            };

        let mut buf = Vec::new();
        traces_data.encode(&mut buf)?;
        Ok(Bytes::from(buf))
    }

    /// Serialize metrics as protobuf
    fn serialize_protobuf_metrics(&self, metrics: &[ResourceMetrics]) -> Result<Bytes> {
        // Create a MetricsData message (from OTLP spec)
        let metrics_data =
            opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
                resource_metrics: metrics.to_vec(),
            };

        let mut buf = Vec::new();
        metrics_data.encode(&mut buf)?;
        Ok(Bytes::from(buf))
    }

    /// Serialize logs as protobuf
    fn serialize_protobuf_logs(&self, logs: &[ResourceLogs]) -> Result<Bytes> {
        // Create a LogsData message (from OTLP spec)
        let logs_data = opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest {
            resource_logs: logs.to_vec(),
        };

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
    fn test_message_key() {
        let key = MessageKey::new("traces");
        assert_eq!(key.to_string(), "traces");

        let key_with_id = MessageKey::new("metrics").with_resource_id("service-123".to_string());
        assert_eq!(key_with_id.to_string(), "metrics:service-123");
    }

    #[test]
    fn test_json_serialization() {
        let builder = KafkaRequestBuilder::new(SerializationFormat::Json);
        let spans: Vec<ResourceSpans> = vec![];

        let result = builder.build_trace_message(&spans);
        assert!(result.is_ok());

        let (key, payload) = result.unwrap();
        assert_eq!(key.telemetry_type, "traces");
        assert!(!payload.is_empty());
    }
}

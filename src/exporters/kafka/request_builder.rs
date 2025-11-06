// SPDX-License-Identifier: Apache-2.0

use crate::exporters::kafka::config::SerializationFormat;
use crate::exporters::kafka::errors::Result;
use crate::topology::payload::OTLPFrom;
use bytes::Bytes;
use opentelemetry_proto::tonic::logs::v1::{LogsData, ResourceLogs};
use opentelemetry_proto::tonic::metrics::v1::{MetricsData, ResourceMetrics};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, TracesData};
use prost::Message;
use serde::Serialize;

/// Trait for converting resources to their corresponding *Data wrapper types
pub trait ToDataWrapper: Sized {
    type DataType: Serialize;
    fn to_data_wrapper(resources: Vec<Self>) -> Self::DataType;
}

impl ToDataWrapper for ResourceSpans {
    type DataType = TracesData;
    fn to_data_wrapper(resources: Vec<Self>) -> Self::DataType {
        TracesData {
            resource_spans: resources,
        }
    }
}

impl ToDataWrapper for ResourceMetrics {
    type DataType = MetricsData;
    fn to_data_wrapper(resources: Vec<Self>) -> Self::DataType {
        MetricsData {
            resource_metrics: resources,
        }
    }
}

impl ToDataWrapper for ResourceLogs {
    type DataType = LogsData;
    fn to_data_wrapper(resources: Vec<Self>) -> Self::DataType {
        LogsData {
            resource_logs: resources,
        }
    }
}

/// Generic builder for creating Kafka messages from telemetry data
#[derive(Clone)]
pub struct KafkaRequestBuilder<Resource, Request> {
    serialization_format: SerializationFormat,
    _phantom: std::marker::PhantomData<(Resource, Request)>,
}

impl<Resource, Request> KafkaRequestBuilder<Resource, Request>
where
    Resource: Message + Serialize + Clone + ToDataWrapper,
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
    pub fn build_message(&self, resources: Vec<Resource>) -> Result<Bytes> {
        let payload = match self.serialization_format {
            SerializationFormat::Json => self.serialize_json(resources)?,
            SerializationFormat::Protobuf => self.serialize_protobuf(resources)?,
        };
        Ok(payload)
    }

    /// Serialize data as JSON using the *Data wrapper types
    fn serialize_json(&self, resources: Vec<Resource>) -> Result<Bytes> {
        // For JSON format, wrap resources in TracesData/MetricsData/LogsData structure
        let data_wrapper = Resource::to_data_wrapper(resources);
        let json = serde_json::to_vec(&data_wrapper)?;
        Ok(Bytes::from(json))
    }

    /// Serialize resources as protobuf using OTLP service request format
    fn serialize_protobuf(&self, resources: Vec<Resource>) -> Result<Bytes> {
        // Create OTLP service request using OTLPFrom trait
        let request = Request::otlp_from(resources);

        let mut buf = Vec::new();
        request.encode(&mut buf)?;
        Ok(Bytes::from(buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::kafka::config::SerializationFormat;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

    #[test]
    fn test_json_serialization() {
        let builder: KafkaRequestBuilder<ResourceSpans, ExportTraceServiceRequest> =
            KafkaRequestBuilder::new(SerializationFormat::Json);
        let spans: Vec<ResourceSpans> = vec![];

        let result = builder.build_message(spans);
        assert!(result.is_ok());

        let payload = result.unwrap();
        assert!(!payload.is_empty());
    }
}

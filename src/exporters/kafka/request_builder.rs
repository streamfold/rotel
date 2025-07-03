// SPDX-License-Identifier: Apache-2.0

use crate::exporters::kafka::config::SerializationFormat;
use crate::exporters::kafka::errors::Result;
use crate::topology::payload::OTLPFrom;
use bytes::Bytes;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::kafka::config::SerializationFormat;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

    #[test]
    fn test_json_serialization() {
        let builder: KafkaRequestBuilder<ResourceSpans, ExportTraceServiceRequest> =
            KafkaRequestBuilder::new(SerializationFormat::Json);
        let spans: Vec<ResourceSpans> = vec![];

        let result = builder.build_message(&spans);
        assert!(result.is_ok());

        let payload = result.unwrap();
        assert!(!payload.is_empty());
    }
}

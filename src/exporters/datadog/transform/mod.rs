// SPDX-License-Identifier: Apache-2.0

use crate::exporters::datadog::request_builder::TransformPayload;
use crate::exporters::datadog::transform::transformer::TraceTransformer;
use crate::exporters::datadog::types::pb::AgentPayload;
use crate::topology::payload::{Message, MessageMetadata};
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

mod attributes;
pub mod cloud;
mod db_types;
pub mod k8s;
pub mod otel_mapping;
mod otel_util;
mod sampler;
pub mod source;
pub mod transformer;

#[derive(Clone)]
pub struct Transformer {
    transformer: TraceTransformer,
    environment: String,
    hostname: String,
}

impl Transformer {
    pub fn new(environment: String, hostname: String) -> Self {
        let transformer = TraceTransformer::new(environment.clone(), hostname.clone());
        Self {
            environment,
            hostname,
            transformer,
        }
    }
}

impl TransformPayload<ResourceSpans> for Transformer {
    fn transform(
        &self,
        messages: Vec<Message<ResourceSpans>>,
    ) -> (AgentPayload, Option<Vec<MessageMetadata>>) {
        let mut payload = AgentPayload {
            host_name: self.hostname.clone(),
            env: self.environment.clone(),
            tracer_payloads: vec![],
            tags: Default::default(),
            agent_version: "0.0.1".to_string(),
            target_tps: 1.0,
            error_tps: 1.0,
            rare_sampler_enabled: false,
        };

        // Move metadata out of messages without cloning
        let metadata: Vec<MessageMetadata> = messages
            .into_iter()
            .filter_map(|message| {
                for rs in message.payload {
                    let tp = self.transformer.apply(rs);
                    payload.tracer_payloads.push(tp);
                }
                message.metadata
            })
            .collect();

        let metadata = if metadata.is_empty() {
            None
        } else {
            Some(metadata)
        };

        (payload, metadata)
    }
}

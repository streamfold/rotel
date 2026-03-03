// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use crate::topology::payload::{Ack, Message, MessageMetadata};

use super::client::RedisConnection;
use super::config::RedisStreamExporterConfig;
use super::errors::Result;
use super::transformer::transform_span;

use crate::exporters::shared::otel_span::find_str_attribute;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use redis::streams::StreamMaxlen;
use std::collections::HashSet;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub struct RedisStreamExporter {
    config: RedisStreamExporterConfig,
    connection: Option<RedisConnection>,
    rx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
}

pub fn build_traces_exporter(
    config: RedisStreamExporterConfig,
    rx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
) -> Result<RedisStreamExporter> {
    Ok(RedisStreamExporter {
        config,
        connection: None,
        rx,
    })
}

impl RedisStreamExporter {
    async fn connect(&mut self) -> Result<()> {
        let conn = RedisConnection::connect(&self.config).await?;
        self.connection = Some(conn);
        Ok(())
    }

    pub async fn start(&mut self, cancel_token: CancellationToken) {
        if let Err(e) = self.connect().await {
            error!(error = %e, "Failed to connect to Redis, exporter will not start");
            return;
        }
        info!(
            endpoint = self.config.endpoint,
            stream_key_template = ?self.config.stream_key_template,
            format = ?self.config.format,
            cluster_mode = self.config.cluster_mode,
            maxlen = ?self.config.maxlen,
            pipeline_size = ?self.config.pipeline_size,
            key_ttl_seconds = ?self.config.key_ttl_seconds,
            filter_service_names = ?self.config.filter_service_names,
            "Redis stream exporter started"
        );

        loop {
            select! {
                m = self.rx.next() => {
                    match m {
                        Some(messages) => {
                            let batch_size = messages.iter().map(|m| m.payload.len()).sum::<usize>();
                            if let Err(e) = self.process_batch(messages).await {
                                if !cancel_token.is_cancelled() {
                                    error!(error = %e, batch_size, "Failed to process Redis stream batch");
                                }
                            }
                        }
                        None => break,
                    }
                },
                _ = cancel_token.cancelled() => break,
            }
        }
        debug!("exiting redis stream exporter");
    }

    async fn process_batch(
        &mut self,
        messages: Vec<Message<ResourceSpans>>,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (all_metadata, xadd_items) = prepare_batch(&self.config, messages);

        // Build and execute pipeline(s)
        let conn = self.connection.as_mut().ok_or_else(|| {
            "Redis connection not established".to_string()
        })?;

        let maxlen = self.config.maxlen.map(StreamMaxlen::Approx);

        let mut unique_keys: HashSet<&str> = HashSet::new();

        let chunk_size = self.config.pipeline_size.unwrap_or(usize::MAX);
        for chunk in xadd_items.chunks(chunk_size) {
            let mut pipe = redis::pipe();
            for (key, fields) in chunk {
                let field_refs: Vec<(&str, &str)> =
                    fields.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                match maxlen {
                    Some(ref ml) => {
                        pipe.xadd_maxlen(key, ml.clone(), "*", &field_refs);
                    }
                    None => {
                        pipe.xadd(key, "*", &field_refs);
                    }
                }
                unique_keys.insert(key);
            }
            conn.execute_pipeline(&pipe).await?;
        }

        // Set TTL on stream keys if configured
        if let Some(ttl) = self.config.key_ttl_seconds {
            if !unique_keys.is_empty() {
                let mut pipe = redis::pipe();
                for key in &unique_keys {
                    pipe.expire(*key, ttl as i64);
                }
                conn.execute_pipeline(&pipe).await?;
            }
        }

        // Ack all metadata on success
        for metadata in all_metadata {
            if let Err(e) = metadata.ack().await {
                warn!(error = ?e, "Failed to acknowledge message");
            }
        }

        Ok(())
    }
}

/// Pure logic: extract metadata, filter by service name, resolve stream keys, transform spans.
/// Returns (metadata_to_ack, xadd_items).
fn prepare_batch(
    config: &RedisStreamExporterConfig,
    messages: Vec<Message<ResourceSpans>>,
) -> (Vec<MessageMetadata>, Vec<(String, Vec<(String, String)>)>) {
    let mut all_metadata: Vec<MessageMetadata> = Vec::new();
    let mut xadd_items: Vec<(String, Vec<(String, String)>)> = Vec::new();

    let has_service_filter = !config.filter_service_names.is_empty();
    let mut filtered_span_count: usize = 0;

    for message in messages {
        if let Some(metadata) = message.metadata {
            all_metadata.push(metadata);
        }

        for rs in message.payload {
            let resource = rs.resource.as_ref();

            // Skip this ResourceSpans if service name doesn't match filter
            if has_service_filter {
                let svc = resource
                    .map(|r| find_str_attribute(SERVICE_NAME, &r.attributes))
                    .unwrap_or_default();
                if !config.filter_service_names.iter().any(|f| f == svc.as_ref()) {
                    let span_count: usize =
                        rs.scope_spans.iter().map(|ss| ss.spans.len()).sum();
                    filtered_span_count += span_count;
                    continue;
                }
            }

            for ss in &rs.scope_spans {
                let (scope_name, scope_version) = match &ss.scope {
                    Some(scope) => (scope.name.as_str(), scope.version.as_str()),
                    None => ("", ""),
                };

                for span in &ss.spans {
                    let stream_key =
                        config.stream_key_template.resolve(resource, &span.attributes);
                    let fields = transform_span(
                        &config.format,
                        span,
                        resource,
                        scope_name,
                        scope_version,
                    );
                    xadd_items.push((stream_key, fields));
                }
            }
        }
    }

    if filtered_span_count > 0 {
        debug!(
            filtered_span_count,
            "Dropped spans not matching service name filter"
        );
    }

    (all_metadata, xadd_items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::redis_stream::config::SerializationFormat;
    use crate::exporters::redis_stream::stream_key::StreamKeyTemplate;
    use crate::topology::payload::Message;
    use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{
        ResourceSpans, ScopeSpans, Span, Status,
    };

    fn make_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
        }
    }

    fn make_span(name: &str, attrs: Vec<KeyValue>) -> Span {
        Span {
            trace_id: vec![0; 16],
            span_id: vec![0; 8],
            parent_span_id: vec![],
            name: name.to_string(),
            kind: 2, // Server
            start_time_unix_nano: 1000000000,
            end_time_unix_nano: 2000000000,
            attributes: attrs,
            status: Some(Status {
                message: "".to_string(),
                code: 1,
            }),
            trace_state: String::new(),
            flags: 0,
            events: vec![],
            links: vec![],
            dropped_attributes_count: 0,
            dropped_events_count: 0,
            dropped_links_count: 0,
        }
    }

    fn make_resource_spans(service_name: &str, spans: Vec<Span>) -> ResourceSpans {
        ResourceSpans {
            resource: Some(Resource {
                attributes: vec![make_kv("service.name", service_name)],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }
    }

    fn make_message(resource_spans: Vec<ResourceSpans>) -> Message<ResourceSpans> {
        Message {
            metadata: None,
            request_context: None,
            payload: resource_spans,
        }
    }

    fn default_config() -> RedisStreamExporterConfig {
        RedisStreamExporterConfig::default()
    }

    #[test]
    fn test_prepare_batch_no_filter() {
        let config = default_config();
        let rs = make_resource_spans("my-service", vec![make_span("span1", vec![])]);
        let messages = vec![make_message(vec![rs])];

        let (_, xadd_items) = prepare_batch(&config, messages);
        assert_eq!(xadd_items.len(), 1);
        assert_eq!(xadd_items[0].0, "rotel:traces");
    }

    #[test]
    fn test_prepare_batch_service_filter_allows_matching() {
        let config = default_config()
            .with_filter_service_names(vec!["api-gateway".to_string()]);
        let rs = make_resource_spans("api-gateway", vec![make_span("span1", vec![])]);
        let messages = vec![make_message(vec![rs])];

        let (_, xadd_items) = prepare_batch(&config, messages);
        assert_eq!(xadd_items.len(), 1);
    }

    #[test]
    fn test_prepare_batch_service_filter_drops_non_matching() {
        let config = default_config()
            .with_filter_service_names(vec!["api-gateway".to_string()]);
        let rs = make_resource_spans(
            "payment-service",
            vec![make_span("span1", vec![]), make_span("span2", vec![])],
        );
        let messages = vec![make_message(vec![rs])];

        let (_, xadd_items) = prepare_batch(&config, messages);
        assert_eq!(xadd_items.len(), 0);
    }

    #[test]
    fn test_prepare_batch_service_filter_mixed_services() {
        let config = default_config()
            .with_filter_service_names(vec!["api-gateway".to_string()]);
        let rs_allowed = make_resource_spans("api-gateway", vec![make_span("span1", vec![])]);
        let rs_blocked = make_resource_spans("payment-service", vec![make_span("span2", vec![])]);
        let messages = vec![make_message(vec![rs_allowed, rs_blocked])];

        let (_, xadd_items) = prepare_batch(&config, messages);
        assert_eq!(xadd_items.len(), 1);
    }

    #[test]
    fn test_prepare_batch_service_filter_multiple_allowed() {
        let config = default_config().with_filter_service_names(vec![
            "api-gateway".to_string(),
            "payment-service".to_string(),
        ]);
        let rs1 = make_resource_spans("api-gateway", vec![make_span("span1", vec![])]);
        let rs2 = make_resource_spans("payment-service", vec![make_span("span2", vec![])]);
        let rs3 = make_resource_spans("unknown-service", vec![make_span("span3", vec![])]);
        let messages = vec![make_message(vec![rs1, rs2, rs3])];

        let (_, xadd_items) = prepare_batch(&config, messages);
        assert_eq!(xadd_items.len(), 2);
    }

    #[test]
    fn test_prepare_batch_stream_key_template() {
        let config = default_config()
            .with_stream_key_template(StreamKeyTemplate::parse("traces:{service.name}"));
        let rs = make_resource_spans("my-service", vec![make_span("span1", vec![])]);
        let messages = vec![make_message(vec![rs])];

        let (_, xadd_items) = prepare_batch(&config, messages);
        assert_eq!(xadd_items.len(), 1);
        assert_eq!(xadd_items[0].0, "traces:my-service");
    }

    #[test]
    fn test_prepare_batch_json_format() {
        let config = default_config().with_format(SerializationFormat::Json);
        let rs = make_resource_spans("my-service", vec![make_span("span1", vec![])]);
        let messages = vec![make_message(vec![rs])];

        let (_, xadd_items) = prepare_batch(&config, messages);
        assert_eq!(xadd_items.len(), 1);
        assert_eq!(xadd_items[0].1.len(), 1);
        assert_eq!(xadd_items[0].1[0].0, "data");
        // Verify it's valid JSON
        let _: serde_json::Value = serde_json::from_str(&xadd_items[0].1[0].1).unwrap();
    }

    #[test]
    fn test_prepare_batch_flat_format() {
        let config = default_config().with_format(SerializationFormat::Flat);
        let rs = make_resource_spans("my-service", vec![make_span("span1", vec![])]);
        let messages = vec![make_message(vec![rs])];

        let (_, xadd_items) = prepare_batch(&config, messages);
        assert_eq!(xadd_items.len(), 1);
        let field_map: std::collections::HashMap<&str, &str> = xadd_items[0]
            .1
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(field_map.get("span_name"), Some(&"span1"));
        assert_eq!(field_map.get("service_name"), Some(&"my-service"));
    }

    #[test]
    fn test_prepare_batch_multiple_spans() {
        let config = default_config();
        let rs = make_resource_spans(
            "my-service",
            vec![
                make_span("span1", vec![]),
                make_span("span2", vec![]),
                make_span("span3", vec![]),
            ],
        );
        let messages = vec![make_message(vec![rs])];

        let (_, xadd_items) = prepare_batch(&config, messages);
        assert_eq!(xadd_items.len(), 3);
    }

    #[test]
    fn test_prepare_batch_empty_messages() {
        let config = default_config();
        let messages: Vec<Message<ResourceSpans>> = vec![];

        let (metadata, xadd_items) = prepare_batch(&config, messages);
        assert!(metadata.is_empty());
        assert!(xadd_items.is_empty());
    }

    #[test]
    fn test_config_ttl_default_none() {
        let config = default_config();
        assert!(config.key_ttl_seconds.is_none());
    }

    #[test]
    fn test_config_ttl_set() {
        let config = default_config().with_key_ttl_seconds(Some(3600));
        assert_eq!(config.key_ttl_seconds, Some(3600));
    }

    #[test]
    fn test_config_builder_chain() {
        let config = RedisStreamExporterConfig::new("redis://custom:6380".to_string())
            .with_format(SerializationFormat::Flat)
            .with_maxlen(Some(10000))
            .with_cluster_mode(true)
            .with_ca_cert_path(Some("/path/to/ca.pem".to_string()))
            .with_username(Some("user".to_string()))
            .with_password(Some("pass".to_string()))
            .with_pipeline_size(Some(500))
            .with_filter_service_names(vec!["svc-a".to_string()])
            .with_key_ttl_seconds(Some(7200));

        assert_eq!(config.endpoint, "redis://custom:6380");
        assert_eq!(config.format, SerializationFormat::Flat);
        assert_eq!(config.maxlen, Some(10000));
        assert!(config.cluster_mode);
        assert_eq!(config.ca_cert_path, Some("/path/to/ca.pem".to_string()));
        assert_eq!(config.username, Some("user".to_string()));
        assert_eq!(config.password, Some("pass".to_string()));
        assert_eq!(config.pipeline_size, Some(500));
        assert_eq!(config.filter_service_names, vec!["svc-a".to_string()]);
        assert_eq!(config.key_ttl_seconds, Some(7200));
    }
}

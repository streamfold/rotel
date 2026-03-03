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
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

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
        info!("Redis stream exporter started");

        loop {
            select! {
                m = self.rx.next() => {
                    match m {
                        Some(messages) => {
                            if let Err(e) = self.process_batch(messages).await {
                                if !cancel_token.is_cancelled() {
                                    error!(error = %e, "Failed to process Redis stream batch");
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
        let mut all_metadata: Vec<MessageMetadata> = Vec::new();
        let mut xadd_items: Vec<(String, Vec<(String, String)>)> = Vec::new();

        let has_service_filter = !self.config.filter_service_names.is_empty();
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
                    if !self.config.filter_service_names.iter().any(|f| f == svc.as_ref()) {
                        let span_count: usize = rs.scope_spans.iter().map(|ss| ss.spans.len()).sum();
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
                        let stream_key = self.config.stream_key_template.resolve(
                            resource,
                            &span.attributes,
                        );
                        let fields = transform_span(
                            &self.config.format,
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
            debug!(filtered_span_count, "Dropped spans not matching service name filter");
        }

        // Build and execute pipeline(s)
        let conn = self
            .connection
            .as_mut()
            .expect("connection must be established before processing");

        let maxlen = self.config.maxlen.map(StreamMaxlen::Approx);

        if let Some(pipeline_size) = self.config.pipeline_size {
            // Split into sub-batches
            for chunk in xadd_items.chunks(pipeline_size) {
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
                }
                conn.execute_pipeline(&pipe).await?;
            }
        } else {
            let mut pipe = redis::pipe();
            for (key, fields) in &xadd_items {
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
            }
            if !xadd_items.is_empty() {
                conn.execute_pipeline(&pipe).await?;
            }
        }

        // Ack all metadata on success
        for metadata in all_metadata {
            if let Err(e) = metadata.ack().await {
                debug!(error = ?e, "Failed to acknowledge message");
            }
        }

        Ok(())
    }
}

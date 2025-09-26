// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use crate::topology::batch::{BatchConfig, BatchSizer, BatchSplittable, NestedBatch};
use crate::topology::fanout::{Fanout, FanoutFuture};
use crate::topology::flush_control::{FlushReceiver, conditional_flush};
use crate::topology::payload::Message;
use opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
#[cfg(feature = "pyo3")]
use rotel_sdk::model::{PythonProcessable, register_processor};
#[cfg(feature = "pyo3")]
use std::env;
use std::error::Error;
#[cfg(feature = "pyo3")]
use std::path::Path;
use std::time::Duration;
use tokio::select;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "pyo3")]
use tower::BoxError;
use tracing::log::warn;
use tracing::{debug, error};

//#[derive(Clone)]
#[allow(dead_code)] // for the sake of the pyo3 feature
pub struct Pipeline<T> {
    receiver: BoundedReceiver<Message<T>>,
    fanout: Fanout<Vec<T>>,
    batch_config: BatchConfig,
    processors: Vec<String>,
    flush_listener: Option<FlushReceiver>,
    resource_attributes: Vec<KeyValue>,
}

pub trait Inspect<T> {
    fn inspect(&self, value: &[T]);
    fn inspect_with_prefix(&self, prefix: Option<String>, value: &[T]);
}

pub trait ResourceAttributeSettable {
    fn set_or_append_attributes(&mut self, attributes: Vec<KeyValue>);
}

impl ResourceAttributeSettable for ResourceSpans {
    fn set_or_append_attributes(&mut self, attributes: Vec<KeyValue>) {
        self.resource = Some(match self.resource.take() {
            Some(rs) => Resource {
                attributes: build_attrs(rs.attributes, attributes),
                dropped_attributes_count: rs.dropped_attributes_count,
                entity_refs: rs.entity_refs,
            },
            None => Resource {
                attributes: build_attrs(Vec::new(), attributes),
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            },
        });
    }
}

impl ResourceAttributeSettable for ResourceMetrics {
    fn set_or_append_attributes(&mut self, attributes: Vec<KeyValue>) {
        self.resource = Some(match self.resource.take() {
            Some(rs) => Resource {
                attributes: build_attrs(rs.attributes, attributes),
                dropped_attributes_count: rs.dropped_attributes_count,
                entity_refs: rs.entity_refs,
            },
            None => Resource {
                attributes: build_attrs(Vec::new(), attributes),
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            },
        });
    }
}

impl ResourceAttributeSettable for ResourceLogs {
    fn set_or_append_attributes(&mut self, attributes: Vec<KeyValue>) {
        self.resource = Some(match self.resource.take() {
            Some(rs) => Resource {
                attributes: build_attrs(rs.attributes, attributes),
                dropped_attributes_count: rs.dropped_attributes_count,
                entity_refs: rs.entity_refs,
            },
            None => Resource {
                attributes: build_attrs(Vec::new(), attributes),
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            },
        });
    }
}

pub fn build_attrs(resource_attributes: Vec<KeyValue>, attributes: Vec<KeyValue>) -> Vec<KeyValue> {
    if resource_attributes.is_empty() {
        return attributes;
    }
    // Let's retain the order and create a map of keys to reduce the number of iterations required to find/replace an existing key
    let mut map = indexmap::IndexMap::<String, KeyValue>::new();
    // Yes there is only one, but we've already determined that we have one.
    for attr in resource_attributes.iter() {
        map.insert(attr.key.clone(), attr.clone());
    }

    // Now iterate the new attributes and see if we already have a key or not
    for new_attr in attributes {
        map.insert(new_attr.key.clone(), new_attr);
    }
    map.values().cloned().collect()
}

#[cfg(not(feature = "pyo3"))]
pub trait PythonProcessable {
    fn process(self, processor: &str) -> Self;
}

#[cfg(not(feature = "pyo3"))]
impl PythonProcessable for opentelemetry_proto::tonic::trace::v1::ResourceSpans {
    fn process(self, _processor: &str) -> Self {
        // Noop
        self
    }
}

#[cfg(not(feature = "pyo3"))]
impl PythonProcessable for opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
    fn process(self, _processor: &str) -> Self {
        // Noop
        self
    }
}

#[cfg(not(feature = "pyo3"))]
impl PythonProcessable for opentelemetry_proto::tonic::logs::v1::ResourceLogs {
    fn process(self, _processor: &str) -> Self {
        // Noop
        self
    }
}

impl<T> Pipeline<T>
where
    T: BatchSizer + BatchSplittable + PythonProcessable + ResourceAttributeSettable + Clone,
    Vec<T>: Send,
{
    pub fn new(
        receiver: BoundedReceiver<Message<T>>,
        fanout: Fanout<Vec<T>>,
        flush_listener: Option<FlushReceiver>,
        batch_config: BatchConfig,
        processors: Vec<String>,
        attributes: Vec<(String, String)>,
    ) -> Self {
        let resource_attributes = attributes
            .iter()
            .map(|a| KeyValue {
                key: a.0.clone(),
                value: Some(AnyValue {
                    value: Some(StringValue(a.1.clone())),
                }),
            })
            .collect();

        Self {
            receiver,
            fanout,
            flush_listener,
            batch_config,
            processors,
            resource_attributes,
        }
    }

    pub async fn start(
        &mut self,
        inspector: impl Inspect<T>,
        pipeline_token: CancellationToken,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let res = self.run(inspector, pipeline_token).await;
        if let Err(e) = res {
            error!(error = e, "Pipeline returned from run loop with error");
        }
        Ok(())
    }

    #[cfg(feature = "pyo3")]
    fn initialize_processors(&mut self) -> Result<Vec<String>, BoxError> {
        let mut processor_modules = vec![];
        let current_dir = env::current_dir()?;
        for (processor_idx, file) in self.processors.iter().enumerate() {
            let file_path = Path::new(file);

            // Use absolute path if provided, otherwise make relative to current directory
            let script_path = if file_path.is_absolute() {
                file_path.to_path_buf()
            } else {
                current_dir.join(file_path)
            };
            let code = std::fs::read_to_string(&script_path)?;
            let module = format!("rotel_processor_{}", processor_idx);
            register_processor(code, file.clone(), module.clone())?;
            processor_modules.push(module);
        }
        Ok(processor_modules)
    }

    async fn run(
        &mut self,
        inspector: impl Inspect<T>,
        pipeline_token: CancellationToken,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let mut batch = NestedBatch::<Message<T>>::new(
            self.batch_config.max_size,
            self.batch_config.timeout,
            self.batch_config.disabled,
        );

        let mut batch_timeout = batch.get_timeout();
        if batch_timeout.is_zero() {
            // Disable the timer in this case because we are manually driven, we set a
            // large value and then reset it anytime a manual flush occurs
            batch_timeout = Duration::from_secs(3600 * 24);
        }
        let mut batch_timer = tokio::time::interval(batch_timeout);
        batch_timer.tick().await; // consume the immediate tick

        #[cfg(feature = "pyo3")]
        let processor_modules = self.initialize_processors()?;
        #[cfg(not(feature = "pyo3"))]
        let processor_modules: Vec<String> = vec![];

        let len_processor_modules = processor_modules.len();
        let mut flush_listener = self.flush_listener.take();

        let mut send_fut: Option<FanoutFuture<Vec<T>>> = None;

        loop {
            select! {
                biased;

                // if there's a pending send future, wait on that first
                Some(resp) = conditional_wait(&mut send_fut), if send_fut.is_some() => match resp {
                    Ok(_) => send_fut = None,
                    Err(e) => {
                        error!(error = ?e, "Unable to send item, exiting.");
                        return Err(format!("Pipeline was unable to send downstream: {}", e).into())
                    }
                },

                // flush batches as they time out
                _ = batch_timer.tick(), if send_fut.is_none() => {
                    if batch.should_flush(Instant::now()) {
                        let messages = batch.take_batch();
                        // Extract payloads from messages for now (future: send full Message<T>)
                        let to_send: Vec<T> = messages.into_iter()
                            .flat_map(|msg| msg.payload)
                            .collect();
                        debug!(batch_size = to_send.len(), "Flushing a batch in timout handler");

                        let fut = self.fanout.send_async(to_send);
                        send_fut = Some(fut);
                    }
                },

                // read another incoming request if we don't have a pending batch to flush
                item = self.receiver.next(), if send_fut.is_none() => {
                    if item.is_none() {
                        debug!("Pipeline receiver has closed, flushing batch and exiting");

                        let remain_messages = batch.take_batch();
                        if remain_messages.is_empty() {
                            return Ok(());
                        }

                        // Extract payloads from messages for now (future: send full Message<T>)
                        let remain_batch: Vec<T> = remain_messages.into_iter()
                            .flat_map(|msg| msg.payload)
                            .collect();

                        // We are exiting, so we just want to log the failure to send
                        if let Err(e) = self.fanout.send_async(remain_batch).await {
                            error!(error = ?e, "Unable to send item while exiting, will drop data.");
                        }

                        return Ok(());
                    }

                    let message = item.unwrap();
                    let mut items = message.payload;

                    // invoke current middleware layer
                    // todo: expand support for observability or transforms
                    if len_processor_modules > 0 {
                        inspector.inspect_with_prefix(Some("OTLP payload before processing".into()), &items);
                    } else {
                        inspector.inspect(&items);
                    }
                    // If any resource attributes were provided on start, set or append them to the resources
                    if !self.resource_attributes.is_empty() {
                        for item in &mut items {
                            item.set_or_append_attributes(self.resource_attributes.clone())
                        }
                    }
                    for p in &processor_modules {
                       let mut new_items = Vec::new();
                       while !items.is_empty() {
                           let item = items.pop();
                           if item.is_some() {
                                let result = item.unwrap().process(p);
                                new_items.push(result);
                           }
                       }
                       items = new_items;
                    }

                    if len_processor_modules > 0 {
                        inspector.inspect_with_prefix(Some("OTLP payload after processing".into()), &items);
                    }

                    // Wrap the processed items back into a Message
                    let processed_message = Message {
                        metadata: message.metadata,
                        payload: items,
                    };

                    match batch.offer(vec![processed_message]) {
                        Ok(Some(popped)) => {
                            // Extract payloads from messages for now (future: send full Message<T>)
                            let to_send: Vec<T> = popped.into_iter()
                                .flat_map(|msg| msg.payload)
                                .collect();
                            let fut = self.fanout.send_async(to_send);
                            send_fut = Some(fut);
                        }
                        Ok(None) => {},
                        Err(_) => {
                            error!("Too many batch items split, dropping data. Consider increasing the batch size.")
                        }
                    }
                },

                Some(resp) = conditional_flush(&mut flush_listener) => {
                    match resp {
                        (Some(req), listener) => {
                            debug!("received force flush in pipeline: {:?}", req);
                            batch_timer.reset();

                            // Flush pending future if it exists
                            if let Some(fut) = send_fut.take() {
                                debug!("Flushing pending send on flush message");

                                if let Err(e) = fut.await {
                                    error!(error = ?e, "Unable to send item, exiting.");
                                    return Err(format!("Pipeline was unable to send downstream: {}", e).into())
                                }
                            }

                            let messages = batch.take_batch();
                            if !messages.is_empty() {
                                // Extract payloads from messages for now (future: send full Message<T>)
                                let to_send: Vec<T> = messages.into_iter()
                                    .flat_map(|msg| msg.payload)
                                    .collect();
                                debug!(batch_size = to_send.len(), "Flushing a batch on flush message");

                                if let Err(e) = self.fanout.send_async(to_send).await {
                                    error!(error = ?e, "Unable to send item, exiting.");
                                    return Err(format!("Pipeline was unable to send downstream: {}", e).into())
                                }
                            }

                            if let Err(e) = listener.ack(req).await {
                                warn!("unable to ack flush request: {}", e);
                            }
                        },
                        (None, _) => warn!("flush channel was closed")
                    }
                },

                _ = pipeline_token.cancelled() => {
                    debug!("Pipeline received shutdown signal, exiting main pipeline loop");

                    return Ok(())
                }
            }
        }
    }
}

pub async fn conditional_wait<T>(
    send_fut: &mut Option<crate::topology::fanout::FanoutFuture<'_, T>>,
) -> Option<Result<(), crate::topology::fanout::FanoutError>>
where
    T: Clone,
{
    match send_fut {
        None => None,
        Some(fut) => Some(fut.await),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utilities::otlp::FakeOTLP;

    #[test]
    fn test_append_attributes() {
        let new_attrs = vec![KeyValue {
            key: "new.attr.key".to_string(),
            value: Some(AnyValue {
                value: Some(StringValue("new_attr_value".to_string())),
            }),
        }];

        let mut trace_request = FakeOTLP::trace_service_request_with_spans(1, 1);
        let mut spans = trace_request.resource_spans.pop().unwrap();
        spans.set_or_append_attributes(new_attrs.clone());
        verify_attr_appended(spans.resource.unwrap().attributes, 7, 6);

        let mut logs_request = FakeOTLP::logs_service_request_with_logs(1, 1);
        let mut logs = logs_request.resource_logs.pop().unwrap();
        logs.set_or_append_attributes(new_attrs.clone());
        verify_attr_appended(logs.resource.unwrap().attributes, 7, 6);

        let mut metrics_request = FakeOTLP::metrics_service_request_with_metrics(1, 1);
        let mut metrics = metrics_request.resource_metrics.pop().unwrap();
        metrics.set_or_append_attributes(new_attrs);
        verify_attr_appended(metrics.resource.unwrap().attributes, 7, 6);
    }

    #[test]
    fn test_append_attributes_no_resource() {
        let new_attrs = vec![KeyValue {
            key: "new.attr.key".to_string(),
            value: Some(AnyValue {
                value: Some(StringValue("new_attr_value".to_string())),
            }),
        }];

        let mut trace_request = FakeOTLP::trace_service_request_with_spans(1, 1);
        let mut spans = trace_request.resource_spans.pop().unwrap();
        spans.resource = None;
        spans.set_or_append_attributes(new_attrs.clone());
        verify_attr_appended(spans.resource.unwrap().attributes, 1, 0);

        let mut logs_request = FakeOTLP::logs_service_request_with_logs(1, 1);
        let mut logs = logs_request.resource_logs.pop().unwrap();
        logs.resource = None;
        logs.set_or_append_attributes(new_attrs.clone());
        verify_attr_appended(logs.resource.unwrap().attributes, 1, 0);

        let mut metrics_request = FakeOTLP::metrics_service_request_with_metrics(1, 1);
        let mut metrics = metrics_request.resource_metrics.pop().unwrap();
        metrics.resource = None;
        metrics.set_or_append_attributes(new_attrs);
        verify_attr_appended(metrics.resource.unwrap().attributes, 1, 0);
    }

    fn verify_attr_appended(mut attrs: Vec<KeyValue>, len: usize, idx: usize) {
        assert_eq!(len, attrs.len());
        let new_attr = attrs.remove(idx);
        assert_eq!("new.attr.key", new_attr.key);
        match new_attr.value.unwrap().value.unwrap() {
            StringValue(v) => {
                assert_eq!("new_attr_value", v);
            }
            _ => {
                panic!("unexpected type for attribute value")
            }
        }
    }

    #[test]
    fn test_update_attributes() {
        let new_attrs = vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(StringValue("overwritten_service_name".to_string())),
            }),
        }];

        let mut trace_request = FakeOTLP::trace_service_request_with_spans(1, 1);
        let mut spans = trace_request.resource_spans.pop().unwrap();
        spans.set_or_append_attributes(new_attrs.clone());
        verify_attr_updated(spans.resource.unwrap().attributes);

        let mut logs_request = FakeOTLP::logs_service_request_with_logs(1, 1);
        let mut logs = logs_request.resource_logs.pop().unwrap();
        logs.set_or_append_attributes(new_attrs.clone());
        verify_attr_updated(logs.resource.unwrap().attributes);

        let mut metrics_request = FakeOTLP::metrics_service_request_with_metrics(1, 1);
        let mut metrics = metrics_request.resource_metrics.pop().unwrap();
        metrics.set_or_append_attributes(new_attrs.clone());
        verify_attr_updated(metrics.resource.unwrap().attributes);
    }

    fn verify_attr_updated(mut attrs: Vec<KeyValue>) {
        assert_eq!(6, attrs.len());
        let new_attr = attrs.remove(0);
        assert_eq!("service.name", new_attr.key);
        match new_attr.value.unwrap().value.unwrap() {
            StringValue(v) => {
                assert_eq!("overwritten_service_name", v);
            }
            _ => {
                panic!("unexpected type for attribute value")
            }
        }
    }
}

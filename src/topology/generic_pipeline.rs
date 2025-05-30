// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::{BoundedReceiver, BoundedSender};
use crate::topology::batch::{BatchConfig, BatchSizer, BatchSplittable, NestedBatch};
use crate::topology::flush_control::{conditional_flush, FlushReceiver};
use flume::r#async::SendFut;
use flume::SendError;
use opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::{ResourceMetrics, ScopeMetrics};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans};
#[cfg(feature = "pyo3")]
use rotel_sdk::model::{register_processor, PythonProcessable};
#[cfg(feature = "pyo3")]
use std::env;
use std::error::Error;
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
    receiver: BoundedReceiver<Vec<T>>,
    sender: BoundedSender<Vec<T>>,
    batch_config: BatchConfig,
    processors: Vec<String>,
    flush_listener: Option<FlushReceiver>,
    resource_attributes: Vec<KeyValue>,
}

pub trait Inspect<T> {
    fn inspect(&self, value: &[T]);
}

pub trait ResourceAttributeSettable {
    fn set_or_append_attributes(&mut self, attributes: Vec<KeyValue>);
}

impl ResourceAttributeSettable for ResourceSpans {
    fn set_or_append_attributes(&mut self, attributes: Vec<KeyValue>) {
        let mut drop_count = 0;
        if let Some(rs) = &self.resource {
            drop_count = rs.dropped_attributes_count;
        }
        self.resource = Some(Resource {
            attributes: build_attrs(&self.resource, attributes),
            dropped_attributes_count: drop_count,
        });
    }
}

impl ResourceAttributeSettable for ResourceMetrics {
    fn set_or_append_attributes(&mut self, attributes: Vec<KeyValue>) {
        let mut drop_count = 0;
        if let Some(rs) = &self.resource {
            drop_count = rs.dropped_attributes_count;
        }
        self.resource = Some(Resource {
            attributes: build_attrs(&self.resource, attributes),
            dropped_attributes_count: drop_count,
        });
    }
}

impl ResourceAttributeSettable for ResourceLogs {
    fn set_or_append_attributes(&mut self, attributes: Vec<KeyValue>) {
        let mut drop_count = 0;
        if let Some(rs) = &self.resource {
            drop_count = rs.dropped_attributes_count;
        }
        self.resource = Some(Resource {
            attributes: build_attrs(&self.resource, attributes),
            dropped_attributes_count: drop_count,
        });
    }
}

pub fn build_attrs(resource: &Option<Resource>, attributes: Vec<KeyValue>) -> Vec<KeyValue> {
    if resource.is_none() {
        return attributes;
    }
    // Let's retain the order and create a map of keys to reduce the number of iterations required to find/replace an existing key
    let mut map = indexmap::IndexMap::<String, KeyValue>::new();
    // Yes there is only one, but we've already determined that we have one.
    for res in resource.iter() {
        for attr in res.attributes.iter() {
            map.insert(attr.key.clone(), attr.clone());
        }
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
    T: BatchSizer + BatchSplittable + PythonProcessable + ResourceAttributeSettable,
    Vec<T>: Send,
{
    pub fn new(
        receiver: BoundedReceiver<Vec<T>>,
        sender: BoundedSender<Vec<T>>,
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
            sender,
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
        let mut batch = NestedBatch::<T>::new(
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

        let mut flush_listener = self.flush_listener.take();

        let mut send_fut: Option<SendFut<Vec<T>>> = None;

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
                        let to_send = batch.take_batch();
                        debug!(batch_size = to_send.len(), "Flushing a batch in timout handler");

                        let fut = self.sender.send_async(to_send);
                        send_fut = Some(fut);
                    }
                },

                // read another incoming request if we don't have a pending batch to flush
                item = self.receiver.next(), if send_fut.is_none() => {
                    if item.is_none() {
                        debug!("Pipeline receiver has closed, flushing batch and exiting");

                        let remain_batch = batch.take_batch();
                        if remain_batch.is_empty() {
                            return Ok(());
                        }

                        // We are exiting, so we just want to log the failure to send
                        if let Err(e) = self.sender.send_async(remain_batch).await {
                            error!(error = ?e, "Unable to send item while exiting, will drop data.");
                        }

                        return Ok(());
                    }

                    let mut items = item.unwrap();
                    // invoke current middleware layer
                    // todo: expand support for observability or transforms
                    inspector.inspect(&items);
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

                    match batch.offer(items) {
                        Ok(Some(popped)) => {
                            let fut = self.sender.send_async(popped);
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

                            let to_send = batch.take_batch();
                            if !to_send.is_empty() {
                                debug!(batch_size = to_send.len(), "Flushing a batch on flush message");

                                if let Err(e) = self.sender.send_async(to_send).await {
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
    send_fut: &mut Option<SendFut<'_, Vec<T>>>,
) -> Option<Result<(), SendError<Vec<T>>>> {
    match send_fut {
        None => None,
        Some(fut) => Some(fut.await),
    }
}

impl BatchSizer for ResourceSpans {
    fn size_of(&self) -> usize {
        self.scope_spans.iter().map(|sc| sc.spans.len()).sum()
    }
}

impl BatchSizer for [ResourceSpans] {
    fn size_of(&self) -> usize {
        self.iter().map(|n| n.size_of()).sum()
    }
}

impl BatchSplittable for ResourceSpans {
    fn split(&mut self, split_n: usize) -> Self
    where
        Self: Sized,
    {
        let mut count_moved = 0;
        let mut split_scope_spans = vec![];
        while !self.scope_spans.is_empty() && count_moved < split_n {
            // first just check and see if we can hoist all the spans in this scope span
            let span_len = self.scope_spans[0].spans.len();
            if span_len + count_moved <= split_n {
                let v = self.scope_spans.remove(0);
                split_scope_spans.push(v);
                count_moved += span_len;
            } else {
                // We'll need to harvest a subset of the spans
                let mut spans = vec![];
                while !self.scope_spans[0].spans.is_empty() && count_moved < split_n {
                    // Remove the span and add to the new ss
                    let s = self.scope_spans[0].spans.remove(0);
                    spans.push(s);
                    count_moved += 1
                }
                // Now we need to clone data from this ScopeSpan for our new ScopeSpans
                let ss = ScopeSpans {
                    scope: self.scope_spans[0].scope.clone(),
                    schema_url: self.scope_spans[0].schema_url.clone(),
                    spans,
                };
                split_scope_spans.push(ss)
            }
        }
        let mut rs = ResourceSpans::default();
        rs.scope_spans = split_scope_spans;
        rs.schema_url = self.schema_url.clone();
        rs.resource = rs.resource.clone();
        rs
    }
}

impl BatchSizer for ResourceMetrics {
    fn size_of(&self) -> usize {
        self.scope_metrics
            .iter()
            .flat_map(|sc| &sc.metrics)
            .map(|m| match &m.data {
                None => 0,
                Some(d) => match d {
                    Data::Gauge(g) => g.data_points.len(),
                    Data::Sum(s) => s.data_points.len(),
                    Data::Histogram(h) => h.data_points.len(),
                    Data::ExponentialHistogram(e) => e.data_points.len(),
                    Data::Summary(s) => s.data_points.len(),
                },
            })
            .sum()
    }
}

impl BatchSizer for [ResourceMetrics] {
    fn size_of(&self) -> usize {
        self.iter().map(|n| n.size_of()).sum()
    }
}

impl BatchSplittable for ResourceMetrics {
    fn split(&mut self, split_n: usize) -> Self
    where
        Self: Sized,
    {
        let mut count_moved = 0;
        let mut split_scope_metrics = vec![];
        while !self.scope_metrics.is_empty() && count_moved < split_n {
            // first just check and see if we can hoist all the spans in this scope span
            let metric_len = self.scope_metrics[0].metrics.len();
            if metric_len + count_moved <= split_n {
                let v = self.scope_metrics.remove(0);
                split_scope_metrics.push(v);
                count_moved += metric_len;
            } else {
                // We'll need to harvest a subset of the spans
                let mut metrics = vec![];
                while !self.scope_metrics[0].metrics.is_empty() && count_moved < split_n {
                    // Remove the span and add to the new ss
                    let s = self.scope_metrics[0].metrics.remove(0);
                    metrics.push(s);
                    count_moved += 1
                }
                // Now we need to clone data from this ScopeSpan for our new ScopeSpans
                let sm = ScopeMetrics {
                    scope: self.scope_metrics[0].scope.clone(),
                    schema_url: self.scope_metrics[0].schema_url.clone(),
                    metrics,
                };
                split_scope_metrics.push(sm)
            }
        }
        let mut rs = ResourceMetrics::default();
        rs.scope_metrics = split_scope_metrics;
        rs.schema_url = self.schema_url.clone();
        rs.resource = rs.resource.clone();
        rs
    }
}

impl BatchSizer for ResourceLogs {
    fn size_of(&self) -> usize {
        self.scope_logs.iter().map(|sc| sc.log_records.len()).sum()
    }
}

impl BatchSizer for [ResourceLogs] {
    fn size_of(&self) -> usize {
        self.iter().map(|n| n.size_of()).sum()
    }
}

impl BatchSplittable for ResourceLogs {
    fn split(&mut self, split_n: usize) -> Self
    where
        Self: Sized,
    {
        let mut count_moved = 0;
        let mut split_scope_logs = vec![];
        while !self.scope_logs.is_empty() && count_moved < split_n {
            // first just check and see if we can hoist all the spans in this scope span
            let span_len = self.scope_logs[0].log_records.len();
            if span_len + count_moved <= split_n {
                let v = self.scope_logs.remove(0);
                split_scope_logs.push(v);
                count_moved += span_len;
            } else {
                // We'll need to harvest a subset of the spans
                let mut log_records = vec![];
                while !self.scope_logs[0].log_records.is_empty() && count_moved < split_n {
                    // Remove the span and add to the new ss
                    let s = self.scope_logs[0].log_records.remove(0);
                    log_records.push(s);
                    count_moved += 1
                }
                // Now we need to clone data from this ScopeSpan for our new ScopeSpans
                let sl = ScopeLogs {
                    scope: self.scope_logs[0].scope.clone(),
                    schema_url: self.scope_logs[0].schema_url.clone(),
                    log_records,
                };
                split_scope_logs.push(sl)
            }
        }
        let mut rs = ResourceLogs::default();
        rs.scope_logs = split_scope_logs;
        rs.schema_url = self.schema_url.clone();
        rs.resource = rs.resource.clone();
        rs
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

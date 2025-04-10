// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::{BoundedReceiver, BoundedSender};
#[cfg(feature = "pyo3")]
use crate::processor::model::register_processor;
#[cfg(feature = "pyo3")]
use crate::processor::py::rotel_python_processor_sdk;
use crate::topology::batch::{BatchConfig, BatchSizer, BatchSplittable, NestedBatch};
use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::{ResourceMetrics, ScopeMetrics};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans};
#[cfg(feature = "pyo3")]
use std::env;
use std::error::Error;
#[cfg(feature = "pyo3")]
use std::sync::Once;
use tokio::select;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "pyo3")]
use tower::BoxError;
use tracing::{debug, error};

#[derive(Clone)]
#[allow(dead_code)] // for the sake of the pyo3 feature
pub struct Pipeline<T> {
    receiver: BoundedReceiver<Vec<T>>,
    sender: BoundedSender<Vec<T>>,
    batch_config: BatchConfig,
    processors: Vec<String>,
}

pub trait Inspect<T> {
    fn inspect(&self, value: &[T]);
}

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

#[cfg(feature = "pyo3")]
static PROCESSOR_INIT: Once = Once::new();

impl<T> Pipeline<T>
where
    T: BatchSizer + BatchSplittable + PythonProcessable,
    Vec<T>: Send,
{
    pub fn new(
        receiver: BoundedReceiver<Vec<T>>,
        sender: BoundedSender<Vec<T>>,
        batch_config: BatchConfig,
        processors: Vec<String>,
    ) -> Self {
        Self {
            receiver,
            sender,
            batch_config,
            processors,
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
        let path = env::current_dir()?;
        if !self.processors.is_empty() {
            PROCESSOR_INIT.call_once(|| {
                pyo3::append_to_inittab!(rotel_python_processor_sdk);
                pyo3::prepare_freethreaded_python();
            });
        }
        let processor_idx = 0;
        for file in &self.processors {
            let script = format!("{}/{}", path.clone().to_str().unwrap(), file);
            let code = std::fs::read_to_string(script)?;
            let module = format!("rotel_processor_{}", processor_idx);
            register_processor(code, file.clone(), module.clone())?;
            processor_modules.push(module)
        }
        Ok(processor_modules)
    }

    async fn run(
        &mut self,
        inspector: impl Inspect<T>,
        pipeline_token: CancellationToken,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let mut batch =
            NestedBatch::<T>::new(self.batch_config.max_size, self.batch_config.timeout);
        let mut batch_timer = tokio::time::interval(batch.get_timeout());

        #[cfg(feature = "pyo3")]
        let processor_modules = self.initialize_processors()?;
        #[cfg(not(feature = "pyo3"))]
        let processor_modules: Vec<String> = vec![];

        loop {
            select! {
                biased;

                // flush batches as they time out
                _ = batch_timer.tick() => {
                    if batch.should_flush(Instant::now()) {
                        let to_send = batch.take_batch();
                        debug!(batch_size = to_send.len(), "Flushing a batch in timout handler");

                        if let Err(e) = self.send_item(to_send, &pipeline_token).await {
                            return match e {
                                SendItemError::Cancelled => {
                                    debug!("Pipeline received shutdown signal, exiting main pipeline loop.");
                                    Ok(())
                                }
                                SendItemError::Error(e) => {
                                    error!(error = ?e, "Unable to send item, exiting.");
                                    Ok(())
                                }
                            }
                        }
                    }
                },

                // read another incoming request if we don't have a pending batch to flush
                item = self.receiver.next() => {
                    if item.is_none() {
                        debug!("Pipeline receiver has closed, flushing batch and exiting");

                        let remain_batch = batch.take_batch();
                        if remain_batch.is_empty() {
                            return Ok(());
                        }

                        // We are exiting, so we just want to log the failure to send
                        if let Err(SendItemError::Error(e)) = self.send_item(remain_batch, &pipeline_token).await {
                            error!(error = ?e, "Unable to send item while exiting, will drop data.");
                        }

                        return Ok(());
                    }

                    let mut items = item.unwrap();
                    // invoke current middleware layer
                    // todo: expand support for observability or transforms
                    inspector.inspect(&items);
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
                    let maybe_popped = batch.offer(items);
                    if let Ok(Some(popped)) = maybe_popped { // todo: handle error?
                        if let Err(e) = self.send_item(popped, &pipeline_token).await {
                            return match e {
                                SendItemError::Cancelled => {
                                    debug!("Pipeline received shutdown signal, exiting main pipeline loop");
                                    Ok(())
                                }
                                SendItemError::Error(e) => {
                                    error!(error = ?e, "Unable to send item, exiting.");
                                    Ok(())
                                }
                            }
                        }
                    }
                },

                _ = pipeline_token.cancelled() => {
                    debug!("Pipeline received shutdown signal, exiting main pipeline loop");

                    return Ok(())
                }
            }
        }
    }

    // send item to the downstream channel
    async fn send_item(
        &self,
        item: Vec<T>,
        cancel: &CancellationToken,
    ) -> Result<(), SendItemError<T>> {
        select! {
            res = self.sender.send_async(item) => {
                match res {
                    Ok(_) => Ok(()),
                    Err(e) => Err(SendItemError::Error(e))
                }
            },
            _ = cancel.cancelled() => {
                Err(SendItemError::Cancelled)
            }
        }
    }
}

enum SendItemError<T> {
    Cancelled,
    Error(flume::SendError<Vec<T>>),
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

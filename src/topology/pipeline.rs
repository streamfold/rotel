// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::{BoundedReceiver, BoundedSender};
use crate::topology::batch::{BatchConfig, BatchSizer, NestedBatch};
use crate::topology::payload::OTLPPayload;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::error::Error;
use tokio::select;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{event, warn, Level};

pub struct Pipeline {
    receiver: BoundedReceiver<OTLPPayload>,
    sender: BoundedSender<OTLPPayload>,
    batch_config: BatchConfig,
}

pub trait Inspect {
    fn inspect(&self, value: &[ResourceSpans]);
}

impl Pipeline {
    pub fn new(receiver: BoundedReceiver<OTLPPayload>, sender: BoundedSender<OTLPPayload>) -> Self {
        Self {
            receiver,
            sender,
            batch_config: BatchConfig::default(), // todo: make configurable
        }
    }

    pub async fn start(
        &mut self,
        inspector: impl Inspect,
        pipeline_token: CancellationToken,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let res = self.run(inspector, pipeline_token).await;
        match res {
            Ok(()) => {
                event!(Level::INFO, "Pipeline returned from run loop successfully");
            }
            Err(e) => {
                event!(
                    Level::ERROR,
                    error = e,
                    "Pipeline returned from run loop with error."
                );
            }
        }
        Ok(())
    }

    async fn run(
        &mut self,
        _inspector: impl Inspect,
        pipeline_token: CancellationToken,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let mut traces_batch = NestedBatch::<ResourceSpans>::new(
            self.batch_config.max_size,
            self.batch_config.timeout,
        );

        let mut metrics_batch = NestedBatch::<ResourceMetrics>::new(
            self.batch_config.max_size,
            self.batch_config.timeout,
        );

        let mut batch_timer = tokio::time::interval(self.batch_config.timeout);

        loop {
            select! {
                biased;
                // flush batches as they time out
                _ = batch_timer.tick() => {
                    // TODO: Code smell, this could be further tried up into iteration of some generic batchers, but for now we're just going to do it here
                    let now = Instant::now();
                    if traces_batch.should_flush(now) {
                        let to_send = traces_batch.take_batch();
                        event!(Level::INFO, batch_size = to_send.size_of(), "Flushing a batch in timout handler");
                        if let Err(e) = self.send_item(From::from(to_send), &pipeline_token).await {
                            return handle_send_error(&e);
                        }
                    }
                     if metrics_batch.should_flush(now) {
                        let to_send = metrics_batch.take_batch();
                        event!(Level::INFO, batch_size = to_send.size_of(), "Flushing a batch in timout handler");
                        if let Err(e) = self.send_item(From::from(to_send), &pipeline_token).await {
                            return handle_send_error(&e);
                        }
                    }
                },

                // read another incoming request if we don't have a pending batch to flush
                item = self.receiver.next() => {
                    if item.is_none() {
                        warn!("Pipeline receiver found None on call to .next(), exiting main loop.");
                        return Ok(());
                    }

                    let item = item.unwrap();
                    //inspector.inspect(&item);
                    match item {
                        OTLPPayload::Traces(t) => {
                            let maybe_popped = traces_batch.offer(t);
                            if let Ok(Some(popped)) = maybe_popped { // todo: handle error?
                                if let Err(e) = self.send_item(From::from(popped), &pipeline_token).await {
                                    return handle_send_error(&e)
                                }
                            }
                        },
                        OTLPPayload::Metrics(metrics) => {
                           let maybe_popped = metrics_batch.offer(metrics);
                            if let Ok(Some(popped)) = maybe_popped { // todo: handle error?
                                if let Err(e) = self.send_item(From::from(popped), &pipeline_token).await {
                                    return handle_send_error(&e)
                                }
                            }
                        }
                    }
                },

                _ = pipeline_token.cancelled() => {
                    event!(Level::INFO, "Pipeline received shutdown signal, exiting main pipeline loop.");

                    return Ok(())
                }
            }
        }
    }

    // send item to the downstream channel
    async fn send_item(
        &self,
        item: OTLPPayload,
        cancel: &CancellationToken,
    ) -> Result<(), SendItemError<OTLPPayload>> {
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

fn handle_send_error(e: &SendItemError<OTLPPayload>) -> Result<(), Box<dyn Error + Send + Sync>> {
    match e {
        SendItemError::Cancelled::<OTLPPayload> => {
            event!(
                Level::INFO,
                "Pipeline received shutdown signal, exiting main pipeline loop"
            );
            Ok(())
        }
        SendItemError::Error(e) => {
            event!(Level::ERROR, error = ?e, "unable to send item, exiting");
            Ok(())
        }
    }
}

enum SendItemError<T> {
    Cancelled,
    Error(flume::SendError<T>),
}

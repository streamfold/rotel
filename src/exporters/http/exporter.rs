use super::acknowledger::Acknowledger;
use super::response::Response;
use crate::topology::flush_control::{FlushReceiver, conditional_flush};
use futures_util::stream::FuturesUnordered;
use futures_util::{Stream, StreamExt, poll};
use http::Request;
use opentelemetry::{KeyValue, global};
use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Add;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;
use tokio::select;
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::time::{Instant, interval, timeout_at};
use tokio_util::sync::CancellationToken;
use tower::BoxError;
use tower::Service;
use tracing::{debug, error, info, warn};

use super::finalizer::ResultFinalizer;

pub(crate) const MAX_CONCURRENT_REQUESTS: usize = 10;

type ExportFuture<Future> = Pin<Box<Future>>;

#[allow(dead_code)] // used in tracing outputs
#[derive(Clone, Debug)]
struct Meta {
    exporter_name: String,
    telemetry_type: String,
}

pub struct Exporter<InStr, Svc, Payload, Finalizer, Ack> {
    meta: Meta,
    input: Pin<Box<InStr>>,
    svc: Svc,
    result_finalizer: Finalizer,
    acknowledger: Ack,
    flush_receiver: Option<FlushReceiver>,
    retry_broadcast: BroadcastSender<bool>,
    encode_drain_max_time: Duration,
    export_drain_max_time: Duration,
    max_concurrent_requests: usize,
    _phantom: PhantomData<Payload>,
}

impl<InStr, Svc, Payload, Finalizer, Ack> Exporter<InStr, Svc, Payload, Finalizer, Ack> {
    pub fn new(
        exporter_name: &'static str,
        telemetry_type: &'static str,
        input: InStr,
        svc: Svc,
        result_finalizer: Finalizer,
        acknowledger: Ack,
        flush_receiver: Option<FlushReceiver>,
        retry_broadcast: BroadcastSender<bool>,
        encode_drain_max_time: Duration,
        export_drain_max_time: Duration,
    ) -> Self {
        let max_concurrent_requests = std::env::var("ROTEL_MAX_CONCURRENT_REQUESTS")
            .ok()
            .and_then(|s| if s.is_empty() { None } else { Some(s) })
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(MAX_CONCURRENT_REQUESTS);

        Self {
            meta: Meta {
                exporter_name: exporter_name.to_string(),
                telemetry_type: telemetry_type.to_string(),
            },
            input: Box::pin(input),
            svc,
            result_finalizer,
            acknowledger,
            flush_receiver,
            retry_broadcast,
            encode_drain_max_time,
            export_drain_max_time,
            max_concurrent_requests,
            _phantom: PhantomData,
        }
    }
}

impl<InStr, Svc, Payload, Finalizer, Ack, T> Exporter<InStr, Svc, Payload, Finalizer, Ack>
where
    InStr: Stream<Item = Result<Request<Payload>, BoxError>>,
    Svc: Service<Request<Payload>, Response = Response<T>>,
    <Svc as Service<Request<Payload>>>::Error: Debug,
    T: Debug + Send + Sync,
    Finalizer: ResultFinalizer<Result<Response<T>, <Svc as Service<Request<Payload>>>::Error>>,
    Ack: Acknowledger<T, Error = <Svc as Service<Request<Payload>>>::Error>,
    Payload: Clone,
{
    pub async fn start(mut self, token: CancellationToken) -> Result<(), BoxError> {
        let meta = self.meta.clone();

        let mut export_futures: FuturesUnordered<
            ExportFuture<<Svc as Service<Request<Payload>>>::Future>,
        > = FuturesUnordered::new();
        let mut flush_receiver = self.flush_receiver.take();

        // Atomic counter for tracking export futures size
        let encode_futures_count = Arc::new(AtomicUsize::new(0));
        let send_futures_count = Arc::new(AtomicUsize::new(0));

        let meta_gauge = meta.clone();
        let encode_futures_count_clone = encode_futures_count.clone();
        let send_futures_count_clone = send_futures_count.clone();
        let _ = global::meter("exporters")
            .i64_observable_gauge("task_queues")
            .with_callback(move |observer| {
                let telemetry_type_kv =
                    KeyValue::new("telemetry_type", meta_gauge.telemetry_type.clone());
                let exporter_type_kv =
                    KeyValue::new("exporter_type", meta_gauge.exporter_name.clone());

                let task_type = KeyValue::new("task_type", "encoding");
                observer.observe(
                    encode_futures_count_clone.load(Ordering::Relaxed) as i64,
                    &[
                        telemetry_type_kv.clone(),
                        exporter_type_kv.clone(),
                        task_type,
                    ],
                );

                let task_type = KeyValue::new("task_type", "sending");
                observer.observe(
                    send_futures_count_clone.load(Ordering::Relaxed) as i64,
                    &[telemetry_type_kv, exporter_type_kv, task_type],
                );
            })
            .build();

        // Create a timer that fires every 1 second to record stats
        let mut stats_timer = interval(Duration::from_secs(1));

        loop {
            select! {
                biased;

                // Record futures count periodically
                _ = stats_timer.tick() => {
                    encode_futures_count.store(self.input.size_hint().0, Ordering::Relaxed);
                    send_futures_count.store(export_futures.len(), Ordering::Relaxed);
                }

                Some(resp) = export_futures.next() => {
                    match resp {
                        Ok(response) => {
                            // First, run the acknowledger with a reference (no cloning)
                            self.acknowledger.acknowledge(&response).await;
                            // Then finalize, giving ownership to the finalizer
                            match self.result_finalizer.finalize(Ok(response)) {
                                Err(e) => {
                                    error!(error = ?e, ?meta, "Finalization failed after acknowledgment.")
                                },
                                Ok(rs) => {
                                    debug!(rs = ?rs, futures_size = export_futures.len(), ?meta, "Exporter sent response");
                                }
                            }
                        },
                        Err(e) => {
                            // Handle error acknowledgment (potentially sending nacks)
                            self.acknowledger.handle_error(&e).await;

                            // Then pass to finalizer
                            match self.result_finalizer.finalize(Err(e)) {
                                Err(e) => {
                                    error!(error = ?e, ?meta, "Exporting failed, dropping data.")
                                },
                                Ok(rs) => {
                                    debug!(rs = ?rs, futures_size = export_futures.len(), ?meta, "Exporter handled error");
                                }
                            }
                        }
                    }
                },

                input = self.input.next(), if export_futures.len() < self.max_concurrent_requests => {
                    match input {
                        None => {
                            debug!(?meta, "Exporter received end of input, exiting.");
                            break
                        },
                        Some(req) => match req {
                            Ok(req) => export_futures.push(Box::pin(self.svc.call(req))),
                            Err(e) => {
                                error!(error = ?e, ?meta, "Failed to encode request, dropping.");
                            }
                        }
                    }
                },

                Some(resp) = conditional_flush(&mut flush_receiver) => {
                    match resp {
                        (Some(req), listener) => {
                            debug!(?meta, request = ?req, "Received force flush in exporter");

                            if let Err(res) = self.drain_futures(&mut export_futures, true).await {
                                warn!(?meta, result = res, "Unable to drain exporter");
                            }

                            if let Err(e) = listener.ack(req).await {
                                warn!(?meta, error = e, "Unable to ack flush request");
                            }
                        },
                        (None, _) => warn!(?meta, "Flush channel was closed")
                    }
                },

                _ = token.cancelled() => {
                    info!(?meta, "Exporter received shutdown signal, exiting main processing loop");
                    break;
                },
            }
        }

        self.drain_futures(&mut export_futures, false).await
    }

    async fn drain_futures(
        &mut self,
        export_futures: &mut FuturesUnordered<
            ExportFuture<<Svc as Service<Request<Payload>>>::Future>,
        >,
        non_blocking: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let finish_encoding = Instant::now().add(self.encode_drain_max_time);
        let finish_sending = Instant::now().add(self.export_drain_max_time);

        // If non-blocking, we must poll at least once to force any messages into the encoding futures
        // list. This allows us to do the size check later, knowing that if there was a pending msg
        // it would have been added to the encoding futures.
        if non_blocking {
            match poll!(self.input.next()) {
                Poll::Ready(None) => {
                    return self.drain_exports(finish_sending, export_futures).await;
                }
                Poll::Ready(Some(res)) => match res {
                    Ok(req) => export_futures.push(Box::pin(self.svc.call(req))),
                    Err(e) => {
                        error!(error = ?e, meta = ?self.meta, "Failed to encode request, dropping.");
                    }
                },
                _ => {}
            }
        }

        // First we must wait on currently encoding futures
        loop {
            // If we are non-blocking, then we only block up to the timeout if there are
            // encoding futures pending. Use the size hint on the stream to check remaining
            // encoding futures and skip waiting if it is empty.
            if non_blocking {
                let (min_sz, _) = self.input.size_hint();
                if min_sz == 0 {
                    break;
                }
            }

            let poll_res = timeout_at(finish_encoding, self.input.next()).await;
            match poll_res {
                Err(_) => {
                    return Err(format!(
                        "Timed out waiting for requests to encode: {:?}",
                        self.meta
                    )
                    .into());
                }
                Ok(res) => match res {
                    None => break,
                    Some(r) => match r {
                        Ok(req) => export_futures.push(Box::pin(self.svc.call(req))),
                        Err(e) => {
                            error!(error = ?e, meta = ?self.meta, "Failed to encode request, dropping.");
                        }
                    },
                },
            }
        }

        self.drain_exports(finish_sending, export_futures).await
    }

    async fn drain_exports(
        &self,
        finish_sending: Instant,
        export_futures: &mut FuturesUnordered<
            ExportFuture<<Svc as Service<Request<Payload>>>::Future>,
        >,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        // We ignore the response here, there may be no receivers and in that case it returns a SendError
        let _ = self.retry_broadcast.send(true);

        let mut drain_errors = 0;
        loop {
            if export_futures.is_empty() {
                break;
            }

            let poll_res = timeout_at(finish_sending, export_futures.next()).await;
            match poll_res {
                Err(_) => {
                    return Err(format!(
                        "Timed out waiting for requests to finish: {:?}",
                        self.meta
                    )
                    .into());
                }
                Ok(res) => match res {
                    None => {
                        error!(meta = ?self.meta, "None returned while polling futures");
                        break;
                    }
                    Some(r) => {
                        match r {
                            Ok(response) => {
                                // First, acknowledge with a reference
                                self.acknowledger.acknowledge(&response).await;

                                // Then finalize, giving ownership
                                if let Err(e) = self.result_finalizer.finalize(Ok(response)) {
                                    error!(meta = ?self.meta,
                                        error = ?e,
                                        "Finalization failed after acknowledgment during drain."
                                    );
                                    drain_errors += 1;
                                }
                            }
                            Err(e) => {
                                // Handle error acknowledgment (potentially sending nacks)
                                self.acknowledger.handle_error(&e).await;

                                // Then finalize
                                if let Err(e) = self.result_finalizer.finalize(Err(e)) {
                                    error!(meta = ?self.meta,
                                        error = ?e,
                                        "Error from exporter endpoint during drain."
                                    );
                                    drain_errors += 1;
                                }
                            }
                        }
                    }
                },
            }
        }

        if drain_errors > 0 {
            Err(format!(
                "Failed draining export requests, {} requests failed: {:?}",
                drain_errors, self.meta,
            )
            .into())
        } else {
            Ok(())
        }
    }
}

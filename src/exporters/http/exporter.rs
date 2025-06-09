use crate::topology::flush_control::{FlushReceiver, conditional_flush};
use futures_util::stream::FuturesUnordered;
use futures_util::{Stream, StreamExt, poll};
use http::Request;
use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Add;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use tokio::time::{Instant, timeout_at};
use tokio::{pin, select};
use tokio_util::sync::CancellationToken;
use tower::BoxError;
use tower::Service;
use tracing::{debug, error, info, warn};

pub(crate) const MAX_CONCURRENT_REQUESTS: usize = 10;

pub trait ResultLogger<Response> {
    fn handle(&self, resp: Response);
}

type ExportFuture<Future> = Pin<Box<Future>>;

#[allow(dead_code)] // used in tracing outputs
#[derive(Clone, Debug)]
struct Meta {
    exporter_name: String,
    telemetry_type: String,
}

pub struct Exporter<InStr, Svc, Payload, Logger> {
    meta: Meta,
    input: InStr,
    svc: Svc,
    result_logger: Logger,
    flush_receiver: Option<FlushReceiver>,
    encode_drain_max_time: Duration,
    export_drain_max_time: Duration,
    _phantom: PhantomData<Payload>,
}

impl<InStr, Svc, Payload, Logger> Exporter<InStr, Svc, Payload, Logger> {
    pub fn new(
        exporter_name: &'static str,
        telemetry_type: &'static str,
        input: InStr,
        svc: Svc,
        result_logger: Logger,
        flush_receiver: Option<FlushReceiver>,
        encode_drain_max_time: Duration,
        export_drain_max_time: Duration,
    ) -> Self {
        Self {
            meta: Meta {
                exporter_name: exporter_name.to_string(),
                telemetry_type: telemetry_type.to_string(),
            },
            input,
            svc,
            result_logger,
            flush_receiver,
            encode_drain_max_time,
            export_drain_max_time,
            _phantom: PhantomData,
        }
    }
}

impl<InStr, Svc, Payload, Logger> Exporter<InStr, Svc, Payload, Logger>
where
    InStr: Stream<Item = Result<Request<Payload>, BoxError>>,
    Svc: Service<Request<Payload>>,
    <Svc as Service<Request<Payload>>>::Error: Debug,
    <Svc as Service<Request<Payload>>>::Response: Debug,
    Logger: ResultLogger<<Svc as Service<Request<Payload>>>::Response>,
    Payload: Clone,
{
    pub async fn start(mut self, token: CancellationToken) -> Result<(), BoxError> {
        let meta = self.meta.clone();

        let input = self.input;
        pin!(input);

        let mut export_futures: FuturesUnordered<
            ExportFuture<<Svc as Service<Request<Payload>>>::Future>,
        > = FuturesUnordered::new();
        let mut flush_receiver = self.flush_receiver.take();
        loop {
            select! {
                biased;

                Some(resp) = export_futures.next() => {
                  match resp {
                        Err(e) => {
                            error!(error = ?e, ?meta, "Exporting failed, dropping data.")
                        },
                        Ok(rs) => {
                            debug!(rs = ?rs, futures_size = export_futures.len(), ?meta, "Exporter sent response");

                            self.result_logger.handle(rs);
                        }
                    }
                },

                input = input.next(), if export_futures.len() < MAX_CONCURRENT_REQUESTS => {
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

                            if let Err(res) = drain_futures(&meta, &mut input, &mut export_futures, self.encode_drain_max_time
                                , self.export_drain_max_time, &mut self.svc, true).await {

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

        drain_futures(
            &meta,
            &mut input,
            &mut export_futures,
            self.encode_drain_max_time,
            self.export_drain_max_time,
            &mut self.svc,
            false,
        )
        .await
    }
}

async fn drain_futures<InStr, Svc, Payload>(
    meta: &Meta,
    enc_stream: &mut Pin<&mut InStr>,
    export_futures: &mut FuturesUnordered<ExportFuture<<Svc as Service<Request<Payload>>>::Future>>,
    encode_drain_max_time: Duration,
    export_drain_max_time: Duration,
    svc: &mut Svc,
    non_blocking: bool,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>>
where
    InStr: Stream<Item = Result<Request<Payload>, BoxError>>,
    Svc: Service<Request<Payload>>,
    <Svc as Service<Request<Payload>>>::Error: Debug,
{
    let finish_encoding = Instant::now().add(encode_drain_max_time);
    let finish_sending = Instant::now().add(export_drain_max_time);

    // If non-blocking, we must poll at least once to force any messages into the encoding futures
    // list. This allows us to do the size check later, knowing that if there was a pending msg
    // it would have been added to the encoding futures.
    if non_blocking {
        match poll!(enc_stream.next()) {
            Poll::Ready(None) => {
                return drain_exports::<Svc, Payload>(
                    finish_sending,
                    export_futures,
                    meta,
                    non_blocking,
                )
                .await;
            }
            Poll::Ready(Some(res)) => match res {
                Ok(req) => export_futures.push(Box::pin(svc.call(req))),
                Err(e) => {
                    error!(error = ?e, ?meta, "Failed to encode request, dropping.");
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
            let (min_sz, _) = enc_stream.size_hint();
            if min_sz == 0 {
                break;
            }
        }

        let poll_res = timeout_at(finish_encoding, enc_stream.next()).await;
        match poll_res {
            Err(_) => {
                return Err(format!("Timed out waiting for requests to encode: {:?}", meta).into());
            }
            Ok(res) => match res {
                None => break,
                Some(r) => match r {
                    Ok(req) => export_futures.push(Box::pin(svc.call(req))),
                    Err(e) => {
                        error!(error = ?e, ?meta, "Failed to encode request, dropping.");
                    }
                },
            },
        }
    }

    drain_exports::<Svc, Payload>(finish_sending, export_futures, meta, non_blocking).await
}

async fn drain_exports<Svc, Payload>(
    finish_sending: Instant,
    export_futures: &mut FuturesUnordered<ExportFuture<<Svc as Service<Request<Payload>>>::Future>>,
    meta: &Meta,
    non_blocking: bool,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>>
where
    <Svc as Service<Request<Payload>>>::Error: Debug,
    Svc: Service<Request<Payload>>,
{
    let mut drain_errors = 0;
    loop {
        if export_futures.is_empty() {
            break;
        }

        let poll_res = timeout_at(finish_sending, export_futures.next()).await;
        match poll_res {
            Err(_) => {
                return Err(format!("Timed out waiting for requests to finish: {:?}", meta).into());
            }
            Ok(res) => match res {
                None => {
                    error!(?meta, "None returned while polling futures");
                    break;
                }
                Some(r) => {
                    if let Err(e) = r {
                        error!(?meta,
                            error = ?e,
                            "Error from exporter endpoint."
                        );

                        drain_errors += 1;
                    }
                }
            },
        }
    }

    if drain_errors > 0 {
        Err(format!(
            "Failed draining export requests, {} requests failed: {:?}",
            drain_errors, meta,
        )
        .into())
    } else {
        Ok(())
    }
}

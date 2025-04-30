use crate::exporters::clickhouse::ClickhouseRespDecoder;
use crate::exporters::clickhouse::payload::ClickhousePayload;
use crate::exporters::http::http_client::HttpClient;
use crate::exporters::http::response::Response;
use crate::exporters::http::retry::RetryPolicy;
use crate::topology::flush_control::{FlushReceiver, conditional_flush};
use futures_util::stream::FuturesUnordered;
use futures_util::{Stream, StreamExt};
use http::Request;
use std::error::Error;
use std::ops::Add;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::{Instant, timeout_at};
use tokio::{pin, select};
use tokio_util::sync::CancellationToken;
use tower::BoxError;
use tower::Service;
use tower::retry::{Retry as TowerRetry, Retry};
use tower::timeout::Timeout;
use tracing::{debug, error, info, warn};

pub(crate) const MAX_CONCURRENT_REQUESTS: usize = 10;

type ExportFuture = Pin<Box<dyn Future<Output = Result<Response<()>, BoxError>> + Send>>;

pub struct Exporter<InStr> {
    input: InStr,
    svc: TowerRetry<
        RetryPolicy<()>,
        Timeout<HttpClient<ClickhousePayload, (), ClickhouseRespDecoder>>,
    >,
    flush_receiver: Option<FlushReceiver>,
    encode_drain_max_time: Duration,
    export_drain_max_time: Duration,
}

impl<InStr> Exporter<InStr> {
    pub fn new(
        input: InStr,
        svc: TowerRetry<
            RetryPolicy<()>,
            Timeout<HttpClient<ClickhousePayload, (), ClickhouseRespDecoder>>,
        >,
        flush_receiver: Option<FlushReceiver>,
        encode_drain_max_time: Duration,
        export_drain_max_time: Duration,
    ) -> Self {
        Self {
            input,
            svc,
            flush_receiver,
            encode_drain_max_time,
            export_drain_max_time,
        }
    }
}

impl<InStr> Exporter<InStr>
where
    InStr: Stream<Item = Result<Request<ClickhousePayload>, BoxError>>,
{
    pub async fn start(mut self, token: CancellationToken) -> Result<(), BoxError> {
        let input = self.input;
        pin!(input);

        let mut export_futures: FuturesUnordered<ExportFuture> = FuturesUnordered::new();
        let mut flush_receiver = self.flush_receiver.take();
        loop {
            select! {
                biased;

                Some(resp) = export_futures.next() => {
                  match resp {
                        Err(e) => {
                            error!(error = ?e, "Exporting failed, dropping data.")
                        },
                        Ok(rs) => {
                            debug!(rs = ?rs, futures_size = export_futures.len(), "Clickhouse exporter sent response");

                            match rs.status_code().as_u16() {
                                200..=202 => {},
                                404 => error!("Received 404 when exporting to Clickhouse, does the table exist?"),
                                _ => error!("Failed to export to Clickhouse: {:?}", rs),
                            };
                        }
                    }
                },

                input = input.next(), if export_futures.len() < MAX_CONCURRENT_REQUESTS => {
                    match input {
                        None => {
                            debug!("Clickhouse exporter received end of input, exiting.");
                            break
                        },
                        Some(req) => match req {
                            Ok(req) => export_futures.push(Box::pin(self.svc.call(req))),
                            Err(e) => {
                                error!(error = ?e, "Failed to encode Clickhouse request, dropping.");
                            }
                        }
                    }
                },

                Some(resp) = conditional_flush(&mut flush_receiver) => {
                    match resp {
                        (Some(req), listener) => {
                            debug!("received force flush in Clickhouse exporter: {:?}", req);

                            if let Err(res) = drain_futures(&mut input, &mut export_futures, self.encode_drain_max_time
                                , self.export_drain_max_time, &mut self.svc).await {

                                warn!("unable to drain exporter: {}", res);
                            }

                            if let Err(e) = listener.ack(req).await {
                                warn!("unable to ack flush request: {}", e);
                            }
                        },
                        (None, _) => warn!("flush channel was closed")
                    }
                },

                _ = token.cancelled() => {
                    info!("Clickhouse exporter received shutdown signal, exiting main processing loop");
                    break;
                },
            }
        }

        drain_futures(
            &mut input,
            &mut export_futures,
            self.encode_drain_max_time,
            self.export_drain_max_time,
            &mut self.svc,
        )
        .await
    }

    // async fn drain_futures(
    //     &mut self,
    //     enc_stream: &mut Pin<&mut InStr>,
    //     export_futures: &mut FuturesUnordered<ExportFuture>,
    // ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    //     let finish_encoding = Instant::now().add(self.encode_drain_max_time);
    //     let finish_sending = Instant::now().add(self.export_drain_max_time);
    //     let type_name = "clickhouse_exporter";
    //
    //     // First we must wait on currently encoding futures
    //     loop {
    //         let poll_res = timeout_at(finish_encoding, enc_stream.next()).await;
    //         match poll_res {
    //             Err(_) => {
    //                 return Err(
    //                     "ClickHouseExporter, timed out waiting for requests to encode".into(),
    //                 );
    //             }
    //             Ok(res) => match res {
    //                 None => break,
    //                 Some(r) => match r {
    //                     Ok(req) => export_futures.push(Box::pin(self.svc.call(req))),
    //                     Err(e) => {
    //                         error!(error = ?e, "Failed to encode Datadog request, dropping.");
    //                     }
    //                 },
    //             },
    //         }
    //     }
    //
    //     let mut drain_errors = 0;
    //     loop {
    //         if export_futures.is_empty() {
    //             break;
    //         }
    //
    //         let poll_res = timeout_at(finish_sending, export_futures.next()).await;
    //         match poll_res {
    //             Err(_) => {
    //                 return Err(
    //                     "ClickHouseExporter, timed out waiting for requests to finish".into(),
    //                 );
    //             }
    //             Ok(res) => match res {
    //                 None => {
    //                     error!(type_name, "None returned while polling futures");
    //                     break;
    //                 }
    //                 Some(r) => {
    //                     if let Err(e) = r {
    //                         error!(type_name,
    //                             error = ?e,
    //                             "ClickHouseExporter error from endpoint."
    //                         );
    //
    //                         drain_errors += 1;
    //                     }
    //                 }
    //             },
    //         }
    //     }
    //
    //     if drain_errors > 0 {
    //         Err(format!(
    //             "Failed draining export requests, {} requests failed",
    //             drain_errors
    //         )
    //         .into())
    //     } else {
    //         Ok(())
    //     }
    // }
}

async fn drain_futures<InStr>(
    enc_stream: &mut Pin<&mut InStr>,
    export_futures: &mut FuturesUnordered<ExportFuture>,
    encode_drain_max_time: Duration,
    export_drain_max_time: Duration,
    svc: &mut Retry<
        RetryPolicy<()>,
        Timeout<HttpClient<ClickhousePayload, (), ClickhouseRespDecoder>>,
    >,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>>
where
    InStr: Stream<Item = Result<Request<ClickhousePayload>, BoxError>>,
{
    let finish_encoding = Instant::now().add(encode_drain_max_time);
    let finish_sending = Instant::now().add(export_drain_max_time);
    let type_name = "clickhouse_exporter";

    // First we must wait on currently encoding futures
    loop {
        let poll_res = timeout_at(finish_encoding, enc_stream.next()).await;
        match poll_res {
            Err(_) => {
                return Err("ClickHouseExporter, timed out waiting for requests to encode".into());
            }
            Ok(res) => match res {
                None => break,
                Some(r) => match r {
                    Ok(req) => export_futures.push(Box::pin(svc.call(req))),
                    Err(e) => {
                        error!(error = ?e, "Failed to encode Datadog request, dropping.");
                    }
                },
            },
        }
    }

    let mut drain_errors = 0;
    loop {
        if export_futures.is_empty() {
            break;
        }

        let poll_res = timeout_at(finish_sending, export_futures.next()).await;
        match poll_res {
            Err(_) => {
                return Err("ClickHouseExporter, timed out waiting for requests to finish".into());
            }
            Ok(res) => match res {
                None => {
                    error!(type_name, "None returned while polling futures");
                    break;
                }
                Some(r) => {
                    if let Err(e) = r {
                        error!(type_name,
                            error = ?e,
                            "ClickHouseExporter error from endpoint."
                        );

                        drain_errors += 1;
                    }
                }
            },
        }
    }

    if drain_errors > 0 {
        Err(format!(
            "Failed draining export requests, {} requests failed",
            drain_errors
        )
        .into())
    } else {
        Ok(())
    }
}

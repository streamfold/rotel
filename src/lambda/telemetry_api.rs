use crate::bounded_channel::BoundedSender;
use crate::listener::Listener;
use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Body;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use lambda_extension::{LambdaTelemetry, LambdaTelemetryRecord};
use std::fmt::{Debug, Display};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_util::sync::CancellationToken;
use tower::{BoxError, Service, ServiceBuilder};
use tracing::{error, info};

pub struct TelemetryAPI {
    pub listener: Listener,
}

impl TelemetryAPI {
    pub async fn bind_and_listen() -> Result<TelemetryAPI, BoxError> {
        let listener = Listener::listen_async("0.0.0.0:0".parse().unwrap()).await?;

        Ok(TelemetryAPI { listener })
    }

    pub fn addr(&self) -> SocketAddr {
        self.listener.bound_address().unwrap()
    }

    // todo: abstract this with the server code in the otlp http receiver
    pub async fn run(
        self,
        bus_tx: BoundedSender<LambdaTelemetry>,
        cancellation: CancellationToken,
    ) -> Result<(), BoxError> {
        let svc = ServiceBuilder::new().service(TelemetryService::new(bus_tx));
        let svc = TowerToHyperService::new(svc);

        let timer = hyper_util::rt::TokioTimer::new();
        let graceful = hyper_util::server::graceful::GracefulShutdown::new();

        let mut builder = Builder::new(TokioExecutor::new());
        builder
            .http1()
            .header_read_timeout(Some(std::time::Duration::from_secs(3)))
            .timer(timer.clone());
        builder.http2().timer(timer);

        let listener = self.listener.into_async()?;
        loop {
            let stream = tokio::select! {
                r = listener.accept() => {
                    match r {
                        Ok((stream, _)) => stream,
                        Err(e) => return Err(e.into()),
                    }
                },
                _ = cancellation.cancelled() => break
            };

            let io = TokioIo::new(stream);

            let conn = builder.serve_connection(io, svc.clone());
            let fut = graceful.watch(conn.into_owned());

            tokio::spawn(async move {
                let _ = fut.await.map_err(|e| {
                    if let Some(hyper_err) = e.downcast_ref::<hyper::Error>() {
                        // xxx: is there any way to get the error kind?
                        let err_str = format!("{:?}", hyper_err);

                        // This may imply a client shutdown race: https://github.com/hyperium/hyper/issues/3775
                        let err_not_connected = err_str.contains("NotConnected");
                        // There is no idle timeout, so header timeout is hit first
                        let err_hdr_timeout = err_str.contains("HeaderTimeout");

                        if !err_not_connected && !err_hdr_timeout {
                            error!("error serving connection: {:?}", hyper_err);
                        }
                    } else {
                        error!("error serving connection: {:?}", e);
                    }
                });
            });
        }

        // gracefully shutdown existing connections
        graceful.shutdown().await;

        Ok(())
    }
}

#[derive(Clone)]
pub struct TelemetryService {
    bus_tx: BoundedSender<LambdaTelemetry>,
}

impl TelemetryService {
    fn new(bus_tx: BoundedSender<LambdaTelemetry>) -> Self {
        Self { bus_tx }
    }
}

impl<H> Service<Request<H>> for TelemetryService
where
    H: Body + Send + Sync + 'static,
    <H as Body>::Data: Send + Sync + Clone,
    <H as Body>::Error: Display + Debug + Send + Sync + ToString,
{
    type Response = Response<Full<Bytes>>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<H>) -> Self::Future {
        let (parts, body) = req.into_parts();

        // will remove when debugged
        info!(?parts, "request received");

        // This part could be decoupled out to a layer, but they are complicated
        // to setup, so inlining for now.
        if parts.method != Method::POST {
            return Box::pin(futures::future::ok(
                response_4xx(StatusCode::METHOD_NOT_ALLOWED).unwrap(),
            ));
        }

        if parts
            .headers
            .get(CONTENT_TYPE)
            .is_none_or(|ct| ct != "application/json")
        {
            return Box::pin(futures::future::ok(
                response_4xx(StatusCode::BAD_REQUEST).unwrap(),
            ));
        }

        Box::pin(handle_request(self.bus_tx.clone(), body))
    }
}

async fn handle_request<H>(
    bus_tx: BoundedSender<LambdaTelemetry>,
    body: H,
) -> Result<Response<Full<Bytes>>, BoxError>
where
    H: Body,
    <H as Body>::Error: Debug,
{
    let buf = body.collect().await.unwrap().to_bytes();

    let events: Vec<LambdaTelemetry> = serde_json::from_slice(&buf.to_vec())
        .map_err(|e| format!("unable to parse telemetry events from json: {}", e))?;

    for event in &events {
        info!("received telemetry event from lambda: {:?}", event);
        match event.record {
            LambdaTelemetryRecord::PlatformRuntimeDone { .. } => {
                if let Err(e) = bus_tx.send(event.clone()).await {
                    error!("unable to send telemetry event to bus: {}", e);
                    // Should handle this?
                }
            }
            _ => {} // todo: handle more
        }
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Full::default())
        .unwrap())
}

fn response_4xx(code: StatusCode) -> Result<Response<Full<Bytes>>, hyper::Error> {
    response_4xx_with_body(code, Bytes::default())
}

fn response_4xx_with_body(
    code: StatusCode,
    body: Bytes,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    Ok(Response::builder()
        .status(code)
        .body(Full::new(body))
        .unwrap())
}

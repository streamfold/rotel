// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::client::{ConnectError, ResponseDecode, build_hyper_client};
use crate::exporters::http::response::Response;
use crate::exporters::http::tls::Config;
use crate::exporters::http::types::ContentEncoding;
use bytes::Bytes;
use http::Request;
use http::header::CONTENT_ENCODING;
use http_body_util::BodyExt;
use hyper::body::{Body, Incoming};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{BoxError, Service};
use tracing::warn;

#[derive(Clone)]
pub struct HttpClient<ReqBody, Resp, Dec> {
    inner: HyperClient<HttpsConnector<HttpConnector>, ReqBody>,
    decoder: Dec,
    _phantom: PhantomData<Resp>,
}

impl<ReqBody, Resp, Dec> HttpClient<ReqBody, Resp, Dec>
where
    ReqBody: Body + Send,
    <ReqBody as Body>::Data: Send,
{
    pub fn build(tls_config: Config, decoder: Dec) -> Result<Self, BoxError> {
        let inner = build_hyper_client(tls_config, false)?;

        Ok(Self {
            inner,
            decoder,
            _phantom: Default::default(),
        })
    }
}

impl<ReqBody, Resp, Dec> HttpClient<ReqBody, Resp, Dec>
where
    ReqBody: Body + Send + 'static + Unpin,
    <ReqBody as Body>::Data: Send,
    <ReqBody as Body>::Error: Into<BoxError>,
    Dec: ResponseDecode<Resp> + Clone,
{
    async fn perform_request(&self, req: Request<ReqBody>) -> Result<Response<Resp>, BoxError> {
        match self.inner.request(req).await {
            Err(e) => {
                if e.is_connect() {
                    Err(ConnectError {}.into())
                } else {
                    Err(e.into())
                }
            }
            Ok(resp) => {
                let (head, mut body) = resp.into_parts();

                let encoding = match head.headers.get(CONTENT_ENCODING) {
                    None => ContentEncoding::None,
                    Some(v) => match TryFrom::try_from(v) {
                        Ok(ce) => ce,
                        Err(e) => return Err(e),
                    },
                };

                // todo: we may want to parse the body on failures in the future?
                if !(200..=202).contains(&head.status.as_u16()) {
                    let body = match self
                        .decoder
                        .decode(response_bytes(body).await?, encoding.clone())
                    {
                        Ok(r) => Some(r),
                        Err(_e) => None, // todo: handle strings types better
                    };

                    return Ok(Response::from_http(head, body));
                }

                let mut resp = Response::from_http(head, None);
                while let Some(next) = body.frame().await {
                    match next {
                        Ok(frame) => {
                            if frame.is_data() {
                                let data = frame.into_data().unwrap();
                                match self.decoder.decode(data, encoding.clone()) {
                                    Ok(r) => resp = resp.with_body(r),
                                    Err(e) => return Err(e),
                                }
                            } else if frame.is_trailers() {
                                let trailers = frame.into_trailers().unwrap();

                                warn!(
                                    "Received unexpected trailers on HTTP client: {:?}",
                                    trailers
                                );
                            }
                        }
                        Err(e) => return Err(format!("failed reading response: {}", e).into()),
                    }
                }

                Ok(resp)
            }
        }
    }
}

impl<ReqBody, Resp, Dec> Service<Request<ReqBody>> for HttpClient<ReqBody, Resp, Dec>
where
    Dec: ResponseDecode<Resp> + Send + Clone + Sync + 'static,
    ReqBody: Body + Clone + Send + 'static + Unpin,
    <ReqBody as Body>::Data: Send,
    <ReqBody as Body>::Error: Into<BoxError>,
    Resp: Send + Clone + Sync + 'static,
{
    type Response = Response<Resp>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let this = self.clone();

        Box::pin(async move {
            match this.perform_request(req).await {
                Ok(r) => Ok(r),
                Err(e) => Err(e.into()),
            }
        })
    }
}

pub async fn response_bytes(body: Incoming) -> Result<Bytes, BoxError> {
    body.collect()
        .await
        .map(|col| col.to_bytes())
        .map_err(|e| format!("failed to read response body: {}", e).into())
}

// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::client::{ConnectError, ResponseDecode, build_hyper_client};
use crate::exporters::http::response::Response;
use crate::exporters::http::tls::Config;
use crate::exporters::http::types::{ContentEncoding, Request};
use bytes::Bytes;
use http::header::CONTENT_ENCODING;
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{BoxError, Service};
use tracing::log::warn;

#[derive(Clone)]
pub struct HttpClient<Resp, Dec> {
    inner: HyperClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
    decoder: Dec,
    _phantom: PhantomData<Resp>,
}

impl<Resp, Dec> HttpClient<Resp, Dec> {
    pub fn build(tls_config: Config, decoder: Dec) -> Result<Self, BoxError> {
        let inner = build_hyper_client(tls_config, false)?;

        Ok(Self {
            inner,
            decoder,
            _phantom: Default::default(),
        })
    }
}

impl<Resp, Dec> HttpClient<Resp, Dec>
where
    Dec: ResponseDecode<Resp> + Clone,
    Resp: Default,
{
    async fn perform_request(&self, req: Request) -> Result<Response<Resp>, BoxError> {
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

                // todo: we may want to parse the body on failures in the future?
                if !(200..=202).contains(&head.status.as_u16()) {
                    return Ok(Response::from_http(head, None));
                }

                let encoding = match head.headers.get(CONTENT_ENCODING) {
                    None => ContentEncoding::None,
                    Some(v) => match TryFrom::try_from(v) {
                        Ok(ce) => ce,
                        Err(e) => return Err(e),
                    },
                };

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

impl<Resp, Dec> Service<Request> for HttpClient<Resp, Dec>
where
    Dec: ResponseDecode<Resp> + Send + Clone + Sync + 'static,
    Resp: Default + Send + Clone + Sync + 'static,
{
    type Response = Response<Resp>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let this = self.clone();

        Box::pin(async move {
            match this.perform_request(req).await {
                Ok(r) => Ok(r),
                Err(e) => Err(e.into()),
            }
        })
    }
}

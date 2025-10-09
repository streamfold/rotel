// SPDX-License-Identifier: Apache-2.0

use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::aws_api::auth::{AwsRequestSigner, SystemClock};
use crate::aws_api::creds::AwsCredsProvider;
use crate::exporters::http::metadata_extractor::{MessagePayload, MetadataExtractor};
use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Full};
use hyper::body::Body;
use tower::{BoxError, Service};

/// A generic middleware service that intercepts HTTP requests to add AWS signing headers.
/// This service can be used with any exporter that uses the ServiceBuilder pattern
/// (Datadog, AWS EMF, X-Ray, etc.).
///
#[derive(Clone)]
pub struct AwsSigningService<S> {
    inner: S,
    config: Arc<SigningConfig>, // Uses Arc to reduce cloning cost
}

pub enum SigningConfig {
    Enabled {
        signer: AwsRequestSigner<SystemClock>,
        creds_provider: AwsCredsProvider,
    },
    Disabled,
}

#[derive(Clone)]
pub struct AwsSigningServiceBuilder {
    config: Arc<SigningConfig>,
}

impl AwsSigningServiceBuilder {
    pub fn new(service: &str, region: &str, creds_provider: AwsCredsProvider) -> Self {
        Self {
            config: Arc::new(SigningConfig::Enabled {
                signer: AwsRequestSigner::new(service, region, SystemClock {}),
                creds_provider,
            }),
        }
    }

    /// Create a builder with AWS signing disabled (pass-through mode)
    /// This is useful for local development and testing where AWS credentials are not needed
    pub fn disabled() -> Self {
        Self {
            config: Arc::new(SigningConfig::Disabled),
        }
    }

    pub fn build<S>(self, inner: S) -> AwsSigningService<S> {
        AwsSigningService {
            inner,
            config: self.config,
        }
    }

    /// Manually sign a request without using the Tower service pattern.
    /// This is useful for one-off requests where the Tower middleware overhead is not needed.
    pub async fn sign_request(
        &self,
        uri: http::Uri,
        method: http::Method,
        headers: http::HeaderMap,
        body: bytes::Bytes,
    ) -> Result<http::Request<http_body_util::Full<bytes::Bytes>>, BoxError> {
        match self.config.as_ref() {
            SigningConfig::Disabled => {
                // No signing needed, just build the request
                let mut req_builder = http::Request::builder().uri(uri).method(method);

                let builder_headers = req_builder.headers_mut().unwrap();
                for (k, v) in headers.iter() {
                    builder_headers.insert(k, v.clone());
                }

                Ok(req_builder.body(http_body_util::Full::from(body))?)
            }
            SigningConfig::Enabled {
                signer,
                creds_provider,
            } => {
                // Get AWS credentials
                let creds = creds_provider.get_creds().await?;

                // Sign the request
                Ok(signer.sign(uri, method, headers, body, &creds)?)
            }
        }
    }
}

impl<S, ReqBody> Service<Request<ReqBody>> for AwsSigningService<S>
where
    ReqBody: Body + Clone + Send + MetadataExtractor + 'static,
    <ReqBody as Body>::Data: Send,
    <ReqBody as Body>::Error: Display,
    S: Service<Request<MessagePayload<Full<Bytes>>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let config = self.config.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let (parts, mut body) = req.into_parts();
            let metadata = body.take_metadata();

            let body_bytes = match body.collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(e) => {
                    return Err(format!("Failed to collect request body: {}", e).into());
                }
            };

            match config.as_ref() {
                SigningConfig::Disabled => {
                    let payload = MessagePayload::new(Full::from(body_bytes), metadata);

                    let req = Request::from_parts(parts, payload);

                    inner.call(req).await.map_err(Into::into)
                }
                SigningConfig::Enabled {
                    signer,
                    creds_provider,
                } => {
                    let creds = creds_provider.get_creds().await?;

                    let signed_req =
                        signer.sign(parts.uri, parts.method, parts.headers, body_bytes, &creds)?;

                    let (parts, body) = signed_req.into_parts();

                    let payload = MessagePayload::new(body, metadata);

                    let req = Request::from_parts(parts, payload);

                    inner.call(req).await.map_err(Into::into)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::aws_api::creds::AwsCreds;

    use super::*;
    use http::{
        Method, Response, StatusCode,
        header::{AUTHORIZATION, HOST},
    };
    use std::convert::Infallible;
    use tower::service_fn;

    fn test_creds() -> AwsCreds {
        AwsCreds::new(
            "AKIAIOSFODNN7EXAMPLE".to_string(),
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            None,
        )
    }

    fn test_provider() -> AwsCredsProvider {
        AwsCredsProvider::from_static(test_creds())
    }

    #[tokio::test]
    async fn test_adds_aws_signing_headers() {
        let inner_service = service_fn(|req: Request<Full<Bytes>>| async move {
            // Verify AWS signing headers were added
            assert!(req.headers().get("X-Amz-Date").is_some());
            assert!(req.headers().get(AUTHORIZATION).is_some());
            assert!(req.headers().get(HOST).is_some());

            let auth_header = req.headers().get(AUTHORIZATION).unwrap().to_str().unwrap();
            assert!(auth_header.starts_with("AWS4-HMAC-SHA256"));
            assert!(auth_header.contains("Credential=AKIAIOSFODNN7EXAMPLE"));
            assert!(auth_header.contains("us-east-1/s3/aws4_request"));

            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body("OK".to_string())
                    .unwrap(),
            )
        });

        let mut signing_service =
            AwsSigningServiceBuilder::new("s3", "us-east-1", test_provider()).build(inner_service);

        let request = Request::builder()
            .uri("https://s3.amazonaws.com/bucket/key")
            .method(Method::GET)
            .body(Full::from(Bytes::new()))
            .unwrap();

        let response = signing_service.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_preserves_existing_headers() {
        let inner_service = service_fn(|req: Request<Full<Bytes>>| async move {
            // Verify both original and AWS headers are present
            assert_eq!(
                req.headers().get("Content-Type").unwrap(),
                "application/json"
            );
            assert!(req.headers().get("X-Amz-Date").is_some());
            assert!(req.headers().get(AUTHORIZATION).is_some());

            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body("OK".to_string())
                    .unwrap(),
            )
        });

        let mut signing_service =
            AwsSigningServiceBuilder::new("s3", "us-east-1", test_provider()).build(inner_service);

        let request = Request::builder()
            .uri("https://s3.amazonaws.com/bucket/key")
            .method(Method::POST)
            .header("Content-Type", "application/json")
            .body(Full::<Bytes>::from(Bytes::from("test body")))
            .unwrap();

        let response = signing_service.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_builder_disabled_mode() {
        let inner_service = service_fn(|req: Request<Full<Bytes>>| async move {
            // Verify AWS signing headers were NOT added
            assert!(req.headers().get("X-Amz-Date").is_none());
            assert!(req.headers().get(AUTHORIZATION).is_none());

            // But original headers should be present
            assert_eq!(req.headers().get("X-Custom-Header").unwrap(), "test-value");

            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body("OK".to_string())
                    .unwrap(),
            )
        });

        let builder = AwsSigningServiceBuilder::disabled();
        let mut signing_service = builder.build(inner_service);

        let request = Request::builder()
            .uri("https://s3.amazonaws.com/bucket/key")
            .method(Method::GET)
            .header("X-Custom-Header", "test-value")
            .body(Full::from(Bytes::new()))
            .unwrap();

        let response = signing_service.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

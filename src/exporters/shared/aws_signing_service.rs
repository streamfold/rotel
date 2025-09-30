// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::aws_api::config::AwsConfig;
use bytes::Bytes;
use chrono::Utc;
use hmac::{Hmac, Mac};
use http::header::{AUTHORIZATION, HOST};
use http::{HeaderValue, Request};
use http_body_util::{BodyExt, Full};
use hyper::body::Body;
use sha2::{Digest, Sha256};
use tower::{BoxError, Service};

type HmacSha256 = Hmac<Sha256>;

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
        service: String,
        region: String,
        config: AwsConfig,
    },
    Disabled,
}

#[derive(Clone)]
pub struct AwsSigningServiceBuilder {
    config: Arc<SigningConfig>,
}

impl AwsSigningServiceBuilder {
    pub fn new(service: &str, region: &str, config: AwsConfig) -> Self {
        Self {
            config: Arc::new(SigningConfig::Enabled {
                service: service.to_string(),
                region: region.to_string(),
                config,
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
}

impl<S> AwsSigningService<S> {
    /// Internal method to perform AWS SigV4 signing on a request
    /// This method is separated for better testability and cleaner code structure
    fn sign_request(
        uri: http::Uri,
        method: http::Method,
        mut headers: http::HeaderMap,
        body_bytes: Bytes,
        service: &str,
        region: &str,
        config: &AwsConfig,
    ) -> Result<Request<Full<Bytes>>, BoxError> {
        // AWS SigV4 signing logic (inlined from auth.rs for performance)
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        // Add host header if it doesn't exist
        if !headers.contains_key(HOST) {
            let host_value = if let Some(port) = uri.port() {
                format!("{}:{}", uri.host().unwrap_or_default(), port)
            } else {
                uri.host().unwrap_or_default().to_string()
            };

            headers.insert(
                HOST,
                HeaderValue::from_str(&host_value)
                    .map_err(|e| BoxError::from(format!("Invalid host header: {}", e)))?,
            );
        }

        // Add session token if provided
        if let Some(token) = &config.aws_session_token {
            if !token.is_empty() {
                headers.insert(
                    "X-Amz-Security-Token",
                    HeaderValue::from_str(token)
                        .map_err(|e| BoxError::from(format!("Invalid session token: {}", e)))?,
                );
            }
        }

        // Add date header
        headers.insert(
            "X-Amz-Date",
            HeaderValue::from_str(&amz_date)
                .map_err(|e| BoxError::from(format!("Invalid date: {}", e)))?,
        );

        // Step 1: Create canonical request
        let canonical_uri = uri.path();

        let canonical_querystring = match uri.query() {
            None => String::new(),
            Some(q) => {
                // Parse and sort query parameters
                let mut query_params: Vec<(&str, &str)> = q
                    .split('&')
                    .map(|s| {
                        let mut splits = s.splitn(2, '=');
                        let key = splits.next().unwrap_or("");
                        let value = splits.next().unwrap_or("");
                        (key, value)
                    })
                    .collect();
                query_params.sort_by(|a, b| a.0.cmp(b.0));

                query_params
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<String>>()
                    .join("&")
            }
        };

        // Get and sort headers for canonical headers
        let mut header_pairs: Vec<(String, String)> = headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_lowercase(),
                    value.to_str().unwrap_or_default().trim().to_string(),
                )
            })
            .collect();
        header_pairs.sort_by(|a, b| a.0.cmp(&b.0));

        let mut canonical_headers = String::new();
        let mut signed_headers = Vec::new();

        for (name, value) in &header_pairs {
            canonical_headers.push_str(&format!("{}:{}\n", name, value));
            signed_headers.push(name.as_str());
        }

        let signed_headers_str = signed_headers.join(";");

        // Calculate payload hash
        let payload_hash = hex::encode(Sha256::digest(&body_bytes));

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method,
            canonical_uri,
            canonical_querystring,
            canonical_headers,
            signed_headers_str,
            payload_hash
        );

        // Step 2: Create the string to sign
        let algorithm = "AWS4-HMAC-SHA256";
        let credential_scope = format!("{}/{}/{}/aws4_request", date_stamp, region, service);
        let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

        let string_to_sign = format!(
            "{}\n{}\n{}\n{}",
            algorithm, amz_date, credential_scope, canonical_request_hash
        );

        // Step 3: Calculate the signature
        let signature = {
            let k_secret = format!("AWS4{}", config.aws_secret_access_key);

            let k_date = Self::sign_hmac(k_secret.as_bytes(), date_stamp.as_bytes())?;
            // let k_date = {
            //     let mut mac = HmacSha256::new_from_slice(k_secret.as_bytes())
            //         .map_err(|e| BoxError::from(format!("Invalid HMAC key: {}", e)))?;
            //     mac.update(date_stamp.as_bytes());
            //     mac.finalize().into_bytes().to_vec()
            // };

            let k_region = Self::sign_hmac(&k_date, region.as_bytes())?;
            // let k_region = {
            //     let mut mac = HmacSha256::new_from_slice(&k_date)
            //         .map_err(|e| BoxError::from(format!("Invalid HMAC key: {}", e)))?;
            //     mac.update(region.as_bytes());
            //     mac.finalize().into_bytes().to_vec()
            // };

            let k_service = Self::sign_hmac(&k_region, service.as_bytes())?;
            // let k_service = {
            //     let mut mac = HmacSha256::new_from_slice(&k_region)
            //         .map_err(|e| BoxError::from(format!("Invalid HMAC key: {}", e)))?;
            //     mac.update(service.as_bytes());
            //     mac.finalize().into_bytes().to_vec()
            // };

            let k_signing = Self::sign_hmac(&k_service, b"aws4_request")?;
            // let k_signing = {
            //     let mut mac = HmacSha256::new_from_slice(&k_service)
            //         .map_err(|e| BoxError::from(format!("Invalid HMAC key: {}", e)))?;
            //     mac.update(b"aws4_request");
            //     mac.finalize().into_bytes().to_vec()
            // };

            // Sign the string to sign
            let sig = Self::sign_hmac(&k_signing, string_to_sign.as_bytes())?;
            // let mut mac = HmacSha256::new_from_slice(&k_signing)
            //     .map_err(|e| BoxError::from(format!("Invalid HMAC key: {}", e)))?;
            // mac.update(string_to_sign.as_bytes());
            // hex::encode(mac.finalize().into_bytes())
            hex::encode(sig)
        };

        // Step 4: Add authorization header
        let authorization_header = format!(
            "{} Credential={}/{}, SignedHeaders={}, Signature={}",
            algorithm, config.aws_access_key_id, credential_scope, signed_headers_str, signature
        );

        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&authorization_header)
                .map_err(|e| BoxError::from(format!("Invalid authorization header: {}", e)))?,
        );

        // Reconstruct the request with signed headers and original body
        let signed_request = Request::builder()
            .uri(uri)
            .method(method)
            .body(Full::from(body_bytes))
            .map_err(|e| BoxError::from(format!("Failed to build request: {}", e)))?;

        // Apply headers to the request
        let (mut parts, body) = signed_request.into_parts();
        parts.headers = headers;
        let final_request = Request::from_parts(parts, body);

        Ok(final_request)
    }

    fn sign_hmac(key: &[u8], message: &[u8]) -> Result<Vec<u8>, BoxError> {
        let mut mac = HmacSha256::new_from_slice(key)
            .map_err(|e| BoxError::from(format!("Invalid HMAC key: {}", e)))?;
        mac.update(message);
        Ok(mac.finalize().into_bytes().to_vec())
    }
}

impl<S> Service<Request<Full<Bytes>>> for AwsSigningService<S>
where
    S: Service<Request<Full<Bytes>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<Full<Bytes>>) -> Self::Future {
        let config = self.config.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            match config.as_ref() {
                SigningConfig::Disabled => inner.call(req).await.map_err(Into::into),
                SigningConfig::Enabled {
                    service,
                    region,
                    config,
                } => {
                    let (parts, body) = req.into_parts();

                    let body_bytes = match body.collect().await {
                        Ok(collected) => collected.to_bytes(),
                        Err(e) => {
                            return Err(format!("Failed to collect request body: {}", e).into());
                        }
                    };

                    let signed_req = Self::sign_request(
                        parts.uri,
                        parts.method,
                        parts.headers,
                        body_bytes,
                        &service,
                        &region,
                        &config,
                    )?;

                    inner.call(signed_req).await.map_err(Into::into)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws_api::config::AwsConfig;
    use http::{Method, Response, StatusCode};
    use std::convert::Infallible;
    use tower::service_fn;

    fn test_config() -> AwsConfig {
        AwsConfig::new(
            "us-east-1".to_string(),
            "AKIAIOSFODNN7EXAMPLE".to_string(),
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            None,
        )
    }

    fn test_config_with_session() -> AwsConfig {
        AwsConfig::new(
            "us-east-1".to_string(),
            "AKIAIOSFODNN7EXAMPLE".to_string(),
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            Some("SESSION_TOKEN_EXAMPLE".to_string()),
        )
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
            AwsSigningServiceBuilder::new("s3", "us-east-1", test_config()).build(inner_service);

        let request = Request::builder()
            .uri("https://s3.amazonaws.com/bucket/key")
            .method(Method::GET)
            .body(Full::from(Bytes::new()))
            .unwrap();

        let response = signing_service.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_adds_session_token_when_provided() {
        let inner_service = service_fn(|req: Request<Full<Bytes>>| async move {
            // Verify session token header was added
            assert_eq!(
                req.headers().get("X-Amz-Security-Token").unwrap(),
                "SESSION_TOKEN_EXAMPLE"
            );

            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body("OK".to_string())
                    .unwrap(),
            )
        });

        let mut signing_service =
            AwsSigningServiceBuilder::new("s3", "us-east-1", test_config_with_session())
                .build(inner_service);

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
            AwsSigningServiceBuilder::new("s3", "us-east-1", test_config()).build(inner_service);

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
    async fn test_handles_query_parameters() {
        let inner_service = service_fn(|req: Request<Full<Bytes>>| async move {
            // Verify the query parameters are preserved in the signed request
            assert_eq!(req.uri().query(), Some("prefix=test&delimiter=%2F"));

            let auth_header = req.headers().get(AUTHORIZATION).unwrap().to_str().unwrap();
            assert!(auth_header.contains("SignedHeaders="));

            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body("OK".to_string())
                    .unwrap(),
            )
        });

        let mut signing_service =
            AwsSigningServiceBuilder::new("s3", "us-east-1", test_config()).build(inner_service);

        let request = Request::builder()
            .uri("https://s3.amazonaws.com/bucket?prefix=test&delimiter=%2F")
            .method(Method::GET)
            .body(Full::from(Bytes::new()))
            .unwrap();

        let response = signing_service.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_sign_request_internal_basic() {
        let uri = "https://s3.amazonaws.com/bucket/key".parse().unwrap();
        let method = Method::GET;
        let headers = http::HeaderMap::new();
        let body_bytes = Bytes::new();

        let signed_request = AwsSigningService::<()>::sign_request(
            uri,
            method,
            headers,
            body_bytes,
            "s3",
            "us-east-1",
            &test_config(),
        )
        .unwrap();

        // Verify AWS signing headers were added
        assert!(signed_request.headers().get("X-Amz-Date").is_some());
        assert!(signed_request.headers().get(AUTHORIZATION).is_some());
        assert!(signed_request.headers().get(HOST).is_some());

        let auth_header = signed_request
            .headers()
            .get(AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth_header.starts_with("AWS4-HMAC-SHA256"));
        assert!(auth_header.contains("Credential=AKIAIOSFODNN7EXAMPLE"));
        assert!(auth_header.contains("us-east-1/s3/aws4_request"));
    }

    #[test]
    fn test_sign_request_internal_with_query_params() {
        let uri = "https://s3.amazonaws.com/bucket?prefix=test&delimiter=%2F"
            .parse()
            .unwrap();
        let method = Method::GET;
        let headers = http::HeaderMap::new();
        let body_bytes = Bytes::new();

        let signed_request = AwsSigningService::<()>::sign_request(
            uri,
            method,
            headers,
            body_bytes,
            "s3",
            "us-east-1",
            &test_config(),
        )
        .unwrap();

        // Verify the query parameters are preserved
        assert_eq!(
            signed_request.uri().query(),
            Some("prefix=test&delimiter=%2F")
        );

        // Verify signing headers are present
        let auth_header = signed_request
            .headers()
            .get(AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth_header.contains("SignedHeaders="));
    }

    #[test]
    fn test_sign_request_internal_with_session_token() {
        let uri = "https://s3.amazonaws.com/bucket/key".parse().unwrap();
        let method = Method::GET;
        let headers = http::HeaderMap::new();
        let body_bytes = Bytes::new();

        let signed_request = AwsSigningService::<()>::sign_request(
            uri,
            method,
            headers,
            body_bytes,
            "s3",
            "us-east-1",
            &test_config_with_session(),
        )
        .unwrap();

        // Verify session token header was added
        assert_eq!(
            signed_request
                .headers()
                .get("X-Amz-Security-Token")
                .unwrap(),
            "SESSION_TOKEN_EXAMPLE"
        );
    }

    #[test]
    fn test_sign_request_internal_with_existing_headers() {
        let uri = "https://s3.amazonaws.com/bucket/key".parse().unwrap();
        let method = Method::POST;
        let mut headers = http::HeaderMap::new();
        headers.insert("Content-Type", "application/json".parse().unwrap());
        let body_bytes = Bytes::from("test payload");

        let signed_request = AwsSigningService::<()>::sign_request(
            uri,
            method,
            headers,
            body_bytes,
            "s3",
            "us-east-1",
            &test_config(),
        )
        .unwrap();

        // Verify original headers are preserved
        assert_eq!(
            signed_request.headers().get("Content-Type").unwrap(),
            "application/json"
        );

        // Verify AWS headers are added
        assert!(signed_request.headers().get("X-Amz-Date").is_some());
        assert!(signed_request.headers().get(AUTHORIZATION).is_some());

        // Verify the authorization header includes the content-type in signed headers
        let auth_header = signed_request
            .headers()
            .get(AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth_header.contains("content-type"));
    }

    #[test]
    fn test_sign_request_internal_with_existing_host() {
        let uri = "https://s3.amazonaws.com/bucket/key".parse().unwrap();
        let method = Method::GET;
        let mut headers = http::HeaderMap::new();
        headers.insert(HOST, "custom-host.com".parse().unwrap());
        let body_bytes = Bytes::new();

        let signed_request = AwsSigningService::<()>::sign_request(
            uri,
            method,
            headers,
            body_bytes,
            "s3",
            "us-east-1",
            &test_config(),
        )
        .unwrap();

        // Verify the existing host header is preserved
        assert_eq!(
            signed_request.headers().get(HOST).unwrap(),
            "custom-host.com"
        );
    }

    #[test]
    fn test_sign_request_internal_with_port() {
        let uri = "https://s3.amazonaws.com:8443/bucket/key".parse().unwrap();
        let method = Method::GET;
        let headers = http::HeaderMap::new();
        let body_bytes = Bytes::new();

        let signed_request = AwsSigningService::<()>::sign_request(
            uri,
            method,
            headers,
            body_bytes,
            "s3",
            "us-east-1",
            &test_config(),
        )
        .unwrap();

        // Verify the host header includes the port
        assert_eq!(
            signed_request.headers().get(HOST).unwrap(),
            "s3.amazonaws.com:8443"
        );
    }

    #[tokio::test]
    async fn test_builder_basic() {
        let inner_service = service_fn(|req: Request<Full<Bytes>>| async move {
            // Verify AWS signing headers were added
            assert!(req.headers().get("X-Amz-Date").is_some());
            assert!(req.headers().get(AUTHORIZATION).is_some());

            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body("OK".to_string())
                    .unwrap(),
            )
        });

        let builder = AwsSigningServiceBuilder::new("s3", "us-east-1", test_config());
        let mut signing_service = builder.build(inner_service);

        let request = Request::builder()
            .uri("https://s3.amazonaws.com/bucket/key")
            .method(Method::GET)
            .body(Full::from(Bytes::new()))
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

    #[tokio::test]
    async fn test_builder_disabled_no_config_needed() {
        let inner_service = service_fn(|req: Request<Full<Bytes>>| async move {
            // Verify no signing headers
            assert!(req.headers().get("X-Amz-Date").is_none());
            assert!(req.headers().get(AUTHORIZATION).is_none());

            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body("OK".to_string())
                    .unwrap(),
            )
        });

        // Can create disabled builder without any AWS config
        let mut signing_service = AwsSigningServiceBuilder::disabled().build(inner_service);

        let request = Request::builder()
            .uri("https://example.com/api")
            .method(Method::POST)
            .body(Full::from(Bytes::new()))
            .unwrap();

        let response = signing_service.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_builder_with_session_token() {
        let inner_service = service_fn(|req: Request<Full<Bytes>>| async move {
            // Verify session token was added
            assert_eq!(
                req.headers().get("X-Amz-Security-Token").unwrap(),
                "SESSION_TOKEN_EXAMPLE"
            );

            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body("OK".to_string())
                    .unwrap(),
            )
        });

        let builder = AwsSigningServiceBuilder::new("s3", "us-east-1", test_config_with_session());
        let mut signing_service = builder.build(inner_service);

        let request = Request::builder()
            .uri("https://s3.amazonaws.com/bucket/key")
            .method(Method::GET)
            .body(Full::from(Bytes::new()))
            .unwrap();

        let response = signing_service.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_disabled_builder_convenience() {
        let inner_service = service_fn(|_req: Request<Full<Bytes>>| async move {
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body("OK".to_string())
                    .unwrap(),
            )
        });

        // Test the convenience of creating a disabled builder
        let mut service1 = AwsSigningServiceBuilder::disabled().build(inner_service.clone());
        let mut service2 = AwsSigningServiceBuilder::disabled().build(inner_service);

        let req1 = Request::builder()
            .uri("https://api.example.com/v1")
            .body(Full::from(Bytes::new()))
            .unwrap();

        let req2 = Request::builder()
            .uri("https://api.example.com/v2")
            .body(Full::from(Bytes::new()))
            .unwrap();

        assert_eq!(service1.call(req1).await.unwrap().status(), StatusCode::OK);
        assert_eq!(service2.call(req2).await.unwrap().status(), StatusCode::OK);
    }
}

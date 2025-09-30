use crate::aws_api::config::AwsConfig;
use crate::exporters::http::client::{ResponseDecode, build_hyper_client};
use crate::exporters::http::tls::Config;
use crate::exporters::http::types::ContentEncoding;
use crate::exporters::shared::aws_signing_service::AwsSigningServiceBuilder;
use bytes::Bytes;
use flate2::Compression;
use flate2::bufread::GzEncoder;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Method, Request, Uri};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use serde_json::json;
use std::io::Read;
use tower::{BoxError, Service, ServiceExt};
use tracing::{debug, error, warn};

use super::{AwsEmfDecoder, AwsEmfResponse};

pub(crate) struct Cloudwatch {
    endpoint: Uri,
    base_headers: HeaderMap,
    log_group: String,
    log_stream: String,
    log_retention: u16,
    signing_builder: AwsSigningServiceBuilder,
    client: HyperClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
}

impl Cloudwatch {
    pub(crate) fn new(
        aws_config: AwsConfig,
        endpoint: Option<String>,
        log_group: String,
        log_stream: String,
        log_retention: u16,
    ) -> Result<Self, BoxError> {
        let endpoint_url =
            endpoint.unwrap_or_else(|| format!("https://logs.{}.amazonaws.com", aws_config.region));

        let endpoint: Uri = endpoint_url
            .parse()
            .map_err(|e| format!("Invalid CloudWatch endpoint: {}", e))?;

        let mut base_headers = HeaderMap::new();
        base_headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        base_headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-amz-json-1.1"),
        );

        let signing_builder =
            AwsSigningServiceBuilder::new("logs", &aws_config.region.clone(), aws_config);

        // Use the existing HTTP client builder
        let client = build_hyper_client(Config::default(), false)?;

        Ok(Self {
            endpoint,
            base_headers,
            log_group,
            log_stream,
            log_retention,
            signing_builder,
            client,
        })
    }

    /// Called when a resource (log group/stream) is not found
    /// Log stream/group are set statically for entire runtime, but we may want to support
    /// dynamic names in the future.
    pub(crate) async fn create_stream(&self) -> Result<(), BoxError> {
        debug!(
            "Attempting to create log stream: {} in group: {}",
            self.log_stream, self.log_group
        );

        match self
            .create_log_stream(&self.log_group, &self.log_stream)
            .await
        {
            Ok(_) => {
                debug!("Successfully created log stream: {}", self.log_stream);
                Ok(())
            }
            Err(e) => {
                if self.is_resource_not_found_error(&e) {
                    warn!(
                        "Log group not found, attempting to create: {}",
                        self.log_group
                    );

                    // Try to create the log group first
                    self.create_log_group(&self.log_group).await?;

                    // Set the retention policy for the newly created log group if not zero.
                    // Log groups default to never expire.
                    if self.log_retention != 0 {
                        self.set_log_retention(&self.log_group, self.log_retention)
                            .await?;
                    }

                    // Now try to create the log stream again
                    self.create_log_stream(&self.log_group, &self.log_stream)
                        .await?;

                    debug!("Successfully created log group and stream");
                    Ok(())
                } else {
                    error!("Failed to create log stream: {}", e);
                    Err(e)
                }
            }
        }
    }

    async fn set_log_retention(
        &self,
        log_group_name: &str,
        retention_in_days: u16,
    ) -> Result<(), BoxError> {
        let payload = json!({
            "logGroupName": log_group_name,
            "retentionInDays": retention_in_days
        });

        let mut headers = self.base_headers.clone();
        headers.insert(
            "X-Amz-Target",
            HeaderValue::from_static("Logs_20140328.PutRetentionPolicy"),
        );

        self.make_request(payload, headers).await
    }

    async fn create_log_stream(
        &self,
        log_group_name: &str,
        log_stream_name: &str,
    ) -> Result<(), BoxError> {
        let payload = json!({
            "logGroupName": log_group_name,
            "logStreamName": log_stream_name
        });

        let mut headers = self.base_headers.clone();
        headers.insert(
            "X-Amz-Target",
            HeaderValue::from_static("Logs_20140328.CreateLogStream"),
        );

        self.make_request(payload, headers).await
    }

    async fn create_log_group(&self, log_group_name: &str) -> Result<(), BoxError> {
        let payload = json!({
            "logGroupName": log_group_name
        });

        let mut headers = self.base_headers.clone();
        headers.insert(
            "X-Amz-Target",
            HeaderValue::from_static("Logs_20140328.CreateLogGroup"),
        );

        self.make_request(payload, headers).await
    }

    async fn make_request(
        &self,
        payload: serde_json::Value,
        headers: HeaderMap,
    ) -> Result<(), BoxError> {
        let payload_str = payload.to_string();
        let payload_bytes = Bytes::from(payload_str.into_bytes());

        // Compress the payload
        let mut gz_vec = Vec::new();
        let mut gz = GzEncoder::new(&payload_bytes[..], Compression::default());
        gz.read_to_end(&mut gz_vec)
            .map_err(|e| format!("Failed to compress request body: {}", e))?;
        let compressed_body = Bytes::from(gz_vec);

        // Build the unsigned request
        let mut req_builder = Request::builder()
            .uri(self.endpoint.clone())
            .method(Method::POST);

        let builder_headers = req_builder.headers_mut().unwrap();
        for (k, v) in headers.iter() {
            builder_headers.insert(k, v.clone());
        }

        let unsigned_request = req_builder.body(Full::from(compressed_body))?;

        // Create a signing service on-demand for this request
        // Wrap the hyper client in a Tower service using service_fn
        let client = self.client.clone();
        let client_service = tower::service_fn(move |req: Request<Full<Bytes>>| {
            let client = client.clone();
            async move {
                client
                    .request(req)
                    .await
                    .map_err(|e| -> BoxError { format!("Hyper client error: {}", e).into() })
            }
        });

        // Wrap with AWS signing service
        let mut signing_service = self.signing_builder.clone().build(client_service);

        // Sign and send the request through the signing service
        let response = signing_service
            .ready()
            .await?
            .call(unsigned_request)
            .await?;

        // Decode and handle the response
        self.handle_response(response, AwsEmfDecoder::default())
            .await
    }

    async fn handle_response<Dec>(
        &self,
        response: hyper::Response<hyper::body::Incoming>,
        decoder: Dec,
    ) -> Result<(), BoxError>
    where
        Dec: ResponseDecode<AwsEmfResponse>,
    {
        // Check the response status
        let status = response.status();
        if status.is_success() {
            debug!("CloudWatch API request successful: {}", status);
            Ok(())
        } else {
            let (head, body) = response.into_parts();

            let encoding = match head.headers.get(CONTENT_ENCODING) {
                None => ContentEncoding::None,
                Some(v) => match TryFrom::try_from(v) {
                    Ok(ce) => ce,
                    Err(e) => return Err(e),
                },
            };

            // Collect response body for error details
            let body_bytes = body
                .collect()
                .await
                .map_err(|e| format!("Failed to read response body: {}", e))?
                .to_bytes();

            // We are looking for the ResourceNotFoundException to identify if we need to create
            // the higher level resources, like log group. It's possible a resource already exists
            // if it was created by another thread, so don't fail those requests.
            //
            // We also translate resource not found into an error for easier handling.
            match decoder.decode(body_bytes, encoding) {
                Ok(AwsEmfResponse::ResourceNotFoundException(_)) => {
                    Err("ResourceNotFoundException".into())
                }
                Ok(AwsEmfResponse::ResourceAlreadyExistsException(_)) => Ok(()),
                Ok(r) => Err(format!("Unexpected error: {}", r).into()),
                Err(e) => Err(e),
            }
        }
    }

    fn is_resource_not_found_error(&self, error: &BoxError) -> bool {
        // Check if the error message matches ResourceNotFoundException
        let error_str = format!("{}", error);
        error_str.contains("ResourceNotFoundException")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws_api::config::AwsConfig;
    use crate::crypto::init_crypto_provider;

    fn sample_aws_config() -> AwsConfig {
        AwsConfig {
            region: "us-east-1".to_string(),
            aws_access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            aws_secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            aws_session_token: None,
        }
    }

    #[test]
    fn test_is_resource_not_found_error() {
        init_crypto_provider().expect("Failed to init crypto");
        let config = sample_aws_config();
        let cw = Cloudwatch::new(config, None, String::new(), String::new(), 0).unwrap();

        let error: BoxError = "ResourceNotFoundException: Log group does not exist".into();
        assert!(cw.is_resource_not_found_error(&error));

        let error: BoxError = "Some other error".into();
        assert!(!cw.is_resource_not_found_error(&error));
    }
}

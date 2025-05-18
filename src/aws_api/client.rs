use crate::aws_api::config::AwsConfig;
use crate::aws_api::error::Error;
use crate::aws_api::paramstore::ParameterStore;
use crate::aws_api::secretsmanager::SecretsManager;
use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_rustls::ConfigBuilderExt;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use rustls::ClientConfig;
use std::time::Duration;
use tower::BoxError;

/// Main client for AWS services
pub struct AwsClient {
    pub(crate) config: AwsConfig,
    client: HyperClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
}

impl AwsClient {
    /// Create a new AWS client
    pub fn new(config: AwsConfig) -> Result<Self, BoxError> {
        let client = build_hyper_client()?;

        Ok(Self { client, config })
    }

    /// Get an instance of the SecretsManager service
    pub fn secrets_manager(&self) -> SecretsManager {
        SecretsManager::new(self)
    }

    /// Get an instance of the ParameterStore service
    pub fn parameter_store(&self) -> ParameterStore {
        ParameterStore::new(self)
    }

    pub async fn perform(&self, req: Request<Full<Bytes>>) -> Result<Bytes, Error> {
        let resp = self.client.request(req).await?;

        // Handle AWS errors
        let (parts, body) = resp.into_parts();
        if !parts.status.is_success() {
            let error_body = response_string(body).await?;

            return Err(Error::AwsError {
                code: parts.status.as_str().to_string(),
                message: error_body,
            });
        }

        // Parse success response
        Ok(body.collect().await?.to_bytes())
    }
}

pub async fn response_string(body: Incoming) -> Result<String, BoxError> {
    Ok(body
        .collect()
        .await
        .map_err(|e| format!("Failed to read response {}", e))
        .map(|c| c.to_bytes())
        .map(|s| String::from_utf8(s.to_vec()))?
        .map_err(|e| format!("Unable to convert response body to string: {}", e))?)
}

fn build_hyper_client() -> Result<HyperClient<HttpsConnector<HttpConnector>, Full<Bytes>>, BoxError>
{
    let tls_config = ClientConfig::builder()
        .with_native_roots()?
        .with_no_client_auth();

    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_http2()
        .build();

    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(2)
        .timer(TokioTimer::new())
        .build::<_, Full<Bytes>>(https);

    Ok(client)
}

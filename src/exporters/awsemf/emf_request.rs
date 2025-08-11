// SPDX-License-Identifier: Apache-2.0

use crate::aws_api::auth::{AwsRequestSigner, SystemClock};
use crate::aws_api::config::AwsConfig;
use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Method, Request, Uri};
use http_body_util::Full;
use serde_json::{Value, json};
use std::error::Error;
use tower::BoxError;

fn build_url(endpoint: &url::Url, path: &str) -> url::Url {
    endpoint.join(path).unwrap()
}

#[derive(Clone)]
pub struct AwsEmfRequestBuilder {
    signer: AwsRequestSigner<SystemClock>,
    pub base_headers: HeaderMap,
    pub uri: Uri,
    pub log_group_name: String,
    pub log_stream_name: String,
}

impl AwsEmfRequestBuilder {
    pub fn new(
        endpoint: String,
        config: AwsConfig,
        log_group_name: String,
        log_stream_name: Option<String>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let uri: url::Url = match endpoint.parse() {
            Ok(u) => u,
            Err(e) => return Err(format!("failed to parse endpoint {}: {}", endpoint, e).into()),
        };

        let logs_url = build_url(&uri, "/");
        let uri: Uri = logs_url.to_string().parse()?;
        let mut base_headers = HeaderMap::new();
        base_headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        base_headers.insert(
            "X-Amz-Target",
            HeaderValue::from_static("Logs_20140328.PutLogEvents"),
        );
        base_headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-amz-json-1.1"),
        );

        let signer =
            AwsRequestSigner::new("logs", config.region.clone().as_str(), config, SystemClock);

        // Generate default log stream name if not provided
        let log_stream_name =
            log_stream_name.unwrap_or_else(|| format!("rotel-{}", uuid::Uuid::new_v4().simple()));

        let s = Self {
            uri,
            base_headers,
            signer,
            log_group_name,
            log_stream_name,
        };
        Ok(s)
    }

    pub fn build(&self, payload: Vec<Value>) -> Result<Vec<Request<Full<Bytes>>>, BoxError> {
        let mut log_events = Vec::new();

        println!("AWS_EMF: {}", json!(payload).to_string());

        for emf_log in payload {
            log_events.push(json!({
                "timestamp": chrono::Utc::now().timestamp_millis(),
                "message": emf_log.to_string()
            }));
        }

        let data = json!({
            "logGroupName": self.log_group_name,
            "logStreamName": self.log_stream_name,
            "logEvents": log_events
        })
        .to_string();

        let data = Bytes::from(data.into_bytes());

        let signed_request = self.signer.sign(
            self.uri.clone(),
            Method::POST,
            self.base_headers.clone(),
            data,
        );

        match signed_request {
            Ok(r) => Ok(vec![r]),
            Err(e) => Err(Box::new(e)),
        }
    }
}

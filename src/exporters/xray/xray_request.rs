// SPDX-License-Identifier: Apache-2.0

use crate::aws_api::auth::{AwsRequestSigner, SystemClock};
use crate::aws_api::config::AwsConfig;
use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Method, Request, Uri};
use http_body_util::Full;
use serde_json::{json, Value};
use std::error::Error;
use tower::BoxError;

const TRACES_PATH: &str = "/TraceSegments";

fn build_url(endpoint: &url::Url, path: &str) -> url::Url {
    endpoint.join(path).unwrap()
}

#[derive(Clone)]
pub struct XRayRequestBuilder<'a> {
    signer: AwsRequestSigner<'a, SystemClock>,
    pub base_headers: HeaderMap,
    pub uri: Uri,
}

impl<'a> XRayRequestBuilder<'a> {
    pub fn new(
        endpoint: String,
        config: &'a AwsConfig,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let uri: url::Url = match endpoint.parse() {
            Ok(u) => u,
            Err(e) => return Err(format!("failed to parse endpoint {}: {}", endpoint, e).into()),
        };

        let trace_url = build_url(&uri, TRACES_PATH);
        let uri: Uri = trace_url.to_string().parse()?;
        let mut base_headers = HeaderMap::new();
        base_headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        base_headers.insert(
            "X-Amz-Target",
            HeaderValue::from_static("XRay_20160712.PutTraceSegments"),
        );
        base_headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-amz-json-1.1"),
        );
        let signer = AwsRequestSigner::new(
            "xray",
            config.region.as_str(),
            config.aws_access_key_id.as_str(),
            config.aws_secret_access_key.as_str(),
            config.aws_session_token.as_deref(),
            SystemClock,
        );

        let s = Self {
            uri,
            base_headers,
            signer,
        };
        Ok(s)
    }

    pub fn build(&self, payload: Vec<Value>) -> Result<Request<Full<Bytes>>, BoxError> {
        // Convert each segment Value to a string
        let segment_strings: Vec<String> = payload
            .into_iter()
            .map(|segment| segment.to_string())
            .collect();

        let data = json!({
        "TraceSegmentDocuments": segment_strings
        });

        let signed_request = self.signer.sign(
            self.uri.clone(),
            Method::POST,
            self.base_headers.clone(),
            data.to_string().into_bytes(),
        );
        match signed_request {
            Ok(r) => Ok(r),
            Err(e) => Err(Box::new(e)),
        }
    }
}

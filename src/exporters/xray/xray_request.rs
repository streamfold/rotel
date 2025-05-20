// SPDX-License-Identifier: Apache-2.0

use crate::aws_api::auth::{AwsRequestSigner, SystemClock};
use crate::aws_api::config::AwsConfig;
use crate::exporters::xray::Region;
use bytes::Bytes;
use flate2::Compression;
use flate2::read::GzEncoder;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Method, Request, Uri};
use http_body_util::Full;
use serde_json::{Value, json};
use std::error::Error;
use std::io::Read;
use tower::BoxError;

const TRACES_PATH: &str = "/TraceSegments";

fn build_url(endpoint: &url::Url, path: &str) -> url::Url {
    endpoint.join(path).unwrap()
}

#[derive(Clone)]
pub struct XRayRequestBuilder<'a> {
    signer: AwsRequestSigner<'a, SystemClock>,
    pub region: Region,
    pub trace_url: url::Url,
    pub base_headers: HeaderMap,
    pub uri: Uri,
}

impl<'a> XRayRequestBuilder<'a> {
    pub fn new(
        endpoint: String,
        region: Region,
        config: &'a AwsConfig,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let uri: url::Url = match endpoint.parse() {
            Ok(u) => u,
            Err(e) => return Err(format!("failed to parse endpoint {}: {}", endpoint, e).into()),
        };

        let trace_url = build_url(&uri, TRACES_PATH);
        let uri: hyper::Uri = trace_url.to_string().parse()?;
        let mut base_headers = HeaderMap::new();
        base_headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        base_headers.insert(
            "X-Amz-Target",
            HeaderValue::from_static("xray.PutTraceSegments"),
        );
        base_headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-amz-json-1.1"),
        );

        // Sign the request
        let signer = AwsRequestSigner::new(
            "ec2",
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
            region,
            trace_url,
        };
        Ok(s)
    }

    pub fn build(&self, payload: Vec<Value>) -> Result<Request<Full<Bytes>>, BoxError> {
        let data = json!({
            "TraceSegmentDocuments": payload,
        });
        let data = serde_json::to_vec(&data)?;
        let mut gz_vec = Vec::new();
        let mut gz = GzEncoder::new(&data[..], Compression::default());
        gz.read_to_end(&mut gz_vec).unwrap();

        let signed_request = self.signer.sign(
            self.uri.clone(),
            Method::POST,
            self.base_headers.clone(),
            gz_vec,
        );
        match signed_request {
            Ok(r) => Ok(r),
            Err(e) => Err(Box::new(e)),
        }
    }
}

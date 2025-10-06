// SPDX-License-Identifier: Apache-2.0

use crate::aws_api::auth::{AwsRequestSigner, SystemClock};
use crate::aws_api::config::AwsConfig;
use crate::exporters::xray::payload::XRayPayload;
use crate::topology::payload::MessageMetadata;
use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Method, Request, Uri};
use serde_json::{Value, json};
use std::error::Error;
use tower::BoxError;

const TRACES_PATH: &str = "/TraceSegments";

fn build_url(endpoint: &url::Url, path: &str) -> url::Url {
    endpoint.join(path).unwrap()
}

#[derive(Clone)]
pub struct XRayRequestBuilder {
    signer: AwsRequestSigner<SystemClock>,
    pub base_headers: HeaderMap,
    pub uri: Uri,
}

impl XRayRequestBuilder {
    pub fn new(endpoint: String, config: AwsConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
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

        let signer =
            AwsRequestSigner::new("xray", config.region.clone().as_str(), config, SystemClock);

        let s = Self {
            uri,
            base_headers,
            signer,
        };
        Ok(s)
    }

    pub fn build(
        &self,
        payload: Vec<Value>,
        metadata: Option<Vec<MessageMetadata>>,
    ) -> Result<Vec<Request<XRayPayload>>, BoxError> {
        // Convert each segment Value to a string
        let segment_strings: Vec<String> = payload
            .into_iter()
            .map(|segment| segment.to_string())
            .collect();

        let data = json!({
        "TraceSegmentDocuments": segment_strings
        })
        .to_string();
        let data = Bytes::from(data.into_bytes());

        // NOTE: Unfortunately we need to clone the data here for AWS request signing.
        // The AWS signer requires raw Bytes to calculate the signature, but we need to
        // wrap the data in XRayPayload to carry metadata through the HTTP pipeline.
        //
        // Future improvement: Consider modifying the AWS signer to accept a generic Body
        // that can provide both the raw bytes for signing AND carry additional metadata,
        // eliminating the need for this clone.
        let signed_request = self.signer.sign(
            self.uri.clone(),
            Method::POST,
            self.base_headers.clone(),
            data.clone(), // TODO: Eliminate this clone by updating signer interface
        );

        match signed_request {
            Ok(request) => {
                // Extract parts from the signed request and rebuild with XRayPayload
                let (parts, _) = request.into_parts();

                // Move the data (already cloned above) into XRayPayload with metadata
                let xray_payload = XRayPayload::new(data, metadata);

                // Rebuild request with XRayPayload
                let new_request = Request::from_parts(parts, xray_payload);
                Ok(vec![new_request])
            }
            Err(e) => Err(Box::new(e)),
        }
    }
}

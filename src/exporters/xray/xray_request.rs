// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Method, Request, Uri};
use http_body_util::Full;
use serde_json::{Value, json};
use std::error::Error;
use tower::BoxError;

use crate::exporters::xray::XRayPayload;
use crate::topology::payload::MessageMetadata;

const TRACES_PATH: &str = "/TraceSegments";

const MAX_SPANS: usize = 50;

fn build_url(endpoint: &url::Url, path: &str) -> url::Url {
    endpoint.join(path).unwrap()
}

#[derive(Clone)]
pub struct XRayRequestBuilder {
    pub base_headers: HeaderMap,
    pub uri: Uri,
}

impl XRayRequestBuilder {
    pub fn new(endpoint: String) -> Result<Self, Box<dyn Error + Send + Sync>> {
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

        let s = Self { uri, base_headers };
        Ok(s)
    }

    pub fn build(
        &self,
        payload: Vec<Value>,
        mut metadata: Option<Vec<MessageMetadata>>,
    ) -> Result<Vec<Request<XRayPayload>>, BoxError> {
        let mut requests = Vec::new();
        let num_chunks = (payload.len() + MAX_SPANS - 1) / MAX_SPANS;

        // X-Ray API limits to 50 spans max
        for (idx, chunk) in payload.chunks(MAX_SPANS).enumerate() {
            let segments: Vec<String> = chunk
                .into_iter()
                .map(|segment| segment.to_string())
                .collect();

            let data = json!({
                "TraceSegmentDocuments": segments
            })
            .to_string();
            let data = Bytes::from(data.into_bytes());

            let mut req_builder = Request::builder().uri(&self.uri).method(Method::POST);

            let builder_headers = req_builder.headers_mut().unwrap();
            for (k, v) in self.base_headers.iter() {
                builder_headers.insert(k, v.clone());
            }

            let req = req_builder.body(Full::from(data))?;

            // Decompose the request to get parts and body
            let (parts, body) = req.into_parts();

            // Only clone metadata when there are multiple batches, otherwise move it
            let batch_metadata = if num_chunks == 1 {
                metadata.take()
            } else if idx == 0 {
                metadata.clone()
            } else {
                None
            };

            // Create MessagePayload with just the body
            let payload = XRayPayload::new(body, batch_metadata);

            // Reconstruct request with the payload as the body
            let wrapped_request = Request::from_parts(parts, payload);

            requests.push(wrapped_request);
        }

        Ok(requests)
    }
}

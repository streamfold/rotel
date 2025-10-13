// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Method, Request, Uri};
use http_body_util::Full;
use serde_json::json;
use std::error::Error;
use tower::BoxError;

use crate::exporters::xray::XRayPayload;
use crate::exporters::xray::transformer::XRayValuePayload;

const TRACES_PATH: &str = "/TraceSegments";

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
        payloads: Vec<XRayValuePayload>,
    ) -> Result<Vec<Request<XRayPayload>>, BoxError> {
        let mut requests = Vec::new();

        // Iterate through payloads and create a request for each
        for mut payload in payloads {
            // The payload.value is already an array of span documents
            let segments = if let serde_json::Value::Array(spans) = payload.value {
                spans.into_iter().map(|s| s.to_string()).collect::<Vec<_>>()
            } else {
                return Err("Expected array of spans in XRayValuePayload".into());
            };

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

            // Move metadata from payload into the request
            let batch_metadata = payload.metadata.take().map(|m| vec![m]);
            let xray_payload = XRayPayload::new(body, batch_metadata);

            // Reconstruct request with the payload as the body
            let wrapped_request = Request::from_parts(parts, xray_payload);

            requests.push(wrapped_request);
        }

        Ok(requests)
    }
}

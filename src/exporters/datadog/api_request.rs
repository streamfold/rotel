// SPDX-License-Identifier: Apache-2.0

use crate::exporters::datadog::DatadogPayload;
use crate::exporters::datadog::types::pb::AgentPayload;
use crate::exporters::http::request::{BaseRequestBuilder, RequestUri};
use crate::topology::payload::MessageMetadata;
use bytes::Bytes;
use flate2::Compression;
use flate2::read::GzEncoder;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Request};
use prost::Message;
use std::error::Error;
use std::io::Read;
use tower::BoxError;

const TRACES_PATH: &str = "/api/v0.2/traces";

fn build_url(endpoint: &url::Url, path: &str) -> url::Url {
    endpoint.join(path).unwrap()
}

#[derive(Clone)]
pub struct ApiRequestBuilder {
    base: BaseRequestBuilder<DatadogPayload>,
}

impl ApiRequestBuilder {
    pub fn new(endpoint: String, api_key: String) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let uri: url::Url = match endpoint.parse() {
            Ok(u) => u,
            Err(e) => return Err(format!("failed to parse endpoint {}: {}", endpoint, e).into()),
        };

        let trace_url = build_url(&uri, TRACES_PATH);

        let mut base_headers = HeaderMap::new();
        base_headers.insert("DD-API-KEY", api_key.parse()?);

        // these might change with different routes in the future
        base_headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-protobuf"),
        );
        base_headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));

        let base = BaseRequestBuilder::new(Some(RequestUri::Post(trace_url)), base_headers);

        let s = Self { base };
        Ok(s)
    }

    pub fn build(
        &self,
        payload: AgentPayload,
        metadata: Option<Vec<MessageMetadata>>,
    ) -> Result<Vec<Request<DatadogPayload>>, BoxError> {
        let mut buf = Vec::new();
        if let Err(e) = payload.encode(&mut buf) {
            // todo: We pass these on as errors which the final service immediately returns,
            // because matching the Future types makes this pretty gnarly. Identify a way to
            // wrap error types so that we can return an immediate error here.
            return Err(format!("failed to convert protobuf: {}", e).into());
        }

        let mut gz_vec = Vec::new();
        let mut gz = GzEncoder::new(&buf[..], Compression::default());
        gz.read_to_end(&mut gz_vec).unwrap();

        let body = Bytes::from(gz_vec);

        // Create MessagePayload with just the body
        let datadog_payload = DatadogPayload::new(http_body_util::Full::new(body), metadata);

        // Use the base builder to create the final request with proper headers
        let wrapped_request = self.base.builder().body(datadog_payload)?.build()?;

        Ok(vec![wrapped_request])
    }
}

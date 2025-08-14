// SPDX-License-Identifier: Apache-2.0

use crate::aws_api::auth::{AwsRequestSigner, SystemClock};
use crate::aws_api::config::AwsConfig;
use bytes::Bytes;
use flate2::Compression;
use flate2::bufread::GzEncoder;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Method, Request, Uri};
use http_body_util::Full;
use serde_json::json;
use std::error::Error;
use std::io::Read;
use tower::BoxError;

use super::event::{Event, EventBatch};

fn build_url(endpoint: &url::Url, path: &str) -> url::Url {
    endpoint.join(path).unwrap()
}

#[derive(Clone)]
pub struct AwsEmfRequestBuilder {
    signer: AwsRequestSigner<SystemClock>,
    base_headers: HeaderMap,
    uri: Uri,
    log_group_name: String,
    log_stream_name: String,
}

impl AwsEmfRequestBuilder {
    pub fn new(
        endpoint: String,
        config: AwsConfig,
        log_group_name: String,
        log_stream_name: String,
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

        let s = Self {
            uri,
            base_headers,
            signer,
            log_group_name,
            log_stream_name,
        };
        Ok(s)
    }

    pub fn build(&self, events: Vec<Event>) -> Result<Vec<Request<Full<Bytes>>>, BoxError> {
        if events.is_empty() {
            return Ok(vec![]);
        }

        let mut events = events;
        // CW requires events are in sorted timestamp order. We don't implement Ord because
        // the sorting only applies for this use case of submitting to Cloudwatch
        events.sort_by(|a, b| a.timestamp_ms.cmp(&b.timestamp_ms));

        let mut batches = Vec::new();
        let mut curr_batch = EventBatch::new();
        for evt in events {
            if let Some(e) = curr_batch.add_event(evt) {
                // Event didn't fit, start new batch
                batches.push(curr_batch);

                curr_batch = EventBatch::new();
                curr_batch.add_event(e);
            }
        }
        batches.push(curr_batch);

        let mut reqs = Vec::with_capacity(batches.len());
        for batch in batches {
            let data = json!({
                "logGroupName": self.log_group_name,
                "logStreamName": self.log_stream_name,
                "logEvents": batch.get_events(),
            })
            .to_string();

            let data = Bytes::from(data.into_bytes());

            let mut gz_vec = Vec::new();
            let mut gz = GzEncoder::new(&data[..], Compression::default());
            gz.read_to_end(&mut gz_vec).unwrap();

            let body = Bytes::from(gz_vec);

            let signed_request = self.signer.sign(
                self.uri.clone(),
                Method::POST,
                self.base_headers.clone(),
                body,
            );

            let r = match signed_request {
                Ok(r) => r,
                Err(e) => return Err(Box::new(e)),
            };

            reqs.push(r);
        }

        Ok(reqs)
    }
}

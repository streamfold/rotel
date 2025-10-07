// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::metadata_extractor::MetadataExtractor;
use crate::topology::payload::MessageMetadata;
use bytes::Bytes;
use http::Request;
use http_body_util::Full;
use hyper::body::{Body, Frame, SizeHint};
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::BoxError;

/// AWS EMF payload wrapper that carries metadata through the HTTP request pipeline
#[derive(Clone)]
pub struct AwsEmfPayload {
    // Store the entire signed request to avoid cloning data
    inner: Option<Full<Bytes>>,
    pub metadata: Option<Vec<MessageMetadata>>,
}

impl AwsEmfPayload {
    /// Create a new AwsEmfPayload from a signed request
    pub fn from_signed_request(
        signed_request: Request<Full<Bytes>>,
        metadata: Option<Vec<MessageMetadata>>,
    ) -> Request<AwsEmfPayload> {
        let (parts, body) = signed_request.into_parts();
        let payload = Self {
            inner: Some(body),
            metadata,
        };
        Request::from_parts(parts, payload)
    }
}

impl MetadataExtractor for AwsEmfPayload {
    fn take_metadata(&mut self) -> Option<Vec<MessageMetadata>> {
        self.metadata.take()
    }
}

// Implement hyper::body::Body for AwsEmfPayload
impl Body for AwsEmfPayload {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        // Delegate to the inner Full<Bytes> body
        match &mut self.inner {
            Some(inner) => {
                // Pin the inner body and poll it
                let inner_pin = Pin::new(inner);
                inner_pin
                    .poll_frame(cx)
                    .map_err(|e| Box::new(e) as BoxError)
            }
            None => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.inner {
            Some(inner) => inner.is_end_stream(),
            None => true,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.inner {
            Some(inner) => inner.size_hint(),
            None => SizeHint::with_exact(0),
        }
    }
}

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

/// OTLP payload wrapper that carries metadata through the HTTP request pipeline
#[derive(Clone)]
pub struct OtlpPayload {
    // Store the body content to avoid cloning data
    inner: Option<Full<Bytes>>,
    pub metadata: Option<Vec<MessageMetadata>>,
    pub size: usize,
}

impl OtlpPayload {
    /// Create a new OtlpPayload from a request with metadata
    pub fn from_request_with_metadata(
        request: Request<Full<Bytes>>,
        metadata: Option<Vec<MessageMetadata>>,
        size: usize,
    ) -> Request<OtlpPayload> {
        let (parts, body) = request.into_parts();
        let payload = Self {
            inner: Some(body),
            metadata,
            size,
        };
        Request::from_parts(parts, payload)
    }

    /// Create a new OtlpPayload with data and metadata
    pub fn new(data: Bytes, metadata: Option<Vec<MessageMetadata>>, size: usize) -> Self {
        Self {
            inner: Some(Full::new(data)),
            metadata,
            size,
        }
    }
}

impl MetadataExtractor for OtlpPayload {
    fn take_metadata(&mut self) -> Option<Vec<MessageMetadata>> {
        self.metadata.take()
    }
}

// Implement hyper::body::Body for OtlpPayload
impl Body for OtlpPayload {
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

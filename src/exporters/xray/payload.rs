// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::metadata_extractor::MetadataExtractor;
use crate::topology::payload::MessageMetadata;
use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::BoxError;

/// XRay payload wrapper that carries metadata through the HTTP request pipeline
#[derive(Clone)]
pub struct XRayPayload {
    data: Option<Bytes>,
    pub metadata: Option<Vec<MessageMetadata>>,
}

impl XRayPayload {
    pub fn new(data: Bytes, metadata: Option<Vec<MessageMetadata>>) -> Self {
        Self {
            data: Some(data),
            metadata,
        }
    }

    pub fn empty() -> Self {
        Self {
            data: None,
            metadata: None,
        }
    }
}

impl MetadataExtractor for XRayPayload {
    fn take_metadata(&mut self) -> Option<Vec<MessageMetadata>> {
        self.metadata.take()
    }
}

// Implement hyper::body::Body for XRayPayload
impl Body for XRayPayload {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        // Take the data once and return it
        match self.data.take() {
            Some(data) => Poll::Ready(Some(Ok(Frame::data(data)))),
            None => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        match &self.data {
            Some(data) => {
                let size = data.len() as u64;
                SizeHint::with_exact(size)
            }
            None => SizeHint::with_exact(0),
        }
    }
}

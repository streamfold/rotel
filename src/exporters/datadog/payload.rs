// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::metadata_extractor::MetadataExtractor;
use crate::topology::payload::MessageMetadata;
use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::BoxError;

/// A wrapper type for Datadog request payloads that preserves message metadata
/// while implementing the Body trait for HTTP requests.
#[derive(Clone)]
pub struct DatadogPayload {
    /// The actual compressed protobuf data to send
    data: Option<Bytes>,
    /// Metadata from the original messages for acknowledgment
    pub metadata: Option<Vec<MessageMetadata>>,
}

impl DatadogPayload {
    /// Create a new DatadogPayload with data and metadata
    pub fn new(data: Bytes, metadata: Option<Vec<MessageMetadata>>) -> Self {
        Self {
            data: Some(data),
            metadata,
        }
    }

    /// Create an empty payload (used for certain error cases)
    pub fn empty() -> Self {
        Self {
            data: None,
            metadata: None,
        }
    }
}

impl Body for DatadogPayload {
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

impl Default for DatadogPayload {
    fn default() -> Self {
        Self::empty()
    }
}

impl MetadataExtractor for DatadogPayload {
    fn take_metadata(&mut self) -> Option<Vec<MessageMetadata>> {
        self.metadata.take()
    }
}

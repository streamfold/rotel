// SPDX-License-Identifier: Apache-2.0

use crate::topology::payload::MessageMetadata;

/// Trait for extracting metadata from request bodies
pub trait MetadataExtractor {
    fn take_metadata(&mut self) -> Option<Vec<MessageMetadata>>;
}

/// Generic message payload that stores a body with metadata
pub struct MessagePayload<ReqBody> {
    body: ReqBody,
    metadata: Option<Vec<MessageMetadata>>,
}

impl<ReqBody> MessagePayload<ReqBody> {
    /// Create a new MessagePayload from just a body with metadata
    pub fn new(body: ReqBody, metadata: Option<Vec<MessageMetadata>>) -> Self {
        Self { body, metadata }
    }
}

/// Custom Clone implementation for retry scenarios
/// Clones the body but shares metadata without incrementing ref count
impl<ReqBody> Clone for MessagePayload<ReqBody>
where
    ReqBody: Clone,
{
    fn clone(&self) -> Self {
        Self {
            body: self.body.clone(),
            metadata: self.metadata.as_ref().map(|vec| {
                // Share the same metadata instances without cloning them
                // This avoids incrementing the reference count for retry scenarios
                vec.iter().map(|meta| meta.shallow_clone()).collect()
            }),
        }
    }
}

impl<ReqBody> MetadataExtractor for MessagePayload<ReqBody> {
    fn take_metadata(&mut self) -> Option<Vec<MessageMetadata>> {
        self.metadata.take()
    }
}

// Implement Body for MessagePayload to delegate to the body directly
impl<ReqBody> hyper::body::Body for MessagePayload<ReqBody>
where
    ReqBody: hyper::body::Body + Clone + Unpin,
{
    type Data = ReqBody::Data;
    type Error = ReqBody::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        // Poll the body directly
        let body_pin = std::pin::Pin::new(&mut self.body);
        body_pin.poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

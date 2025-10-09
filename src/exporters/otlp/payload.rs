// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::metadata_extractor::{MessagePayload, MetadataExtractor};
use crate::topology::payload::MessageMetadata;
use bytes::Bytes;
use http_body_util::Full;

/// OTLP payload that includes size tracking for metrics
#[derive(Clone)]
pub struct OtlpPayload {
    inner: MessagePayload<Full<Bytes>>,
    pub size: usize,
}

impl OtlpPayload {
    /// Create a new OtlpPayload from just a body with metadata and size
    pub fn new(body: Full<Bytes>, metadata: Option<Vec<MessageMetadata>>, size: usize) -> Self {
        let inner = MessagePayload::new(body, metadata);
        Self { inner, size }
    }
}

impl MetadataExtractor for OtlpPayload {
    fn take_metadata(&mut self) -> Option<Vec<MessageMetadata>> {
        self.inner.take_metadata()
    }
}

// Implement Body for OtlpPayload to delegate to the inner MessagePayload
impl hyper::body::Body for OtlpPayload {
    type Data = <MessagePayload<Full<Bytes>> as hyper::body::Body>::Data;
    type Error = <MessagePayload<Full<Bytes>> as hyper::body::Body>::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let inner_pin = std::pin::Pin::new(&mut self.inner);
        inner_pin.poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::body::Body;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    struct MockWaker;
    impl std::task::Wake for MockWaker {
        fn wake(self: std::sync::Arc<Self>) {}
    }

    #[test]
    fn test_body_consumption_end_stream_behavior() {
        let data = Bytes::from("test data");
        let mut payload = OtlpPayload::new(Full::new(data), None, 9);

        // Initially should not be end of stream
        assert!(
            !payload.is_end_stream(),
            "Should not be end of stream initially"
        );

        let waker = Waker::from(std::sync::Arc::new(MockWaker));
        let mut ctx = Context::from_waker(&waker);

        // Pin the payload for polling
        let mut pinned = Pin::new(&mut payload);

        // Poll the first frame
        match pinned.as_mut().poll_frame(&mut ctx) {
            Poll::Ready(Some(Ok(_frame))) => {
                // Full<Bytes> sends one frame and then reports end of stream
                assert!(
                    pinned.is_end_stream(),
                    "Should be end of stream after Full<Bytes> sends its frame"
                );
            }
            other => panic!("Expected to get a frame, got: {:?}", other),
        }

        // Next poll should return None
        match pinned.as_mut().poll_frame(&mut ctx) {
            Poll::Ready(None) => {
                assert!(
                    pinned.is_end_stream(),
                    "Should still be end of stream after None"
                );
            }
            other => panic!("Expected None, got: {:?}", other),
        }
    }
}

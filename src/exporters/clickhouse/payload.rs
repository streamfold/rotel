use crate::exporters::clickhouse::ch_error::Error;
use crate::exporters::clickhouse::rowbinary::serialize_into;
use crate::exporters::clickhouse::{BUFFER_SIZE, Compression, MIN_CHUNK_SIZE, compression};
use crate::exporters::http::metadata_extractor::MetadataExtractor;
use crate::topology::payload::MessageMetadata;
use bytes::{Bytes, BytesMut};
use hyper::body::{Body, Frame};
use serde::Serialize;
use std::collections::VecDeque;
use std::mem;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tower::BoxError;

pub struct ClickhousePayloadBuilder {
    curr_chunk: BytesMut,
    closed: Vec<Bytes>,
    pub compression: Compression,
}

impl ClickhousePayloadBuilder {
    pub(crate) fn new(compression: Compression) -> Self {
        Self {
            curr_chunk: BytesMut::with_capacity(BUFFER_SIZE),
            closed: Vec::new(),
            compression,
        }
    }
}

impl ClickhousePayloadBuilder {
    pub(crate) fn add_row(&mut self, row: &impl Serialize) -> Result<usize, BoxError> {
        let old_size = self.curr_chunk.len();
        serialize_into(&mut self.curr_chunk, &row)?;
        let written = self.curr_chunk.len() - old_size;

        if self.curr_chunk.len() >= MIN_CHUNK_SIZE {
            let new_chunk = self.take_and_close_current()?;
            self.closed.push(new_chunk);
        }

        Ok(written)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.curr_chunk.is_empty() && self.closed.is_empty()
    }

    pub(crate) fn finish_with_metadata(
        mut self,
        metadata: Option<Vec<MessageMetadata>>,
    ) -> Result<ClickhousePayload, BoxError> {
        if !self.curr_chunk.is_empty() {
            let new_chunk = self.take_and_close_current()?;
            self.closed.push(new_chunk);
        }

        Ok(ClickhousePayload::new_with_metadata(self.closed, metadata))
    }

    fn take_and_close_current(&mut self) -> Result<Bytes, Error> {
        Ok(if self.compression == Compression::Lz4 {
            let compressed = compression::lz4::compress(&self.curr_chunk)?;
            self.curr_chunk.clear();
            compressed
        } else {
            mem::replace(&mut self.curr_chunk, BytesMut::with_capacity(BUFFER_SIZE)).freeze()
        })
    }
}

pub struct ClickhousePayload {
    inner: Arc<Mutex<Inner>>,
    metadata: Option<Vec<MessageMetadata>>,
}

// We manually clone so that the state of the inner struct is maintained
// per request. Otherwise we won't replay all chunks on a retry.
impl Clone for ClickhousePayload {
    fn clone(&self) -> Self {
        let inner = self.inner.lock().unwrap();
        Self {
            inner: Arc::new(Mutex::new(inner.clone())),
            metadata: self.metadata.clone(),
        }
    }
}

#[derive(Clone)]
pub struct Inner {
    chunks: Arc<VecDeque<Bytes>>,
    current: usize,
}

impl ClickhousePayload {
    pub fn new_with_metadata(chunks: Vec<Bytes>, metadata: Option<Vec<MessageMetadata>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                chunks: Arc::new(chunks.into()),
                current: 0,
            })),
            metadata,
        }
    }
}

impl Body for ClickhousePayload {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut inner = self.inner.lock().unwrap();

        if inner.current >= inner.chunks.len() {
            return Poll::Ready(None);
        }

        let curr = inner.current;
        inner.current += 1;

        Poll::Ready(Some(Ok(Frame::data(inner.chunks[curr].clone()))))
    }
}

impl MetadataExtractor for ClickhousePayload {
    fn take_metadata(&mut self) -> Option<Vec<MessageMetadata>> {
        self.metadata.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Waker};

    // Helper function to create a simple test row
    #[derive(Serialize)]
    struct TestRow {
        id: u32,
        name: String,
    }

    struct MockWaker;
    impl std::task::Wake for MockWaker {
        fn wake(self: Arc<Self>) {}
    }

    #[test]
    fn test_clone_independence() {
        // Create a payload builder and add some test data
        let mut builder = ClickhousePayloadBuilder::new(Compression::None);

        // Add enough data to create multiple chunks
        for i in 0..2 {
            let row = TestRow {
                id: i,
                name: format!("test_{}", i),
            };
            builder.add_row(&row).unwrap();
        }

        let original_payload = builder.finish_with_metadata(None).unwrap();
        let cloned_payload = original_payload.clone();

        // Pin the payloads for polling
        let mut original_pinned = Box::pin(original_payload);
        let mut cloned_pinned = Box::pin(cloned_payload);

        let waker = Waker::from(std::sync::Arc::new(MockWaker));
        let mut ctx = Context::from_waker(&waker);

        // Poll the original payload once to advance its state
        let original_result = original_pinned.as_mut().poll_frame(&mut ctx);

        // Verify we got a frame from the original
        assert!(matches!(original_result, Poll::Ready(Some(Ok(_)))));

        // Poll the cloned payload - it should start from the beginning
        let cloned_result = cloned_pinned.as_mut().poll_frame(&mut ctx);

        // Verify we got a frame from the clone
        assert!(matches!(cloned_result, Poll::Ready(Some(Ok(_)))));

        // Extract the frames to compare
        if let (Poll::Ready(Some(Ok(original_frame))), Poll::Ready(Some(Ok(cloned_frame)))) =
            (original_result, cloned_result)
        {
            // Both should return the same first chunk since the clone starts fresh
            let original_data = original_frame.into_data().unwrap();
            let cloned_data = cloned_frame.into_data().unwrap();
            assert_eq!(
                original_data, cloned_data,
                "Clone should start from the beginning"
            );
        }

        // Poll the original again to advance it further
        let original_second = original_pinned.as_mut().poll_frame(&mut ctx);

        // Poll the clone again - it should return the second chunk
        let cloned_second = cloned_pinned.as_mut().poll_frame(&mut ctx);

        // Both should get their respective second chunks
        if let (Poll::Ready(Some(Ok(original_frame2))), Poll::Ready(Some(Ok(cloned_frame2)))) =
            (original_second, cloned_second)
        {
            let original_data2 = original_frame2.into_data().unwrap();
            let cloned_data2 = cloned_frame2.into_data().unwrap();
            assert_eq!(
                original_data2, cloned_data2,
                "Both should have independent chunk sequences"
            );
        }

        // Both should return None now

        let original_third = original_pinned.as_mut().poll_frame(&mut ctx);
        let cloned_third = cloned_pinned.as_mut().poll_frame(&mut ctx);

        assert!(matches!(original_third, Poll::Ready(None)));
        assert!(matches!(cloned_third, Poll::Ready(None)));
    }
}

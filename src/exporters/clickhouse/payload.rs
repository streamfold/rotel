use crate::exporters::clickhouse::ch_error::Error;
use crate::exporters::clickhouse::rowbinary::serialize_into;
use crate::exporters::clickhouse::{BUFFER_SIZE, Compression, MIN_CHUNK_SIZE, compression};
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

    pub(crate) fn finish(mut self) -> Result<ClickhousePayload, BoxError> {
        if !self.curr_chunk.is_empty() {
            let new_chunk = self.take_and_close_current()?;
            self.closed.push(new_chunk);
        }

        Ok(ClickhousePayload::new(self.closed))
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

#[derive(Clone)]
pub struct ClickhousePayload {
    inner: Arc<Mutex<Inner>>,
}

pub struct Inner {
    chunks: VecDeque<Bytes>,
}

impl ClickhousePayload {
    fn new(chunks: Vec<Bytes>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                chunks: chunks.into(),
            })),
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
        match self.inner.lock().unwrap().chunks.pop_front() {
            None => Poll::Ready(None),
            Some(chunk) => Poll::Ready(Some(Ok(Frame::data(chunk.clone())))),
        }
    }
}

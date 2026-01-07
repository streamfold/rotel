// SPDX-License-Identifier: Apache-2.0

use crate::topology::payload::Message;
use futures_util::{
    Stream, StreamExt, ready,
    stream::{Fuse, FuturesOrdered},
};
use http::Request;
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::task::JoinHandle;
use tower::BoxError;
use tracing::{error, info};

// todo: Make this configurable?
const MAX_CONCURRENT_ENCODERS: usize = 8;

pub trait BuildRequest<Resource, Payload> {
    type Output: IntoIterator<Item = Request<Payload>>;

    fn build(&self, input: Vec<Message<Resource>>) -> Result<Self::Output, BoxError>;
}

#[pin_project]
pub struct RequestBuilderMapper<InStr, Resource, Payload, ReqBuilder>
where
    InStr: Stream<Item = Vec<Message<Resource>>>,
    ReqBuilder: BuildRequest<Resource, Payload>,
    <ReqBuilder as BuildRequest<Resource, Payload>>::Output: IntoIterator<Item = Request<Payload>>,
{
    #[pin]
    input: Fuse<InStr>,

    req_builder: ReqBuilder,
    encoding_futures: FuturesOrdered<JoinHandle<Result<ReqBuilder::Output, BoxError>>>,
    max_concurrent_encoders: usize,
}

impl<InStr, Resource, Payload, ReqBuilder>
    RequestBuilderMapper<InStr, Resource, Payload, ReqBuilder>
where
    InStr: Stream<Item = Vec<Message<Resource>>>,
    ReqBuilder: BuildRequest<Resource, Payload>,
    <ReqBuilder as BuildRequest<Resource, Payload>>::Output: IntoIterator<Item = Request<Payload>>,
{
    pub fn new(input: InStr, req_builder: ReqBuilder) -> Self {
        let max_concurrent_encoders = std::env::var("ROTEL_MAX_CONCURRENT_ENCODERS")
            .ok()
            .and_then(|s| if s.trim().is_empty() { None } else { Some(s) })
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(MAX_CONCURRENT_ENCODERS);

        Self {
            input: input.fuse(),
            req_builder,
            max_concurrent_encoders,
            encoding_futures: FuturesOrdered::new(),
        }
    }
}

impl<InStr, Resource, Payload, ReqBuilder> Stream
    for RequestBuilderMapper<InStr, Resource, Payload, ReqBuilder>
where
    InStr: Stream<Item = Vec<Message<Resource>>>,
    Resource: Send + Clone + 'static,
    ReqBuilder: BuildRequest<Resource, Payload> + Send + Sync + Clone + 'static,
    <ReqBuilder as BuildRequest<Resource, Payload>>::Output:
        IntoIterator<Item = Request<Payload>> + Send + Sync + Clone,
    Payload: Send + 'static,
{
    type Item = Result<<ReqBuilder as BuildRequest<Resource, Payload>>::Output, BoxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // start as many encoding futures as we can
        loop {
            // We are at the limit of encoding futures
            if this.encoding_futures.len() >= *this.max_concurrent_encoders {
                break;
            }

            match this.input.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    info!(metadata = ?item[0].metadata.as_ref().unwrap(), "request builder mapper received request");
                    let req_builder = this.req_builder.clone();
                    let jh = tokio::task::spawn_blocking(move || req_builder.build(item));
                    this.encoding_futures.push_back(jh);
                }
                _ => break, // skip to waiting on encoding futures
            }
        }

        // If we hit the end, but haven't encoded anything new, bail
        if this.input.is_done() && this.encoding_futures.is_empty() {
            return Poll::Ready(None);
        }

        match ready!(this.encoding_futures.poll_next_unpin(cx)) {
            None => Poll::Pending,
            Some(item) => match item {
                Ok(item) => Poll::Ready(Some(item)),
                Err(e) => {
                    // XXX: should we panic here?
                    error!(error = ?e, "Encoding future has failed, dropping data.");
                    Poll::Pending
                }
            },
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.encoding_futures.len(), None)
    }
}

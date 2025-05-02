// SPDX-License-Identifier: Apache-2.0

use futures_util::{
    ready, stream::{Fuse, FuturesOrdered}, Stream,
    StreamExt,
};
use http::Request;
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::task::JoinHandle;
use tower::BoxError;
use tracing::error;

// todo: Make this configurable?
const MAX_CONCURRENT_ENCODERS: usize = 8;

pub trait BuildRequest<Resource, Payload> {
    fn build(&self, input: Vec<Resource>) -> Result<Request<Payload>, BoxError>;
}

#[pin_project]
pub struct RequestBuilderMapper<InStr, Resource, Payload, ReqBuilder>
where
    InStr: Stream<Item = Vec<Resource>>,
{
    #[pin]
    input: Fuse<InStr>,

    req_builder: ReqBuilder,
    encoding_futures: FuturesOrdered<JoinHandle<Result<Request<Payload>, BoxError>>>,
}

impl<InStr, Resource, Payload, ReqBuilder> RequestBuilderMapper<InStr, Resource, Payload, ReqBuilder>
where
    InStr: Stream<Item = Vec<Resource>>,
{
    pub fn new(input: InStr, req_builder: ReqBuilder) -> Self {
        Self {
            input: input.fuse(),
            req_builder,
            encoding_futures: FuturesOrdered::new(),
        }
    }
}

impl<InStr, Resource, Payload, ReqBuilder> Stream for RequestBuilderMapper<InStr, Resource, Payload, ReqBuilder>
where
    InStr: Stream<Item = Vec<Resource>>,
    Resource: Send + Clone + 'static,
    ReqBuilder: BuildRequest<Resource, Payload> + Send + Sync + Clone + 'static,
    Payload: Send + 'static,
{
    type Item = Result<Request<Payload>, BoxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // start as many encoding futures as we can
        loop {
            // We are at the limit of encoding futures
            if this.encoding_futures.len() >= MAX_CONCURRENT_ENCODERS {
                break;
            }

            match this.input.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
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
}

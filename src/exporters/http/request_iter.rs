use futures_util::{Stream, StreamExt, stream::Fuse};
use http::Request;
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::BoxError;
use tracing::error;

#[pin_project]
pub struct RequestIterator<InStr, InputIter, Payload>
where
    InputIter: IntoIterator<Item = Request<Payload>>,
    InStr: Stream<Item = Result<InputIter, BoxError>>,
{
    #[pin]
    input: Fuse<InStr>,

    inner: Inner<InputIter, Payload>,
}

struct Inner<InputIter, Payload>
where
    InputIter: IntoIterator<Item = Request<Payload>>,
{
    curr_iter: Option<<InputIter as IntoIterator>::IntoIter>,
}

impl<InStr, InputIter, Payload> RequestIterator<InStr, InputIter, Payload>
where
    InputIter: IntoIterator<Item = Request<Payload>>,
    InStr: Stream<Item = Result<InputIter, BoxError>>,
{
    pub fn new(input: InStr) -> Self {
        Self {
            input: input.fuse(),
            inner: Inner { curr_iter: None },
        }
    }
}

impl<InputIter, Payload> Inner<InputIter, Payload>
where
    InputIter: IntoIterator<Item = Request<Payload>>,
{
    fn next(&mut self) -> Option<<InputIter as IntoIterator>::Item> {
        match &mut self.curr_iter {
            None => None,
            Some(item) => {
                let next = item.next();
                if next.is_none() {
                    // Once we hit the first none, clear the iterator. It might be undefined
                    // to continue calling next.
                    self.curr_iter = None;
                }

                next
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self.curr_iter.as_ref() {
            None => (0, None),
            Some(item) => item.size_hint(),
        }
    }

    fn update(&mut self, item: <InputIter as IntoIterator>::IntoIter) {
        self.curr_iter = Some(item)
    }
}

impl<InStr, InputIter, Payload> Stream for RequestIterator<InStr, InputIter, Payload>
where
    InputIter: IntoIterator<Item = Request<Payload>>,
    InStr: Stream<Item = Result<InputIter, BoxError>>,
{
    type Item = Result<Request<Payload>, BoxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            if let Some(next) = this.inner.next() {
                return Poll::Ready(Some(Ok(next)));
            }

            // Either we hit the end of the iterator or we haven't pulled a new item from
            // the stream
            match this.input.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(ret) => match ret {
                    None => return Poll::Ready(None),
                    Some(res) => match res {
                        Ok(item) => this.inner.update(item.into_iter()),
                        Err(err) => {
                            error!(error = err, "Failed to encode request, dropping")
                        }
                    },
                },
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self.input.size_hint() {
            // If the input is a zero, defer to the size of the remaining iterator
            (0, _) => self.inner.size_hint(),
            (min, max) => (min, max),
        }
    }
}

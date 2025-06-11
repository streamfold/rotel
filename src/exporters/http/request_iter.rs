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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_channel::bounded;
    use bytes::Bytes;
    use http::Method;
    use http_body_util::Full;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn iterator_size_hint() {
        let (tx, rx) = bounded::<Result<Vec<Request<Full<Bytes>>>, BoxError>>(5);
        let mut stream = RequestIterator::new(rx.into_stream());

        let (min, max) = stream.size_hint();
        assert_eq!(0, min);
        assert_eq!(None, max);

        let hosts = vec!["127.0.0.1", "127.0.0.2", "127.0.0.3"];
        let reqs: Vec<Request<Full<Bytes>>> = hosts
            .iter()
            .map(|h| new_request(format!("http://{}", h).as_str()))
            .collect();

        tx.send(Ok(reqs)).await.unwrap();

        // Still zero because we haven't called next/poll_next on the iterator
        let (min, max) = stream.size_hint();
        assert_eq!(0, min);
        assert_eq!(None, max);

        for (idx, e) in hosts.iter().enumerate() {
            println!("iter: {}", idx);
            let r = stream.next().await;
            assert!(r.is_some());

            let req = r.unwrap().unwrap();
            assert_eq!(*e, req.uri().host().unwrap());

            let (min, max) = stream.size_hint();
            assert_eq!(hosts.len() - idx - 1, min);
            assert_eq!(Some(hosts.len() - idx - 1), max);
        }

        let (min, max) = stream.size_hint();
        assert_eq!(0, min);
        assert_eq!(Some(0), max);
    }

    fn new_request(host: &str) -> Request<Full<Bytes>> {
        Request::builder()
            .method(Method::GET)
            .uri(host.to_string())
            .body(Full::from(Bytes::new()))
            .unwrap()
    }
}

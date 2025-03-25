// SPDX-License-Identifier: Apache-2.0

use std::error::Error;
use std::net::SocketAddr;
use std::net::TcpListener as StdTcpListener;
use tokio::net::TcpListener as AsyncTcpListener;
use tokio_stream::wrappers::TcpListenerStream;

/// Represents a TCP socket that is both bound and configured for listening. This supports
/// both async and non-async (std) methods of binding the socket, allowing for these to
/// be constructed outside a tokio runtime if need be (e.g. during early init). Both versions
/// can be converted to an async TcpListenerStream.
///
/// Because it is a bit confusing when comparing these terms to the Berkeley socket model, creating
/// a Listener calls both:
///  1) bind()
///  2) listen()
/// Therefore, it is ready to accept() connections after success here.
//
pub struct Listener {
    inner: ListenerInner,
}

enum ListenerInner {
    Async(AsyncTcpListener),
    Std(StdTcpListener),
}

impl Listener {
    pub fn listen_std(endpoint: SocketAddr) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match StdTcpListener::bind(endpoint) {
            Ok(l) => Ok(Self {
                inner: ListenerInner::Std(l),
            }),
            Err(e) => Err(format!("unable to bind to {}: {}", endpoint, e).into()),
        }
    }

    pub async fn listen_async(endpoint: SocketAddr) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match AsyncTcpListener::bind(endpoint).await {
            Ok(l) => Ok(Self {
                inner: ListenerInner::Async(l),
            }),
            Err(e) => Err(format!("unable to find to {}: {}", endpoint, e).into()),
        }
    }

    pub fn bound_address(&self) -> Result<SocketAddr, Box<dyn Error + Send + Sync>> {
        Ok(match &self.inner {
            ListenerInner::Async(inner) => inner.local_addr()?,
            ListenerInner::Std(inner) => inner.local_addr()?,
        })
    }

    pub fn into_async(self) -> Result<AsyncTcpListener, Box<dyn Error + Send + Sync>> {
        match self.inner {
            ListenerInner::Async(inner) => Ok(inner),
            ListenerInner::Std(inner) => {
                // We convert this to async by setting non-blocking
                inner.set_nonblocking(true)?;

                let listener = AsyncTcpListener::from_std(inner)?;
                Ok(listener)
            }
        }
    }

    pub fn into_stream(self) -> Result<TcpListenerStream, Box<dyn Error + Send + Sync>> {
        let l = self.into_async()?;
        Ok(TcpListenerStream::new(l))
    }
}

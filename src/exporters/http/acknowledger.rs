// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::response::Response;
use crate::topology::payload::Ack;
use std::future::Future;
use std::pin::Pin;
use tracing::warn;

/// Trait for handling acknowledgment of messages after a successful export
pub trait Acknowledger<T>: Send + Sync + Clone {
    /// Process acknowledgments for a successful response
    fn acknowledge<'a>(
        &'a self,
        response: &'a Response<T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// Default acknowledger that sends acknowledgments for all metadata
#[derive(Default, Clone)]
pub struct DefaultAcknowledger;

impl<T> Acknowledger<T> for DefaultAcknowledger
where
    T: Send + Sync,
{
    fn acknowledge<'a>(
        &'a self,
        response: &'a Response<T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if let Some(metadata_vec) = response.metadata() {
                let mut metadata_vec = metadata_vec.clone();
                for metadata in metadata_vec.iter_mut() {
                    if let Err(e) = metadata.ack().await {
                        warn!("Failed to acknowledge message: {:?}", e);
                        // Continue acknowledging other messages even if one fails
                    }
                }
            }
        })
    }
}

/// No-op acknowledger that does not acknowledge any messages
#[derive(Default, Clone)]
pub struct NoOpAcknowledger;

impl<T> Acknowledger<T> for NoOpAcknowledger
where
    T: Send + Sync,
{
    fn acknowledge<'a>(
        &'a self,
        _response: &'a Response<T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // Do nothing
        })
    }
}

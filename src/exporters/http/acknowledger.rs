// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::client::TransportErrorWithMetadata;
use crate::exporters::http::response::Response;
use crate::topology::payload::{Ack, ExporterError};
use std::future::Future;
use std::pin::Pin;
use tracing::warn;

/// Trait for handling acknowledgment of messages after a successful export
pub trait Acknowledger<T>: Send + Sync + Clone {
    type Error: std::fmt::Debug;

    /// Process acknowledgments for a successful response
    fn acknowledge<'a>(
        &'a self,
        response: &'a Response<T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    /// Handle errors, potentially sending nacks when finite retry is enabled
    fn handle_error<'a>(
        &'a self,
        error: &'a Self::Error,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// Default HTTP acknowledger that sends acknowledgments for all metadata
#[derive(Default, Clone)]
pub struct DefaultHTTPAcknowledger;

impl<T> Acknowledger<T> for DefaultHTTPAcknowledger
where
    T: Send + Sync,
{
    type Error = tower::BoxError;
    fn acknowledge<'a>(
        &'a self,
        response: &'a Response<T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // Check if this is an HTTP error response
            let is_error_response = match response {
                Response::Http(parts, _, _) => {
                    parts.status.as_u16() < 200 || parts.status.as_u16() >= 300
                }
                Response::Grpc(status, _, _) => status.code() != tonic::Code::Ok,
            };

            if let Some(metadata_vec) = response.metadata() {
                for metadata in metadata_vec {
                    if is_error_response {
                        // Send nacks for error responses - offset tracker will enforce retry policy
                        let error_reason = ExporterError::ExportFailed {
                            error_code: response.status_code().as_u16(),
                            error_message: format!(
                                "HTTP export failed with status: {}",
                                response.status_code()
                            ),
                        };

                        if let Err(nack_err) = metadata.nack(error_reason).await {
                            warn!("Failed to nack message after HTTP error: {:?}", nack_err);
                        }
                    } else {
                        // Normal case: acknowledge successful responses
                        if let Err(e) = metadata.ack().await {
                            warn!("Failed to acknowledge message: {:?}", e);
                            // Continue acknowledging other messages even if one fails
                        }
                    }
                }
            }
        })
    }

    fn handle_error<'a>(
        &'a self,
        error: &'a Self::Error,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // Check if this is a transport error with metadata
            if let Some(transport_error) = error.downcast_ref::<TransportErrorWithMetadata>() {
                if let Some(metadata_vec) = &transport_error.metadata {
                    for metadata in metadata_vec {
                        let error_reason = ExporterError::RetryExhausted {
                            last_error: format!("{}", transport_error.original_error),
                            last_error_code: None,
                            elapsed_time_ms: 0, // TODO: capture actual elapsed time
                            attempt_count: 0,   // TODO: capture actual attempt count
                        };

                        if let Err(nack_err) = metadata.nack(error_reason).await {
                            warn!(
                                "Failed to nack message after transport error: {:?}",
                                nack_err
                            );
                        }
                    }
                }
            }
        })
    }
}

/// No-op acknowledger that does not acknowledge any messages
#[derive(Default, Clone)]
#[allow(dead_code)]
pub struct NoOpAcknowledger;

impl<T> Acknowledger<T> for NoOpAcknowledger
where
    T: Send + Sync,
{
    type Error = tower::BoxError;
    fn acknowledge<'a>(
        &'a self,
        _response: &'a Response<T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // Do nothing
        })
    }

    fn handle_error<'a>(
        &'a self,
        _error: &'a Self::Error,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // Do nothing
        })
    }
}

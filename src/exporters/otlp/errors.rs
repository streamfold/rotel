// SPDX-License-Identifier: Apache-2.0

use http::StatusCode;
use std::error::Error;
use std::fmt;
use tonic::Status;

/// ExporterError is the result of exporting a trace msg
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum ExporterError {
    /// Processing error
    Generic(String),

    Connect,

    /// HTTP error resulting in invalid status code
    Http(StatusCode, Option<String>),

    /// GRPC Error status, from tonic
    Grpc(Status),
}

impl fmt::Display for ExporterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExporterError::Generic(msg) => write!(f, "Generic error: {}", msg),
            ExporterError::Connect => write!(f, "Failed to connect"),
            ExporterError::Http(status, resp) => match resp {
                None => write!(f, "HTTP error: {}", status),
                Some(text) => write!(f, "HTTP error: {}:{}", status, text),
            },
            ExporterError::Grpc(status) => write!(f, "GRPC error: {}", status),
        }
    }
}

impl Error for ExporterError {}

/// Determine if an error is retryable
pub fn is_retryable_error(status: &ExporterError) -> bool {
    match status {
        ExporterError::Generic(_) => false, // todo: further classify errors here
        ExporterError::Connect => true,
        ExporterError::Http(status, _) => {
            match status.as_u16() {
                200..=202 => false,
                408 | 429 => true,
                500..=504 => true,
                _ => false,
            }
        }
        ExporterError::Grpc(status) => matches!(
            status.code(),
            tonic::Code::Unavailable |     // Service temporarily unavailable
                tonic::Code::Internal |        // Internal server error
                tonic::Code::DeadlineExceeded| // Request timeout
                tonic::Code::ResourceExhausted // Server overloaded
        ),
    }
}

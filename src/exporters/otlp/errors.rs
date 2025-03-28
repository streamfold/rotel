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
    Http(StatusCode),

    /// GRPC Error status, from tonic
    Grpc(Status),
}

impl fmt::Display for ExporterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExporterError::Generic(msg) => write!(f, "Generic error: {}", msg),
            ExporterError::Connect => write!(f, "Failed to connect"),
            ExporterError::Http(status) => write!(f, "HTTP error: {}", status),
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
        ExporterError::Http(status) => {
            let code = status.as_u16();
            (500..505).contains(&code)
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

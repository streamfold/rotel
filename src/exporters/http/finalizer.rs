use std::fmt::Display;

use http::StatusCode;
use tonic::Status;
use tower::BoxError;

use super::response::Response;

pub trait ResultFinalizer<Res> {
    fn finalize(&self, result: Res) -> Result<(), BoxError>;
}

// Basic finalizer that respects HTTP or GRPC success status codes
#[derive(Default, Clone)]
pub struct SuccessStatusFinalizer;

pub enum FinalizerError<RespBody> {
    HttpStatus(StatusCode, Option<RespBody>),
    GrpcStatus(Status, Option<RespBody>),
}

impl<RespBody: Display> From<FinalizerError<RespBody>> for BoxError {
    fn from(err: FinalizerError<RespBody>) -> Self {
        match err {
            FinalizerError::HttpStatus(status, body) => match body {
                Some(body) => format!("Invalid HTTP status: {} ({})", status, body).into(),
                None => format!("Invalid HTTP status: {})", status).into(),
            },
            FinalizerError::GrpcStatus(status, body) => match body {
                Some(body) => format!("Invalid gRPC status: {} ({})", status, body).into(),
                None => format!("Invalid gRPC status: {}", status).into(),
            },
        }
    }
}

impl<T, Err> ResultFinalizer<Result<Response<T>, Err>> for SuccessStatusFinalizer
where
    Err: Into<BoxError>,
    T: Display,
{
    fn finalize(&self, result: Result<Response<T>, Err>) -> Result<(), BoxError> {
        match result {
            Ok(r) => match r {
                Response::Http(parts, body) => match parts.status.as_u16() {
                    200..=202 => Ok(()),
                    _ => Err(FinalizerError::HttpStatus(parts.status, body).into()),
                },
                Response::Grpc(status, body) => {
                    if status.code() == tonic::Code::Ok {
                        Ok(())
                    } else {
                        Err(FinalizerError::GrpcStatus(status, body).into())
                    }
                }
            },
            Err(e) => Err(e.into()),
        }
    }
}

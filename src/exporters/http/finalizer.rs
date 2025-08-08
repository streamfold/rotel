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

pub enum FinalizerError {
    HttpStatus(StatusCode),

    GrpcStatus(Status),
}

impl From<FinalizerError> for BoxError {
    fn from(err: FinalizerError) -> Self {
        match err {
            FinalizerError::HttpStatus(status) => format!("Invalid HTTP status: {}", status).into(),
            FinalizerError::GrpcStatus(status) => format!("Invalid gRPC status: {}", status).into(),
        }
    }
}

impl<T, Err> ResultFinalizer<Result<Response<T>, Err>> for SuccessStatusFinalizer
where
    Err: Into<BoxError>,
{
    fn finalize(&self, result: Result<Response<T>, Err>) -> Result<(), BoxError> {
        match result {
            Ok(r) => match r {
                Response::Http(parts, _) => match parts.status.as_u16() {
                    200..=202 => Ok(()),
                    _ => Err(FinalizerError::HttpStatus(parts.status).into()),
                },
                Response::Grpc(status, _) => {
                    if status.code() == tonic::Code::Ok {
                        Ok(())
                    } else {
                        Err(FinalizerError::GrpcStatus(status).into())
                    }
                }
            },
            Err(e) => Err(e.into()),
        }
    }
}

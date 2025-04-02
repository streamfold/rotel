// SPDX-License-Identifier: Apache-2.0

use http::StatusCode;
use http::response::Parts;
use tonic::Status;

#[derive(Debug)]
pub enum Response<T> {
    Http(Parts, Option<T>),
    Grpc(Status, Option<T>),
}

impl<T> Response<T> {
    pub fn from_http(head: Parts, body: Option<T>) -> Self {
        Self::Http(head, body)
    }

    pub fn from_grpc(status: Status, body: Option<T>) -> Self {
        Self::Grpc(status, body)
    }

    pub fn with_body(self, body: T) -> Self {
        match self {
            Response::Http(head, _) => Response::Http(head, Some(body)),
            Response::Grpc(status, _) => Response::Grpc(status, Some(body)),
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Response::Http(head, _) => head.status,
            Response::Grpc(_, _) => StatusCode::OK, // gRPC always OK if parsed
        }
    }
}

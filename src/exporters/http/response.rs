// SPDX-License-Identifier: Apache-2.0

use std::fmt::{self, Debug, Formatter};

use http::StatusCode;
use http::response::Parts;
use tonic::Status;

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

impl<T> Debug for Response<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Response::Http(head, body) => match body {
                Some(body) => write!(f, "HTTPResponse{{Status={}, Body={:?}}}", head.status, body),
                None => write!(f, "HTTPResponse{{Status={}}}", head.status),
            },
            Response::Grpc(status, body) => match body {
                Some(body) => write!(
                    f,
                    "gRPCResponse{{Status={}, Body={:?}}}",
                    status.code(),
                    body
                ),
                None => write!(f, "gRPCResponse{{Status={}}}", status.code()),
            },
        }
    }
}

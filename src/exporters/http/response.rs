// SPDX-License-Identifier: Apache-2.0

use crate::topology::payload::MessageMetadata;
use std::fmt::{self, Debug, Formatter};

use http::StatusCode;
use http::response::Parts;
use tonic::Status;

pub enum Response<T> {
    Http(Parts, Option<T>, Option<Vec<MessageMetadata>>),
    Grpc(Status, Option<T>, Option<Vec<MessageMetadata>>),
}

impl<T> Response<T> {
    pub fn from_http(head: Parts, body: Option<T>) -> Self {
        Self::Http(head, body, None)
    }

    pub fn from_http_with_metadata(
        head: Parts,
        body: Option<T>,
        metadata: Option<Vec<MessageMetadata>>,
    ) -> Self {
        Self::Http(head, body, metadata)
    }

    pub fn from_grpc(status: Status, body: Option<T>) -> Self {
        Self::Grpc(status, body, None)
    }

    pub fn from_grpc_with_metadata(
        status: Status,
        body: Option<T>,
        metadata: Option<Vec<MessageMetadata>>,
    ) -> Self {
        Self::Grpc(status, body, metadata)
    }

    pub fn with_body(self, body: T) -> Self {
        match self {
            Response::Http(head, _, metadata) => Response::Http(head, Some(body), metadata),
            Response::Grpc(status, _, metadata) => Response::Grpc(status, Some(body), metadata),
        }
    }

    pub fn with_metadata(self, metadata: Option<Vec<MessageMetadata>>) -> Self {
        match self {
            Response::Http(head, body, _) => Response::Http(head, body, metadata),
            Response::Grpc(status, body, _) => Response::Grpc(status, body, metadata),
        }
    }

    pub fn metadata(&self) -> &Option<Vec<MessageMetadata>> {
        match self {
            Response::Http(_, _, metadata) => metadata,
            Response::Grpc(_, _, metadata) => metadata,
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Response::Http(head, _, _) => head.status,
            Response::Grpc(_, _, _) => StatusCode::OK, // gRPC always OK if parsed
        }
    }
}

impl<T> Debug for Response<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Response::Http(head, body, metadata) => {
                let has_metadata = metadata.as_ref().map(|m| m.len()).unwrap_or(0) > 0;
                match body {
                    Some(body) => write!(
                        f,
                        "HTTPResponse{{Status={}, Body={:?}, Metadata={}}}",
                        head.status, body, has_metadata
                    ),
                    None => write!(
                        f,
                        "HTTPResponse{{Status={}, Metadata={}}}",
                        head.status, has_metadata
                    ),
                }
            }
            Response::Grpc(status, body, metadata) => {
                let has_metadata = metadata.as_ref().map(|m| m.len()).unwrap_or(0) > 0;
                match body {
                    Some(body) => write!(
                        f,
                        "gRPCResponse{{Status={}, Body={:?}, Metadata={}}}",
                        status.code(),
                        body,
                        has_metadata
                    ),
                    None => write!(
                        f,
                        "gRPCResponse{{Status={}, Metadata={}}}",
                        status.code(),
                        has_metadata
                    ),
                }
            }
        }
    }
}

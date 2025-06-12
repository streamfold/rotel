// SPDX-License-Identifier: Apache-2.0

use crate::exporters::clickhouse::payload::ClickhousePayload;
use crate::exporters::clickhouse::request_mapper::{RequestMapper, RequestType};
use crate::exporters::http::request_builder_mapper::BuildRequest;
use http::Request;
use std::marker::PhantomData;
use std::sync::Arc;
use tower::BoxError;

pub trait TransformPayload<T> {
    fn transform(&self, input: Vec<T>) -> Result<Vec<(RequestType, ClickhousePayload)>, BoxError>;
}

// todo: identify the cost of recursively cloning these
#[derive(Clone)]
pub struct RequestBuilder<Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    transformer: Transform,
    request_mapper: Arc<RequestMapper>,
    _phantom: PhantomData<Resource>,
}

impl<Resource, Transform> RequestBuilder<Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    pub fn new(
        transformer: Transform,
        request_mapper: Arc<RequestMapper>,
    ) -> Result<Self, BoxError> {
        Ok(Self {
            transformer,
            request_mapper,
            _phantom: PhantomData,
        })
    }
}

impl<Resource, Transform> BuildRequest<Resource, ClickhousePayload>
    for RequestBuilder<Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    type Output = Vec<Request<ClickhousePayload>>;

    fn build(&self, input: Vec<Resource>) -> Result<Self::Output, BoxError> {
        let payloads = self.transformer.transform(input)?;

        let requests: Result<Self::Output, BoxError> = payloads
            .into_iter()
            .map(|(req_type, payload)| self.request_mapper.build(req_type, payload))
            .collect();

        requests
    }
}

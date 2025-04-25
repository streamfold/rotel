// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::types::Request;
use std::marker::PhantomData;
use tower::BoxError;
use crate::exporters::clickhouse::api_request::ApiRequestBuilder;
use crate::exporters::clickhouse::payload::ClickhousePayload;
use crate::exporters::clickhouse::request_builder_mapper::BuildRequest;
use crate::exporters::clickhouse::transformer::Transformer;

pub trait TransformPayload<T> {
    fn transform(&self, input: Vec<T>) -> ClickhousePayload;
}

// todo: identify the cost of recursively cloning these
#[derive(Clone)]
pub struct RequestBuilder<Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    transformer: Transform,
    api_req_builder: ApiRequestBuilder,
    _phantom: PhantomData<Resource>,
}

impl<Resource, Transform> RequestBuilder<Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    pub fn new(
        transformer: Transform,
        api_req_builder: ApiRequestBuilder,
    ) -> Result<Self, BoxError> {
        
        Ok(Self {
            transformer,
            api_req_builder,
            _phantom: PhantomData,
        })
    }
}

impl<Resource, Transform> BuildRequest<Resource> for RequestBuilder<Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    fn build(&self, input: Vec<Resource>) -> Result<Request, BoxError> {
        let payload = self.transformer.transform(input);

        self.api_req_builder.build(payload)
    }
}

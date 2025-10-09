// SPDX-License-Identifier: Apache-2.0

use crate::exporters::datadog::DatadogPayload;
use crate::exporters::datadog::Region;
use crate::exporters::datadog::api_request::ApiRequestBuilder;
use crate::exporters::datadog::types::pb::AgentPayload;
use crate::exporters::http::request_builder_mapper::BuildRequest;
use crate::topology::payload::{Message, MessageMetadata};
use http::Request;
use std::marker::PhantomData;
use tower::BoxError;

pub trait TransformPayload<T> {
    fn transform(&self, input: Vec<Message<T>>) -> (AgentPayload, Option<Vec<MessageMetadata>>);
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
        region: Region,
        custom_endpoint: Option<String>,
        api_key: String,
    ) -> Result<Self, BoxError> {
        let endpoint = if let Some(custom) = custom_endpoint {
            custom
        } else {
            region.trace_endpoint()
        };
        let api_req_builder = ApiRequestBuilder::new(endpoint, api_key)?;

        Ok(Self {
            transformer,
            api_req_builder,
            _phantom: PhantomData,
        })
    }
}

impl<Resource, Transform> BuildRequest<Resource, DatadogPayload>
    for RequestBuilder<Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    type Output = Vec<Request<DatadogPayload>>;

    fn build(&self, input: Vec<Message<Resource>>) -> Result<Self::Output, BoxError> {
        let (payload, metadata) = self.transformer.transform(input);

        self.api_req_builder.build(payload, metadata)
    }
}

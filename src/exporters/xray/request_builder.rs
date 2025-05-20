// SPDX-License-Identifier: Apache-2.0

use crate::aws_api::config::AwsConfig;
use crate::exporters::http::request_builder_mapper::BuildRequest;
use crate::exporters::xray::transformer::ExportError;
use crate::exporters::xray::xray_request::XRayRequestBuilder;
use crate::exporters::xray::Region;
use bytes::Bytes;
use http::Request;
use http_body_util::Full;
use serde_json::Value;
use std::marker::PhantomData;
use tower::BoxError;

pub trait TransformPayload<T> {
    fn transform(&self, input: Vec<T>) -> Result<Vec<Value>, ExportError>;
}

// todo: identify the cost of recursively cloning these
#[derive(Clone)]
pub struct RequestBuilder<'a, Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    transformer: Transform,
    api_req_builder: XRayRequestBuilder<'a>,
    _phantom: PhantomData<Resource>,
}

impl<'a, Resource, Transform> RequestBuilder<'a, Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    pub fn new(
        transformer: Transform,
        config: &'a AwsConfig,
        region: Region,
        custom_endpoint: Option<String>,
    ) -> Result<Self, BoxError> {
        let endpoint = if let Some(custom) = custom_endpoint {
            custom
        } else {
            format!("https://xray.{}.amazonaws.com", region).to_string()
        };
        let api_req_builder = XRayRequestBuilder::new(endpoint, region, config)?;
        Ok(Self {
            transformer,
            api_req_builder,
            _phantom: PhantomData,
        })
    }
}

impl<'a, Resource, Transform> BuildRequest<Resource, Full<Bytes>>
    for RequestBuilder<'a, Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    fn build(&self, input: Vec<Resource>) -> Result<Request<Full<Bytes>>, BoxError> {
        let payload = self.transformer.transform(input);
        match payload {
            Ok(p) => self.api_req_builder.build(p),
            Err(e) => Err(format!("Export error: {:?}", e).into()),
        }
    }
}

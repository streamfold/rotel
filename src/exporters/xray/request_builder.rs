// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::request_builder_mapper::BuildRequest;
use crate::exporters::xray::Region;
use crate::exporters::xray::XRayPayload;
use crate::exporters::xray::transformer::{ExportError, XRayValuePayload};
use crate::exporters::xray::xray_request::XRayRequestBuilder;
use crate::topology::payload::Message;
use http::Request;
use std::marker::PhantomData;
use tower::BoxError;

pub trait TransformPayload<T> {
    fn transform(&self, input: Vec<Message<T>>) -> Result<Vec<XRayValuePayload>, ExportError>;
}

// todo: identify the cost of recursively cloning these
#[derive(Clone)]
pub struct RequestBuilder<Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    transformer: Transform,
    api_req_builder: XRayRequestBuilder,
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
    ) -> Result<Self, BoxError> {
        let endpoint = if let Some(custom) = custom_endpoint {
            custom
        } else {
            format!("https://xray.{}.amazonaws.com", region).to_string()
        };
        let api_req_builder = XRayRequestBuilder::new(endpoint)?;
        Ok(Self {
            transformer,
            api_req_builder,
            _phantom: PhantomData,
        })
    }
}

impl<Resource, Transform> BuildRequest<Resource, XRayPayload>
    for RequestBuilder<Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    type Output = Vec<Request<XRayPayload>>;

    fn build(&self, input: Vec<Message<Resource>>) -> Result<Self::Output, BoxError> {
        match self.transformer.transform(input) {
            Ok(payloads) => self.api_req_builder.build(payloads),
            Err(e) => Err(format!("Export error: {:?}", e).into()),
        }
    }
}

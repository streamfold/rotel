// SPDX-License-Identifier: Apache-2.0

use super::event::Event;
use crate::aws_api::config::AwsConfig;
use crate::exporters::awsemf::AwsEmfExporterConfig;
use crate::exporters::awsemf::AwsEmfPayload;
use crate::exporters::awsemf::emf_request::AwsEmfRequestBuilder;
use crate::exporters::awsemf::transformer::ExportError;
use crate::exporters::http::request_builder_mapper::BuildRequest;
use crate::topology::payload::{Message, MessageMetadata};
use http::Request;
use std::marker::PhantomData;
use tower::BoxError;

pub trait TransformPayload<T> {
    fn transform(
        &self,
        input: Vec<Message<T>>,
    ) -> (
        Result<Vec<Event>, ExportError>,
        Option<Vec<MessageMetadata>>,
    );
}

#[derive(Clone)]
pub struct RequestBuilder<Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    transformer: Transform,
    api_req_builder: AwsEmfRequestBuilder,
    _phantom: PhantomData<Resource>,
}

impl<Resource, Transform> RequestBuilder<Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    pub fn new(
        transformer: Transform,
        aws_config: AwsConfig,
        config: AwsEmfExporterConfig,
    ) -> Result<Self, BoxError> {
        let endpoint = if let Some(custom) = &config.custom_endpoint {
            custom.clone()
        } else {
            format!("https://logs.{}.amazonaws.com", config.region)
        };

        let api_req_builder = AwsEmfRequestBuilder::new(
            endpoint,
            aws_config,
            config.log_group_name.clone(),
            config.log_stream_name.clone(),
        )?;

        Ok(Self {
            transformer,
            api_req_builder,
            _phantom: PhantomData,
        })
    }
}

impl<Resource, Transform> BuildRequest<Resource, AwsEmfPayload>
    for RequestBuilder<Resource, Transform>
where
    Transform: TransformPayload<Resource>,
{
    type Output = Vec<Request<AwsEmfPayload>>;

    fn build(&self, input: Vec<Message<Resource>>) -> Result<Self::Output, BoxError> {
        let (payload_result, metadata) = self.transformer.transform(input);
        match payload_result {
            Ok(p) => self.api_req_builder.build(p, metadata),
            Err(e) => Err(format!("Export error: {:?}", e).into()),
        }
    }
}

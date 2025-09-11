use std::marker::PhantomData;

use bytes::Bytes;
use http::Request;
use http_body_util::Full;

use crate::{
    exporters::{http::request_builder_mapper::BuildRequest, otlp::{request::RequestBuilder, signer::RequestSigner}},
    topology::{batch::BatchSizer, payload::OTLPFrom},
};

#[derive(Clone)]
pub(crate) struct ExporterRequestBuilder<Resource, ResRequest, Signer>
where
    Resource: prost::Message + std::fmt::Debug + Clone + Send + 'static,
    ResRequest: prost::Message + OTLPFrom<Vec<Resource>> + Clone + 'static,
    Signer: Clone,
{
    req_builder: RequestBuilder<ResRequest, Signer>,
    _phantom: PhantomData<Resource>,
}

impl<Resource, ResRequest, Signer> ExporterRequestBuilder<Resource, ResRequest, Signer>
where
    Resource: prost::Message + std::fmt::Debug + Clone + Send + 'static,
    ResRequest: prost::Message + OTLPFrom<Vec<Resource>> + Clone + 'static,
    Signer: Clone,
{
    pub(crate) fn new(req_builder: RequestBuilder<ResRequest, Signer>) -> Self {
        Self{
            req_builder,
            _phantom: PhantomData::default(),
        }
    }
}

impl<Resource, ResRequest, Signer> BuildRequest<Resource, Full<Bytes>>
    for ExporterRequestBuilder<Resource, ResRequest, Signer>
where
    Resource: prost::Message + std::fmt::Debug + Clone + Send + 'static,
    ResRequest: prost::Message + OTLPFrom<Vec<Resource>> + Clone + 'static,
    [Resource]: BatchSizer,
    Signer: RequestSigner + Clone + Send + 'static,
{
    type Output = Vec<Request<Full<Bytes>>>;

    fn build(&self, input: Vec<Resource>) -> Result<Self::Output, tower::BoxError> {
        let size = BatchSizer::size_of(input.as_slice());

        let req = self.req_builder.encode(ResRequest::otlp_from(input), size)?;

        Ok(vec![req.request])
    }
}

use crate::aws_api::auth::{AwsRequestSigner, SystemClock};
use crate::aws_api::config::AwsConfig;
use crate::aws_api::host::parse_aws_hostname;
use bytes::Bytes;
use http::{HeaderMap, Method, Request, Uri};
use http_body_util::Full;
use tower::BoxError;

pub trait RequestSignerBuilder {
    type Signer;

    fn build(&self, uri: &str) -> Result<Self::Signer, BoxError>;
}

pub trait RequestSigner {
    fn sign(
        &self,
        method: Method,
        headers: HeaderMap,
        body: Bytes,
    ) -> Result<Request<Full<Bytes>>, BoxError>;
}

#[derive(Clone)]
pub struct AwsSigv4RequestSignerBuilder {
    config: AwsConfig,
}

impl AwsSigv4RequestSignerBuilder {
    pub fn new(config: AwsConfig) -> Self {
        Self { config }
    }
}

impl RequestSignerBuilder for AwsSigv4RequestSignerBuilder {
    type Signer = AwsSigv4RequestSigner;

    fn build(&self, uri: &str) -> Result<Self::Signer, BoxError> {
        let uri_parse = match uri.parse::<Uri>() {
            Ok(u) => u,
            Err(_) => {
                return Err(format!("unable to parse signing host from uri: {}", uri).into());
            }
        };

        let host = match uri_parse.host() {
            None => return Err(format!("unable to find host in signing uri: {}", uri).into()),
            Some(h) => h,
        };

        let svc = match parse_aws_hostname(host) {
            None => return Err(format!("unable to match AWS host in signing uri: {}", host).into()),
            Some(svc) => svc,
        };

        // XXX: leak these so that we can elide the lifetime of signer
        let svc = Box::leak(Box::new(svc));
        let config = Box::leak(Box::new(self.config.clone()));

        let signer = AwsRequestSigner::new(
            &svc.service,
            &svc.region,
            &config.aws_access_key_id,
            &config.aws_secret_access_key,
            config.aws_session_token.as_deref(),
            SystemClock {},
        );

        Ok(AwsSigv4RequestSigner::new(signer, uri_parse))
    }
}

#[derive(Clone)]
pub struct AwsSigv4RequestSigner {
    signer: AwsRequestSigner<'static, SystemClock>,
    uri: Uri,
}

impl AwsSigv4RequestSigner {
    pub fn new(signer: AwsRequestSigner<'static, SystemClock>, uri: Uri) -> Self {
        Self { signer, uri }
    }
}

impl RequestSigner for AwsSigv4RequestSigner {
    fn sign(
        &self,
        method: Method,
        headers: HeaderMap,
        body: Bytes,
    ) -> Result<Request<Full<Bytes>>, BoxError> {
        match self.signer.sign(self.uri.clone(), method, headers, body) {
            Ok(req) => Ok(req),
            Err(e) => Err(format!("unable to sign request: {}", e).into()),
        }
    }
}

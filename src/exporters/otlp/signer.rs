use crate::aws_api::auth::{AwsRequestSigner, SystemClock};
use crate::aws_api::config::AwsConfig;
use crate::aws_api::host::parse_aws_hostname;
use bytes::Bytes;
use http::{HeaderMap, Method, Request, Uri};
use http_body_util::Full;
use tower::BoxError;

pub trait RequestSigner {
    fn sign(
        &self,
        uri: &str,
        method: Method,
        headers: HeaderMap,
        body: Bytes,
    ) -> Result<Request<Full<Bytes>>, BoxError>;
}

#[derive(Clone)]
pub struct AwsSigv4RequestSigner {
    config: AwsConfig,
}

impl AwsSigv4RequestSigner {
    pub fn new(config: AwsConfig) -> Self {
        Self { config }
    }
}

impl RequestSigner for AwsSigv4RequestSigner {
    fn sign(
        &self,
        uri: &str,
        method: Method,
        headers: HeaderMap,
        body: Bytes,
    ) -> Result<Request<Full<Bytes>>, BoxError> {
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

        let signer = AwsRequestSigner::new(
            &svc.service,
            &svc.region,
            &self.config.aws_access_key_id,
            &self.config.aws_secret_access_key,
            self.config.aws_session_token.as_deref(),
            SystemClock {},
        );

        match signer.sign(uri_parse, method, headers, body) {
            Ok(req) => Ok(req),
            Err(e) => Err(format!("unable to sign request: {}", e).into()),
        }
    }
}

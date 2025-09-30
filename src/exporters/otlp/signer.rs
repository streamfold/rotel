use crate::aws_api::config::AwsConfig;
use crate::aws_api::host::parse_aws_hostname;
use crate::exporters::otlp::request::RequestBuilder;
use crate::exporters::shared::aws_signing_service::AwsSigningServiceBuilder;
use http::Uri;
use tower::BoxError;

pub fn get_signing_service_builder<T: prost::Message>(
    req_builder: &RequestBuilder<T>,
    aws_cfg: AwsConfig,
) -> Result<AwsSigningServiceBuilder, BoxError> {
    let uri = req_builder.uri();

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

    Ok(AwsSigningServiceBuilder::new(
        &svc.service,
        &svc.region,
        aws_cfg,
    ))
}

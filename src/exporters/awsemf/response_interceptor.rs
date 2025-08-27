// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tower::util::ServiceExt;
use tower::{BoxError, Service};
use tracing::warn;

use super::cloudwatch::Cloudwatch;
use super::errors::AwsEmfResponse;
use crate::exporters::http::response::Response;

/// Middleware that intercepts responses from the client layer and potentially
/// invokes methods on CloudwatchAPI before passing the response to the retry layer
#[derive(Clone)]
pub(crate) struct ResponseInterceptor<S> {
    inner: S,
    cloudwatch_api: Arc<Cloudwatch>,
}

impl<S> ResponseInterceptor<S> {
    pub(crate) fn new(inner: S, cloudwatch_api: Arc<Cloudwatch>) -> Self {
        Self {
            inner,
            cloudwatch_api,
        }
    }
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for ResponseInterceptor<S>
where
    S: Service<http::Request<ReqBody>, Response = Response<AwsEmfResponse>, Error = BoxError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = Response<AwsEmfResponse>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let cloudwatch_api = self.cloudwatch_api.clone();

        Box::pin(async move {
            // Call the inner service (timeout -> client)
            let response = inner.ready().await?.call(req).await;

            if let Ok(resp) = &response {
                // Call CloudwatchAPI method based on the response
                if let Err(e) = process_response(cloudwatch_api, resp).await {
                    warn!("Failed to process response with CloudWatch API: {}", e);
                    // Continue with original response even if CloudWatch processing fails
                }
            }

            response
        })
    }
}

async fn process_response(
    cloudwatch_api: Arc<Cloudwatch>,
    response: &Response<AwsEmfResponse>,
) -> Result<(), BoxError> {
    let body = match response {
        crate::exporters::http::response::Response::Http(_, Some(body)) => body,
        crate::exporters::http::response::Response::Grpc(_, Some(body)) => body,
        _ => return Ok(()), // No body to process
    };

    match body {
        AwsEmfResponse::ResourceNotFoundException(_) => {
            // Resource not found - maybe create log group/stream
            cloudwatch_api.create_stream().await?;
        }
        _ => {}
    }

    Ok(())
}

// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::client::ConnectError;
use crate::exporters::http::response::Response;
use std::fmt::Debug;
use std::future::Future;
use std::ops::Sub;
use std::pin::Pin;
use std::time::Duration;
use tokio::{
    select,
    sync::broadcast::{self, Sender},
    time::Instant,
};
use tower::BoxError;
use tower::retry::Policy;
use tracing::info;

#[derive(Clone)]
pub struct RetryConfig {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_elapsed_time: Duration,
    pub indefinite_retry: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(30),
            max_elapsed_time: Duration::from_secs(300),
            indefinite_retry: false,
        }
    }
}

// Custom function for determining if a request should be retried.
// Returns:
//   Some(cond): Should or should not be retried based on cond
//   None: Fall back to status code checks
type RetryableFn<Resp> = fn(&Result<Response<Resp>, BoxError>) -> Option<bool>;

#[derive(Clone)]
pub struct RetryPolicy<Resp> {
    config: RetryConfig,
    current_backoff: Duration,
    request_start: Option<Instant>,
    retry_broadcast: Sender<bool>,
    attempts: u32,
    is_retryable: Option<RetryableFn<Resp>>,
}

impl<Resp> RetryPolicy<Resp> {
    fn should_retry(&self, now: Instant, result: &Result<Response<Resp>, BoxError>) -> bool {
        let start = self.request_start.unwrap();

        // If not indefinite retry, check if we've exceeded the max elapsed time
        if !self.config.indefinite_retry
            && now.gt(&start)
            && now.sub(start) >= self.config.max_elapsed_time
        {
            return false;
        }

        if result.is_err() {
            let err = result.as_ref().err().unwrap();

            // Timeouts are always retried
            let elapsed_err = err.downcast_ref::<tower::timeout::error::Elapsed>();
            if elapsed_err.is_some() {
                return true;
            }

            // As are connection errors
            let connect_err = err.downcast_ref::<ConnectError>();
            if connect_err.is_some() {
                return true;
            }
        }

        if let Some(is_retryable) = self.is_retryable {
            match (is_retryable)(result) {
                Some(res) => return res,
                None => {}
            }
        }

        // Fall back to the HTTP/GRPC status
        if let Ok(resp) = result {
            return match resp {
                Response::Http(parts, _, _) => {
                    match parts.status.as_u16() {
                        // No need to retry success
                        200..=202 => false,
                        408 | 429 => true,
                        500..=504 => true,
                        _ => false,
                    }
                }
                Response::Grpc(status, _, _) => {
                    matches!(
                        status.code(),
                        tonic::Code::Unavailable |     // Service temporarily unavailable
                            tonic::Code::Internal |        // Internal server error
                            tonic::Code::DeadlineExceeded| // Request timeout
                            tonic::Code::ResourceExhausted // Server overloaded
                    )
                }
            };
        }

        false
    }

    pub fn new(retry_config: RetryConfig, is_retryable: Option<RetryableFn<Resp>>) -> Self {
        // We immediately drop the receiver channel and only keep receivers open for
        // active retries. Size mostly needs to be >0.
        let (tx, _) = broadcast::channel(16);

        Self {
            current_backoff: retry_config.initial_backoff,
            config: retry_config,
            retry_broadcast: tx,
            request_start: None,
            attempts: 0,
            is_retryable,
        }
    }

    pub fn retry_broadcast(&self) -> Sender<bool> {
        self.retry_broadcast.clone()
    }
}

impl<Req, Resp> Policy<Req, Response<Resp>, BoxError> for RetryPolicy<Resp>
where
    Req: Clone,
    Resp: Debug,
{
    type Future = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn retry(
        &mut self,
        _req: &mut Req,
        result: &mut Result<Response<Resp>, BoxError>,
    ) -> Option<Self::Future> {
        // Set the request start time. Ideally we would know the start time from the start of the
        // execution of the request, but we don't have that data in the request yet. Instead,
        // the actual max elasped time may include the initial timeout. TODO
        if self.request_start.is_none() {
            self.request_start = Some(Instant::now());
        }

        let now = Instant::now();
        match self.should_retry(now, result) {
            true => {
                self.attempts += 1;

                let backoff_ms = self.current_backoff.as_millis() as i64;

                let mut v = backoff_ms / 2;
                // avoid div by zero
                if v == 0 {
                    v = 1;
                }

                // Exponential backoff with jitter
                let jitter = (rand::random::<i64>() % v) - (v / 2);
                let mut sleep_ms = backoff_ms + jitter;
                if sleep_ms < 0 {
                    sleep_ms = 1;
                }
                let sleep_duration = Duration::from_millis(sleep_ms as u64);

                // If the sleep duration would put us over the maximum elapsed time, then
                // return false to stop retries. Skip this check for indefinite retry.
                if !self.config.indefinite_retry {
                    // Use checked_add to avoid overflow
                    if let Some(deadline) = self
                        .request_start
                        .unwrap()
                        .checked_add(self.config.max_elapsed_time)
                    {
                        if let Some(sleep_end) = now.checked_add(sleep_duration) {
                            if sleep_end > deadline {
                                return None;
                            }
                        }
                    }
                }

                // Log the retry attempt
                match result {
                    Ok(r) => info!(
                        attempt = self.attempts,
                        delay = ?sleep_duration,
                        response = ?r,
                        "Exporting failed, will retry again after delay.",
                    ),
                    Err(e) => info!(
                        attempt = self.attempts,
                        delay = ?sleep_duration,
                        error = e,
                        "Exporting failed, will retry again after delay.",
                    ),
                };

                let mut rx = self.retry_broadcast.subscribe();
                let fut = async move {
                    let delay_fut = tokio::time::sleep(sleep_duration);
                    select! {
                        _ = rx.recv() => {},
                        _ = delay_fut => {},
                    }
                };

                // Increase backoff for next retry, but cap at max_backoff
                self.current_backoff =
                    std::cmp::min(self.current_backoff * 2, self.config.max_backoff);

                Some(Box::pin(fut))
            }
            false => None,
        }
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(req.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::{RetryConfig, RetryPolicy};
    use crate::exporters::http::client::ConnectError;
    use crate::exporters::http::response::Response;
    use crate::exporters::otlp::errors::{ExporterError, is_retryable_error};
    use http::{Request, StatusCode};
    use std::time::Duration;
    use tokio::time::{Instant, timeout};
    use tower::BoxError;
    use tower::retry::Policy;

    // Helper function to create a test request
    fn create_test_request() -> Request<String> {
        Request::builder()
            .uri("http://example.com")
            .body("test body".to_string())
            .unwrap()
    }

    // Helper function to create a response with status code
    fn create_response(status: StatusCode) -> Result<Response<()>, BoxError> {
        // Create a proper HTTP response and extract parts
        let http_response = http::Response::builder().status(status).body(()).unwrap();
        let (parts, _) = http_response.into_parts();

        Ok(Response::from_http(parts, Some(())))
    }

    // Helper function to create a timeout error
    fn create_timeout_error() -> Result<Response<()>, BoxError> {
        Err(Box::new(tower::timeout::error::Elapsed::new()))
    }

    // Helper function to create a connection error
    fn create_connection_error() -> Result<Response<()>, BoxError> {
        Err(Box::new(ConnectError))
    }

    #[test]
    fn test_should_retry_success_status_codes() {
        let config = RetryConfig::default();
        let mut policy = RetryPolicy::<()>::new(config, None);
        policy.request_start = Some(Instant::now());

        // Test 200-202 should not retry
        for status in [200, 201, 202] {
            let response = create_response(StatusCode::from_u16(status).unwrap());
            assert!(!policy.should_retry(Instant::now(), &response));
        }
    }

    #[test]
    fn test_should_retry_retryable_status_codes() {
        let config = RetryConfig::default();
        let mut policy = RetryPolicy::<()>::new(config, None);
        policy.request_start = Some(Instant::now());

        // Test 408, 429, 500-504 should retry
        for status in [408, 429, 500, 501, 502, 503, 504] {
            let response = create_response(StatusCode::from_u16(status).unwrap());
            assert!(policy.should_retry(Instant::now(), &response));
        }
    }

    #[test]
    fn test_should_retry_non_retryable_status_codes() {
        let config = RetryConfig::default();
        let mut policy = RetryPolicy::<()>::new(config, None);
        policy.request_start = Some(Instant::now());

        // Test other status codes should not retry
        for status in [400, 401, 403, 404, 410] {
            let response = create_response(StatusCode::from_u16(status).unwrap());
            assert!(!policy.should_retry(Instant::now(), &response));
        }
    }

    #[test]
    fn test_should_retry_timeout_error() {
        let config = RetryConfig::default();
        let mut policy = RetryPolicy::<()>::new(config, None);
        policy.request_start = Some(Instant::now());

        let timeout_error = create_timeout_error();
        assert!(policy.should_retry(Instant::now(), &timeout_error));
    }

    #[test]
    fn test_should_retry_connection_error() {
        let config = RetryConfig::default();
        let mut policy = RetryPolicy::<()>::new(config, None);
        policy.request_start = Some(Instant::now());

        let connection_error = create_connection_error();
        assert!(policy.should_retry(Instant::now(), &connection_error));
    }

    #[test]
    fn test_should_retry_max_elapsed_time_exceeded() {
        let config = RetryConfig {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
            max_elapsed_time: Duration::from_millis(500),
            indefinite_retry: false,
        };
        let mut policy = RetryPolicy::<()>::new(config, None);

        // Cloning the request should not set a start time
        let _ = policy.clone_request(&create_test_request());
        assert!(policy.request_start.is_none());

        let start = Instant::now();
        policy.request_start = Some(start);

        // Simulate time passing beyond max_elapsed_time
        let future_time = start + Duration::from_millis(600);
        let response = create_response(StatusCode::INTERNAL_SERVER_ERROR);

        assert!(!policy.should_retry(future_time, &response));
    }

    #[test]
    fn test_should_retry_custom_retryable_function() {
        let config = RetryConfig::default();
        let custom_retryable = |result: &Result<Response<()>, BoxError>| {
            match result {
                Ok(resp) => Some(resp.status_code() == StatusCode::BAD_REQUEST), // Custom logic: retry 400
                Err(_) => Some(false), // Don't retry errors
            }
        };
        let mut policy = RetryPolicy::new(config, Some(custom_retryable));
        policy.request_start = Some(Instant::now());

        // Test custom retryable logic
        let bad_request = create_response(StatusCode::BAD_REQUEST);
        assert!(policy.should_retry(Instant::now(), &bad_request));

        let not_found = create_response(StatusCode::NOT_FOUND);
        assert!(!policy.should_retry(Instant::now(), &not_found));

        // Custom function is called for errors too, but timeout/connection errors
        // are handled before the custom function, so they will still retry
        let timeout_error = create_timeout_error();
        assert!(policy.should_retry(Instant::now(), &timeout_error));
    }

    #[test]
    fn test_fallback_to_status_on_none() {
        let config = RetryConfig::default();
        let custom_retryable = |result: &Result<Response<()>, BoxError>| {
            match result {
                Ok(resp) => {
                    if resp.status_code() == StatusCode::BAD_REQUEST {
                        Some(true)
                    } else {
                        None // Should fallback to status code check
                    }
                }
                Err(_) => Some(false), // Don't retry errors
            }
        };
        let mut policy = RetryPolicy::new(config, Some(custom_retryable));
        policy.request_start = Some(Instant::now());

        // Test custom retryable logic
        let bad_request = create_response(StatusCode::BAD_REQUEST);
        assert!(policy.should_retry(Instant::now(), &bad_request));

        // Next two fall back to status code check
        let not_found = create_response(StatusCode::NOT_FOUND);
        assert!(!policy.should_retry(Instant::now(), &not_found));

        let too_many = create_response(StatusCode::TOO_MANY_REQUESTS);
        assert!(policy.should_retry(Instant::now(), &too_many));
    }

    #[tokio::test]
    async fn test_retry_method_returns_future_on_retryable_error() {
        let config = RetryConfig {
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            max_elapsed_time: Duration::from_secs(10),
            indefinite_retry: false,
        };
        let mut policy = RetryPolicy::<()>::new(config, None);
        let mut request = create_test_request();
        let mut result = create_response(StatusCode::INTERNAL_SERVER_ERROR);
        assert!(policy.request_start.is_none());

        let future_opt = policy.retry(&mut request, &mut result);
        assert!(future_opt.is_some());
        assert!(policy.request_start.is_some());

        // Test that future completes
        if let Some(future) = future_opt {
            future.await;
        }

        // Verify attempts were incremented
        assert_eq!(policy.attempts, 1);
    }

    #[tokio::test]
    async fn test_retry_method_returns_immediately_on_broadcast() {
        let config = RetryConfig {
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_millis(1_000),
            max_elapsed_time: Duration::from_secs(5),
            indefinite_retry: false,
        };
        let mut policy = RetryPolicy::<()>::new(config.clone(), None);
        let mut request = create_test_request();
        let mut result = create_response(StatusCode::INTERNAL_SERVER_ERROR);

        let future_opt = policy.retry(&mut request, &mut result);
        assert!(future_opt.is_some());

        // This will timeout
        let res = timeout(Duration::from_millis(2), future_opt.unwrap()).await;
        assert!(res.is_err());

        // Try again with broadcast
        let mut policy = RetryPolicy::<()>::new(config, None);
        let retry_broadcast = policy.retry_broadcast();
        let mut request = create_test_request();
        let mut result = create_response(StatusCode::INTERNAL_SERVER_ERROR);

        let future_opt = policy.retry(&mut request, &mut result);
        assert!(future_opt.is_some());

        let res = retry_broadcast.send(true);
        assert!(res.is_ok());

        // Should be ok
        let res = timeout(Duration::from_millis(2), future_opt.unwrap()).await;
        assert!(res.is_ok());
    }

    #[test]
    fn test_retry_method_exponential_backoff() {
        let config = RetryConfig {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(1000),
            max_elapsed_time: Duration::from_secs(10),
            indefinite_retry: false,
        };
        let mut policy = RetryPolicy::<()>::new(config.clone(), None);
        let mut request = create_test_request();
        let mut result = create_response(StatusCode::INTERNAL_SERVER_ERROR);

        // First retry
        assert_eq!(policy.current_backoff, config.initial_backoff);
        let future_opt = policy.retry(&mut request, &mut result);
        assert!(future_opt.is_some());
        assert_eq!(policy.current_backoff, Duration::from_millis(200)); // 2x initial

        // Second retry
        let future_opt = policy.retry(&mut request, &mut result);
        assert!(future_opt.is_some());
        assert_eq!(policy.current_backoff, Duration::from_millis(400)); // 2x previous

        // Continue until max_backoff
        policy.retry(&mut request, &mut result);
        assert_eq!(policy.current_backoff, Duration::from_millis(800));

        policy.retry(&mut request, &mut result);
        assert_eq!(policy.current_backoff, Duration::from_millis(1000)); // Capped at max_backoff
    }

    #[test]
    fn test_jitter_bounds() {
        // This test verifies that jitter calculation doesn't panic with edge cases
        let config = RetryConfig {
            initial_backoff: Duration::from_millis(1), // Very small backoff
            max_backoff: Duration::from_millis(2),
            max_elapsed_time: Duration::from_secs(10),
            indefinite_retry: false,
        };
        let mut policy = RetryPolicy::<()>::new(config, None);
        let mut request = create_test_request();
        let mut result = create_response(StatusCode::INTERNAL_SERVER_ERROR);

        // Should not panic even with very small backoff values
        let future_opt = policy.retry(&mut request, &mut result);
        assert!(future_opt.is_some());
    }

    fn create_exporter_error(err: ExporterError) -> Result<Response<()>, BoxError> {
        Err(err.into())
    }

    // Since we use this from OTLP Exporter, verify the custom error function
    #[test]
    fn exporter_errors_retried() {
        let config = RetryConfig::default();

        let mut policy = RetryPolicy::new(config, Some(is_retryable_error));
        policy.request_start = Some(Instant::now());

        // Test connect error, should be retried
        let conn_error = create_exporter_error(ExporterError::Connect);
        assert!(policy.should_retry(Instant::now(), &conn_error));

        // Generic errors are not
        let conn_error = create_exporter_error(ExporterError::Generic("unknown".to_string()));
        assert!(!policy.should_retry(Instant::now(), &conn_error));
    }

    #[test]
    fn test_indefinite_retry_no_overflow() {
        // Test that indefinite retry doesn't overflow when checking elapsed time
        let config = RetryConfig {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(1000),
            max_elapsed_time: Duration::from_secs(300), // Doesn't matter for indefinite retry
            indefinite_retry: true,
        };
        let mut policy = RetryPolicy::<()>::new(config, None);
        policy.request_start = Some(Instant::now());

        // Even after a very long time, should still retry with indefinite_retry enabled
        let far_future = Instant::now() + Duration::from_secs(365 * 24 * 60 * 60); // 1 year in the future
        let response = create_response(StatusCode::INTERNAL_SERVER_ERROR);
        assert!(policy.should_retry(far_future, &response));
    }

    #[tokio::test]
    async fn test_indefinite_retry_returns_future() {
        // Test that retry() method returns a future for indefinite retry
        let config = RetryConfig {
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            max_elapsed_time: Duration::from_secs(1), // Doesn't matter for indefinite retry
            indefinite_retry: true,
        };
        let mut policy = RetryPolicy::<()>::new(config, None);
        let mut request = create_test_request();
        let mut result = create_response(StatusCode::INTERNAL_SERVER_ERROR);

        // Even if we've exceeded max_elapsed_time, indefinite retry should still return a future
        policy.request_start = Some(Instant::now() - Duration::from_secs(10)); // Started 10 seconds ago
        let future_opt = policy.retry(&mut request, &mut result);
        assert!(
            future_opt.is_some(),
            "Indefinite retry should return a future even after max_elapsed_time"
        );

        if let Some(future) = future_opt {
            future.await;
        }
    }
}

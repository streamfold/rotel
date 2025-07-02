// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::client::ConnectError;
use crate::exporters::http::response::Response;
use http::Request;
use std::fmt::Debug;
use std::future::Future;
use std::ops::Sub;
use std::pin::Pin;
use std::time::Duration;
use tokio::{select, sync::broadcast::{self, Sender}, time::Instant};
use tower::BoxError;
use tower::retry::Policy;
use tracing::info;

#[derive(Clone)]
pub struct RetryConfig {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_elapsed_time: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(30),
            max_elapsed_time: Duration::from_secs(300),
        }
    }
}

type RetryableFn<Resp> = fn(&Result<Response<Resp>, BoxError>) -> bool;

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
        if now.gt(&start) && now.sub(start) >= self.config.max_elapsed_time {
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
            return (is_retryable)(result);
        }

        // Fall back to HTTP status
        if let Ok(resp) = result {
            return match resp.status_code().as_u16() {
                // No need to retry success
                200..=202 => false,
                408 | 429 => true,
                500..=504 => true,
                _ => false,
            };
        }

        false
    }

    pub fn new(retry_config: RetryConfig, is_retryable: Option<RetryableFn<Resp>>) -> Self {
        // We immediately drop the receiver channel and only keep receivers open for
        // active retries
        let (tx, _) = broadcast::channel(100);

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

impl<ReqBody, Resp> Policy<Request<ReqBody>, Response<Resp>, BoxError> for RetryPolicy<Resp>
where
    Resp: Debug,
    ReqBody: Clone,
{
    type Future = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn retry(
        &mut self,
        _req: &mut Request<ReqBody>,
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
                // return false to stop retries.
                if now + sleep_duration > self.request_start.unwrap() + self.config.max_elapsed_time
                {
                    return None;
                }

                // Log the retry attempt
                info!(
                    attempt = self.attempts,
                    delay = ?sleep_duration,
                    status = ?result,
                    "Exporting failed, will retry again after delay.",
                );

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

    fn clone_request(&mut self, req: &Request<ReqBody>) -> Option<Request<ReqBody>> {
        Some(req.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::{RetryConfig, RetryPolicy};
    use crate::exporters::http::client::ConnectError;
    use crate::exporters::http::response::Response;
    use http::{Request, StatusCode};
    use std::time::Duration;
    use tokio::time::Instant;
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
                Ok(resp) => resp.status_code() == StatusCode::BAD_REQUEST, // Custom logic: retry 400
                Err(_) => false,                                           // Don't retry errors
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

    #[tokio::test]
    async fn test_retry_method_returns_future_on_retryable_error() {
        let config = RetryConfig {
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            max_elapsed_time: Duration::from_secs(10),
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

    #[test]
    fn test_retry_method_exponential_backoff() {
        let config = RetryConfig {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(1000),
            max_elapsed_time: Duration::from_secs(10),
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
        };
        let mut policy = RetryPolicy::<()>::new(config, None);
        let mut request = create_test_request();
        let mut result = create_response(StatusCode::INTERNAL_SERVER_ERROR);

        // Should not panic even with very small backoff values
        let future_opt = policy.retry(&mut request, &mut result);
        assert!(future_opt.is_some());
    }
}

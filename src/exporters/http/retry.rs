// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::client::ConnectError;
use crate::exporters::http::response::Response;
use crate::exporters::http::types::Request;
use std::fmt::Debug;
use std::future::Future;
use std::ops::Sub;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::Instant;
use tower::BoxError;
use tower::retry::Policy;
use tracing::{info, warn};

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
        Self {
            current_backoff: retry_config.initial_backoff,
            config: retry_config,
            request_start: None,
            attempts: 0,
            is_retryable,
        }
    }
}

impl<Resp> Policy<Request, Response<Resp>, BoxError> for RetryPolicy<Resp>
where
    Resp: Debug,
{
    type Future = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn retry(
        &mut self,
        _req: &mut Request,
        result: &mut Result<Response<Resp>, BoxError>,
    ) -> Option<Self::Future> {
        // Should never happen
        if self.request_start.is_none() {
            warn!("Request start time not set in retry policy, refusing retry.");
            return None;
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

                let fut = async move {
                    tokio::time::sleep(sleep_duration).await;
                };

                // Increase backoff for next retry, but cap at max_backoff
                self.current_backoff =
                    std::cmp::min(self.current_backoff * 2, self.config.max_backoff);

                Some(Box::pin(fut))
            }
            false => None,
        }
    }

    fn clone_request(&mut self, req: &Request) -> Option<Request> {
        // Set the request start time
        if self.request_start.is_none() {
            self.request_start = Some(Instant::now());
        }
        Some(req.clone())
    }
}

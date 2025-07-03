// SPDX-License-Identifier: Apache-2.0

use crate::exporters::otlp::errors::ExporterError;
use crate::exporters::otlp::request::EncodedRequest;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
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

#[derive(Clone)]
pub struct RetryPolicy<T> {
    config: RetryConfig,
    current_backoff: Duration,
    request_start: Option<Instant>,
    retry_broadcast: Sender<bool>,
    attempts: u32,
    is_retryable: fn(&ExporterError) -> bool,
    _phantom: PhantomData<T>,
}

impl<T> RetryPolicy<T> {
    fn should_retry(&mut self, now: Instant, result: &Result<T, BoxError>) -> bool {
        if result.is_ok() {
            return false;
        }

        let start = self.request_start.unwrap();
        if now.gt(&start) && now.sub(start) >= self.config.max_elapsed_time {
            return false;
        }

        let err = result.as_ref().err().unwrap();

        let elapsed_err = err.downcast_ref::<tower::timeout::error::Elapsed>();
        if elapsed_err.is_some() {
            return true;
        }

        let downcast_err = err.downcast_ref::<ExporterError>();
        if downcast_err.is_none_or(|e| !(self.is_retryable)(e)) {
            return false;
        }

        true
    }

    pub fn new(retry_config: RetryConfig, is_retryable: fn(&ExporterError) -> bool) -> Self {
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
            _phantom: PhantomData,
        }
    }

    pub fn retry_broadcast(&self) -> Sender<bool> {
        self.retry_broadcast.clone()
    }
}

impl<T: Debug + Clone + Send + 'static> Policy<EncodedRequest, T, BoxError> for RetryPolicy<T> {
    type Future = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn retry(
        &mut self,
        _req: &mut EncodedRequest,
        result: &mut Result<T, BoxError>,
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

    fn clone_request(&mut self, req: &EncodedRequest) -> Option<EncodedRequest> {
        Some(req.clone())
    }
}

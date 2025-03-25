// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::tls::{Config, ConfigBuilder};
use crate::exporters::otlp::{CompressionEncoding, Endpoint, Protocol, DEFAULT_REQUEST_TIMEOUT};
use crate::exporters::retry::RetryConfig;
use std::time::Duration;

// We expect these configs might diverge, for now they are type references to the same underlying type.
pub type OTLPExporterTracesConfig = OTLPExporterConfig;
pub type OTLPExporterMetricsConfig = OTLPExporterConfig;
pub type OTLPExporterLogsConfig = OTLPExporterConfig;

#[derive(Clone)]
pub struct OTLPExporterConfig {
    pub(crate) type_name: String,
    pub(crate) endpoint: Endpoint,
    pub(crate) protocol: Protocol,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) compression: Option<CompressionEncoding>,
    pub(crate) retry_config: RetryConfig,
    pub(crate) request_timeout: Duration,
    pub(crate) encode_drain_max_time: Duration,
    pub(crate) export_drain_max_time: Duration,
    pub(crate) tls_cfg_builder: ConfigBuilder,
}

impl Default for OTLPExporterConfig {
    fn default() -> Self {
        Self {
            type_name: "".to_string(),
            endpoint: Endpoint::Base("".to_string()),
            protocol: Protocol::Grpc,
            headers: vec![],
            tls_cfg_builder: Config::builder(),
            compression: Some(CompressionEncoding::Gzip),
            retry_config: RetryConfig::default(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            encode_drain_max_time: Duration::from_secs(2),
            export_drain_max_time: Duration::from_secs(3),
        }
    }
}

impl OTLPExporterConfig {
    pub fn with_cert_file(mut self, cert_file: &str) -> Self {
        self.tls_cfg_builder = self.tls_cfg_builder.with_cert_file(cert_file.to_string());
        self
    }

    pub fn with_cert_pem(mut self, cert_pem: &str) -> Self {
        self.tls_cfg_builder = self.tls_cfg_builder.with_cert_pem(cert_pem.to_string());
        self
    }

    pub fn with_key_file(mut self, key_file: &str) -> Self {
        self.tls_cfg_builder = self.tls_cfg_builder.with_key_file(key_file.to_string());
        self
    }

    pub fn with_key_pem(mut self, key_pem: &str) -> Self {
        self.tls_cfg_builder = self.tls_cfg_builder.with_key_pem(key_pem.to_string());
        self
    }

    pub fn with_ca_file(mut self, ca_file: &str) -> Self {
        self.tls_cfg_builder = self.tls_cfg_builder.with_ca_file(ca_file.to_string());
        self
    }

    pub fn with_ca_pem(mut self, ca_pem: &str) -> Self {
        self.tls_cfg_builder = self.tls_cfg_builder.with_ca_pem(ca_pem.to_string());
        self
    }

    pub fn with_tls_skip_verify(mut self, skip_verify: bool) -> Self {
        self.tls_cfg_builder = self.tls_cfg_builder.with_tls_skip_verify(skip_verify);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_header(mut self, header_name: &str, header_value: &str) -> Self {
        self.headers
            .push((header_name.to_string(), header_value.to_string()));
        self
    }

    pub fn with_headers(mut self, headers: &[(String, String)]) -> Self {
        headers
            .iter()
            .for_each(|kv| self.headers.push(kv.to_owned()));
        self
    }

    pub fn with_compression_encoding(mut self, encoding: Option<CompressionEncoding>) -> Self {
        self.compression = encoding;
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_max_elapsed_time(mut self, max_elapsed_time: Duration) -> Self {
        self.retry_config.max_elapsed_time = max_elapsed_time;
        self
    }

    pub fn with_initial_backoff(mut self, backoff: Duration) -> Self {
        self.retry_config.initial_backoff = backoff;
        self
    }

    pub fn with_max_backoff(mut self, backoff: Duration) -> Self {
        self.retry_config.max_backoff = backoff;
        self
    }

    pub fn with_encode_drain_max_time(mut self, max_time: Duration) -> Self {
        self.encode_drain_max_time = max_time;
        self
    }

    pub fn with_export_drain_max_time(mut self, max_time: Duration) -> Self {
        self.export_drain_max_time = max_time;
        self
    }
}

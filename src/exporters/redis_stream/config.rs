// SPDX-License-Identifier: Apache-2.0

use super::stream_key::StreamKeyTemplate;

#[derive(Clone, Debug, PartialEq)]
pub enum SerializationFormat {
    Json,
    Flat,
}

impl Default for SerializationFormat {
    fn default() -> Self {
        SerializationFormat::Json
    }
}

#[derive(Clone, Debug)]
pub struct RedisStreamExporterConfig {
    pub endpoint: String,
    pub stream_key_template: StreamKeyTemplate,
    pub format: SerializationFormat,
    pub maxlen: Option<usize>,
    pub cluster_mode: bool,
    pub ca_cert_path: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub pipeline_size: Option<usize>,
    pub filter_service_names: Vec<String>,
}

impl Default for RedisStreamExporterConfig {
    fn default() -> Self {
        Self {
            endpoint: "redis://localhost:6379".to_string(),
            stream_key_template: StreamKeyTemplate::parse("rotel:traces"),
            format: SerializationFormat::default(),
            maxlen: None,
            cluster_mode: false,
            ca_cert_path: None,
            username: None,
            password: None,
            pipeline_size: None,
            filter_service_names: Vec::new(),
        }
    }
}

impl RedisStreamExporterConfig {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            ..Default::default()
        }
    }

    pub fn with_stream_key_template(mut self, template: StreamKeyTemplate) -> Self {
        self.stream_key_template = template;
        self
    }

    pub fn with_format(mut self, format: SerializationFormat) -> Self {
        self.format = format;
        self
    }

    pub fn with_maxlen(mut self, maxlen: Option<usize>) -> Self {
        self.maxlen = maxlen;
        self
    }

    pub fn with_cluster_mode(mut self, cluster_mode: bool) -> Self {
        self.cluster_mode = cluster_mode;
        self
    }

    pub fn with_ca_cert_path(mut self, ca_cert_path: Option<String>) -> Self {
        self.ca_cert_path = ca_cert_path;
        self
    }

    pub fn with_username(mut self, username: Option<String>) -> Self {
        self.username = username;
        self
    }

    pub fn with_password(mut self, password: Option<String>) -> Self {
        self.password = password;
        self
    }

    pub fn with_pipeline_size(mut self, pipeline_size: Option<usize>) -> Self {
        self.pipeline_size = pipeline_size;
        self
    }

    pub fn with_filter_service_names(mut self, filter_service_names: Vec<String>) -> Self {
        self.filter_service_names = filter_service_names;
        self
    }
}

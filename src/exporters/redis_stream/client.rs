// SPDX-License-Identifier: Apache-2.0

use super::config::RedisStreamExporterConfig;
use super::errors::{RedisStreamExportError, Result};
use redis::aio::ConnectionManager;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{Client, ConnectionInfo, IntoConnectionInfo, Pipeline, TlsCertificates};
use tracing::info;

pub(crate) enum RedisConnection {
    Standalone(ConnectionManager),
    Cluster(ClusterConnection),
}

impl RedisConnection {
    pub async fn connect(config: &RedisStreamExporterConfig) -> Result<Self> {
        let tls_certs = build_tls_certs(config)?;

        if config.cluster_mode {
            Self::connect_cluster(config, tls_certs).await
        } else {
            Self::connect_standalone(config, tls_certs).await
        }
    }

    async fn connect_standalone(
        config: &RedisStreamExporterConfig,
        tls_certs: Option<TlsCertificates>,
    ) -> Result<Self> {
        let conn_info = build_connection_info(&config.endpoint, config)?;

        let client = match tls_certs {
            Some(certs) => Client::build_with_tls(conn_info, certs)?,
            None => Client::open(conn_info)?,
        };

        let conn = ConnectionManager::new(client).await?;
        info!(
            endpoint = config.endpoint,
            "Connected to Redis (standalone)"
        );
        Ok(RedisConnection::Standalone(conn))
    }

    async fn connect_cluster(
        config: &RedisStreamExporterConfig,
        tls_certs: Option<TlsCertificates>,
    ) -> Result<Self> {
        let nodes: Vec<ConnectionInfo> = config
            .endpoint
            .split(',')
            .map(|url| build_connection_info(url.trim(), config))
            .collect::<Result<Vec<_>>>()?;

        let mut builder = ClusterClient::builder(nodes);
        if let Some(certs) = tls_certs {
            builder = builder.certs(certs);
        }
        let client = builder
            .build()
            .map_err(RedisStreamExportError::RedisError)?;

        let conn = client.get_async_connection().await?;
        info!(endpoint = config.endpoint, "Connected to Redis (cluster)");
        Ok(RedisConnection::Cluster(conn))
    }

    pub async fn execute_pipeline(&mut self, pipeline: &Pipeline) -> Result<()> {
        match self {
            RedisConnection::Standalone(conn) => {
                pipeline
                    .query_async::<()>(conn)
                    .await
                    .map_err(RedisStreamExportError::RedisError)?;
            }
            RedisConnection::Cluster(conn) => {
                pipeline
                    .query_async::<()>(conn)
                    .await
                    .map_err(RedisStreamExportError::RedisError)?;
            }
        }
        Ok(())
    }
}

/// Build a ConnectionInfo from a URL string, injecting username/password from config.
fn build_connection_info(
    url: &str,
    config: &RedisStreamExporterConfig,
) -> Result<ConnectionInfo> {
    let mut conn_info = url
        .into_connection_info()
        .map_err(RedisStreamExportError::RedisError)?;

    if let Some(ref username) = config.username {
        conn_info.redis.username = Some(username.clone());
    }
    if let Some(ref password) = config.password {
        conn_info.redis.password = Some(password.clone());
    }

    Ok(conn_info)
}

/// Build TlsCertificates if a custom CA cert path is configured.
fn build_tls_certs(config: &RedisStreamExporterConfig) -> Result<Option<TlsCertificates>> {
    match &config.ca_cert_path {
        Some(path) => {
            let ca_pem = std::fs::read(path).map_err(|e| {
                RedisStreamExportError::TlsError(format!(
                    "Failed to read CA cert file '{}': {}",
                    path, e
                ))
            })?;
            Ok(Some(TlsCertificates {
                client_tls: None,
                root_cert: Some(ca_pem),
            }))
        }
        None => Ok(None),
    }
}

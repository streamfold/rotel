use std::collections::HashSet;
use std::time::Duration;

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use tracing::{debug, warn};
use url::Url;

use crate::exporters::http::tls;

/// Represents the extended column capabilities discovered in a Clickhouse table at startup.
/// When a table was created without the newer columns (e.g., `EventName`), those capabilities
/// are marked as absent so that the exporter writes only the columns that exist in the table.
#[derive(Debug, Clone, Default)]
pub struct TableCapabilities {
    pub columns: HashSet<String>,
}

impl TableCapabilities {
    pub fn has_column(&self, col: &str) -> bool {
        self.columns.contains(col)
    }
}

/// A set of capabilities for all tables used by the Clickhouse exporter.
#[derive(Debug, Clone, Default)]
pub struct ExporterTableCapabilities {
    pub logs: TableCapabilities,
}

/// Probe the Clickhouse HTTP endpoint with `DESCRIBE TABLE` for the logs table,
/// returning the discovered column set. On any error (connection refused, table not
/// found, etc.) we log a warning and return an empty capability set, which causes
/// the exporter to fall back to the minimal baseline schema.
pub async fn probe_table_capabilities(
    endpoint: &str,
    database: &str,
    table_prefix: &str,
    auth_user: Option<&str>,
    auth_password: Option<&str>,
) -> ExporterTableCapabilities {
    let logs_table = format!("{database}.{table_prefix}_logs");

    let logs = describe_table(endpoint, database, &logs_table, auth_user, auth_password)
        .await
        .unwrap_or_else(|e| {
            warn!(
                table = %logs_table,
                error = %e,
                "Failed to describe Clickhouse logs table; falling back to baseline schema."
            );
            TableCapabilities::default()
        });

    ExporterTableCapabilities { logs }
}

/// Send `DESCRIBE TABLE <table>` to Clickhouse over HTTP and return the set of column names.
///
/// The Clickhouse HTTP interface returns `DESCRIBE TABLE` results in `TabSeparated` format
/// by default. The first tab-separated field on every data row is the column name.
async fn describe_table(
    endpoint: &str,
    database: &str,
    table: &str,
    auth_user: Option<&str>,
    auth_password: Option<&str>,
) -> Result<TableCapabilities, Box<dyn std::error::Error + Send + Sync>> {
    let base = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.trim_end_matches('/').to_string()
    } else {
        format!("http://{}", endpoint.trim_end_matches('/'))
    };

    let query = format!("DESCRIBE TABLE {}", table);

    let mut url = Url::parse(&format!("{}/", base))?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("database", database);
        pairs.append_pair("query", &query);
    }

    // Build a hyper client using the project's existing TLS configuration helper.
    let tls_config = tls::Config::default()
        .into_client_config()
        .map_err(|e| format!("failed to build TLS config for DESCRIBE TABLE: {e}"))?;

    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_http1()
        .build();

    let client: HyperClient<_, Empty<Bytes>> = HyperClient::builder(TokioExecutor::new())
        .timer(TokioTimer::new())
        .build(https);

    let uri: hyper::Uri = url.as_str().parse()?;
    let mut req_builder = http::Request::builder().method(http::Method::GET).uri(uri);

    if let Some(user) = auth_user {
        req_builder = req_builder.header("X-ClickHouse-User", user);
    }
    if let Some(password) = auth_password {
        req_builder = req_builder.header("X-ClickHouse-Key", password);
    }

    let request = req_builder.body(Empty::<Bytes>::new())?;

    let response = tokio::time::timeout(Duration::from_secs(10), client.request(request))
        .await
        .map_err(|_| "DESCRIBE TABLE request timed out")?
        .map_err(|e| format!("DESCRIBE TABLE request failed: {e}"))?;

    let status = response.status();
    let body_bytes = response
        .into_body()
        .collect()
        .await
        .map_err(|e| format!("failed to read DESCRIBE TABLE response body: {e}"))?
        .to_bytes();

    if !status.is_success() {
        let body_str = String::from_utf8_lossy(&body_bytes);
        return Err(format!(
            "DESCRIBE TABLE returned HTTP {}: {}",
            status,
            body_str.trim()
        )
        .into());
    }

    let body_str = String::from_utf8_lossy(&body_bytes);
    let mut columns = HashSet::new();

    for line in body_str.lines() {
        // Each line is tab-separated; the first field is the column name.
        if let Some(col_name) = line.split('\t').next() {
            let col_name = col_name.trim();
            if !col_name.is_empty() {
                debug!(table = table, column = col_name, "Discovered table column");
                columns.insert(col_name.to_string());
            }
        }
    }

    Ok(TableCapabilities { columns })
}

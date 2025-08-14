mod ddl;
mod ddl_logs;
mod ddl_metrics;
mod ddl_traces;

use crate::ddl_logs::get_logs_ddl;
use crate::ddl_metrics::get_metrics_ddl;
use crate::ddl_traces::get_traces_ddl;
use bytes::Bytes;
use clap::{Args, Parser, ValueEnum};
use http::{HeaderName, Method};
use http_body_util::{BodyExt, Full};
use hyper_rustls::{ConfigBuilderExt, HttpsConnector};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::{Client as HyperClient, Client};
use hyper_util::rt::{TokioExecutor, TokioTimer};
use rotel::crypto::init_crypto_provider;
use rustls::ClientConfig;
use std::fmt::Display;
use std::process::ExitCode;
use std::str;
use std::time::Duration;
use tower::BoxError;

#[derive(Debug, Args, Clone)]
pub struct CreateDDLArgs {
    /// Clickhouse endpoint
    #[arg(long)]
    pub endpoint: String,

    /// Database
    #[arg(long, default_value = "otel")]
    pub database: String,

    /// Table prefix
    #[arg(long, default_value = "otel")]
    pub table_prefix: String,

    /// User
    #[arg(long)]
    pub user: Option<String>,

    /// Password
    #[arg(long)]
    pub password: Option<String>,

    /// DB engine
    #[arg(long, value_enum, default_value = "MergeTree")]
    pub engine: Engine,

    /// Cluster name
    #[arg(long)]
    pub cluster: Option<String>,

    /// Create trace spans tables
    #[arg(long, default_value = "false")]
    pub traces: bool,

    /// Create logs tables
    #[arg(long, default_value = "false")]
    pub logs: bool,

    /// Create metrics tables
    #[arg(long, default_value = "false")]
    pub metrics: bool,

    /// TTL
    #[arg(long, default_value = "0s")]
    pub ttl: humantime::Duration,

    /// Enable JSON column type
    #[arg(long, default_value = "false")]
    pub enable_json: bool,
}

#[derive(Clone, Debug, Copy, PartialEq, ValueEnum)]
pub enum Engine {
    #[clap(name = "MergeTree")]
    MergeTree,

    #[clap(name = "ReplicatedMergeTree")]
    ReplicatedMergeTree,

    #[clap(name = "Null")]
    Null,
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::MergeTree => write!(f, "MergeTree"),
            Engine::ReplicatedMergeTree => write!(f, "ReplicatedMergeTree"),
            Engine::Null => write!(f, "Null"),
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "clickhouse-ddl")]
#[command(bin_name = "clickhouse-ddl")]
#[command(version, about, long_about = None)]
#[command(subcommand_required = true)]
struct Arguments {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, clap::Subcommand)]
enum Commands {
    /// Create tables
    Create(CreateDDLArgs),

    /// Return version
    Version,
}

#[tokio::main]
async fn main() -> ExitCode {
    let opt = Arguments::parse();

    match opt.command {
        Some(Commands::Create(ddl)) => {
            if !ddl.traces && !ddl.logs && !ddl.metrics {
                eprintln!(
                    "Must select a resource type to create tables for: --traces, --logs, and/or --metrics."
                );
                return ExitCode::FAILURE;
            }

            if let Err(e) = init_crypto_provider() {
                eprintln!("Failed to initialize crypto: {}", e);
                return ExitCode::FAILURE;
            }

            let client = match build_hyper_client() {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("failed to build client: {}", e);
                    return ExitCode::FAILURE;
                }
            };

            let ttl: Duration = ddl.ttl.into();

            if ddl.traces {
                let sql = get_traces_ddl(
                    &ddl.cluster,
                    &ddl.database,
                    &ddl.table_prefix,
                    ddl.engine,
                    &ttl,
                    ddl.enable_json,
                );

                for q in sql {
                    if let Err(e) = execute_ddl(&client, &ddl, q).await {
                        eprintln!("Can not execute DDL: {}", e);
                        return ExitCode::FAILURE;
                    }
                }
            }

            if ddl.logs {
                let sql = get_logs_ddl(
                    &ddl.cluster,
                    &ddl.database,
                    &ddl.table_prefix,
                    ddl.engine,
                    &ttl,
                    ddl.enable_json,
                );

                for q in sql {
                    if let Err(e) = execute_ddl(&client, &ddl, q).await {
                        eprintln!("Can not execute DDL: {}", e);
                        return ExitCode::FAILURE;
                    }
                }
            }

            if ddl.metrics {
                let sql = get_metrics_ddl(
                    &ddl.cluster,
                    &ddl.database,
                    &ddl.table_prefix,
                    ddl.engine,
                    &ttl,
                    ddl.enable_json,
                );

                for q in sql {
                    if let Err(e) = execute_ddl(&client, &ddl, q).await {
                        eprintln!("Can not execute DDL: {}", e);
                        return ExitCode::FAILURE;
                    }
                }
            }

            ExitCode::SUCCESS
        }
        Some(Commands::Version) => {
            println!("clickhouse-ddl {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        None => {
            eprintln!("Must specify command!");
            ExitCode::FAILURE
        }
    }
}

async fn execute_ddl(
    client: &Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
    ddl: &CreateDDLArgs,
    query: String,
) -> Result<(), BoxError> {
    let mut req = hyper::Request::builder()
        .method(Method::POST)
        .uri(ddl.endpoint.clone());

    let headers = req.headers_mut().unwrap();
    if let Some(user) = &ddl.user {
        headers.insert(
            HeaderName::from_static("x-clickhouse-user"),
            user.clone().parse().unwrap(),
        );
    }
    if let Some(password) = &ddl.password {
        headers.insert(
            HeaderName::from_static("x-clickhouse-key"),
            password.clone().parse().unwrap(),
        );
    }

    let req = match req.body(Full::from(Bytes::from(query))) {
        Ok(r) => r,
        Err(e) => return Err(format!("Failed to build request: {}", e).into()),
    };

    let resp = client.request(req).await?;
    if !resp.status().is_success() {
        let (parts, body) = resp.into_parts();
        let body = body.collect().await?.to_bytes();
        return Err(format!(
            "Create DDL statement failed: {}: {}",
            parts.status,
            str::from_utf8(&body)?
        )
        .into());
    }

    Ok(())
}

fn build_hyper_client() -> Result<HyperClient<HttpsConnector<HttpConnector>, Full<Bytes>>, BoxError>
{
    let tls_config = ClientConfig::builder()
        .with_native_roots()?
        .with_no_client_auth();

    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_http2()
        .build();

    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(2)
        .timer(TokioTimer::new())
        .build::<_, Full<Bytes>>(https);

    Ok(client)
}

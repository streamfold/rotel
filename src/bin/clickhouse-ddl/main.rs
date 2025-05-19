mod ddl;

use crate::ddl::get_traces_ddl;
use bytes::Bytes;
use clap::{Args, Parser};
use http::{HeaderName, Method};
use http_body_util::Full;
use hyper_rustls::{ConfigBuilderExt, HttpsConnector};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use rotel::crypto::init_crypto_provider;
use rustls::ClientConfig;
use std::process::ExitCode;
use std::time::Duration;
use tower::BoxError;

#[derive(Debug, Args, Clone)]
pub struct CreateDDLArgs {
    /// Endpoint
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
    #[arg(long, default_value = "MergeTree")]
    pub engine: String,

    /// Cluster name
    #[arg(long)]
    pub cluster: Option<String>,

    /// Create trace spans tables
    #[arg(long, default_value = "false")]
    pub traces: bool,

    /// Create logs tables
    #[arg(long, default_value = "false")]
    pub logs: bool,

    /// TTL
    #[arg(long, default_value = "0s")]
    pub ttl: humantime::Duration,

    /// Enable JSON
    #[arg(long, default_value = "false")]
    pub enable_json: bool,
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
            if !ddl.traces && !ddl.logs {
                eprintln!("Must select a resource type to create tables for");
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
                    &ddl.engine,
                    &ttl,
                    ddl.enable_json,
                );

                for q in sql {
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

                    let req = req.body(Full::from(Bytes::from(q)));
                    if req.is_err() {
                        eprintln!("Unable to build request");
                        return ExitCode::FAILURE;
                    }

                    let resp = client.request(req.unwrap()).await;
                    match resp {
                        Ok(r) => {
                            if !r.status().is_success() {
                                eprintln!("Create DDL statement failed: {}", r.status());
                                return ExitCode::FAILURE;
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to create DDL: {:?}", e);
                            return ExitCode::FAILURE;
                        }
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

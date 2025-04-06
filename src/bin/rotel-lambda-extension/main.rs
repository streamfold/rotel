extern crate core;

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use http_body_util::Full;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use lambda_extension::{LambdaTelemetryRecord, NextEvent};
use rotel::bounded_channel::bounded;
use rotel::init::agent::Agent;
use rotel::init::args;
use rotel::init::args::{AgentRun, Exporter};
use rotel::init::misc::bind_endpoints;
use rotel::init::wait;
use rotel::lambda;
use rotel::lambda::telemetry_api::TelemetryAPI;
use rotel::listener::Listener;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;
use tokio::select;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tower_http::BoxError;
use tracing::{error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};

pub const SENDING_QUEUE_SIZE: usize = 10;

#[derive(Debug, Parser)]
#[command(name = "rotel-lambda-extension")]
#[command(bin_name = "rotel-lambda-extension")]
struct Arguments {
    #[arg(long, global = true, env = "ROTEL_LOG_LEVEL", default_value = "info")]
    /// Log configuration
    log_level: String,

    #[arg(long, env = "ROTEL_TELEMETRY_ENDPOINT", default_value = "0.0.0.0:8990", value_parser = args::parse_endpoint)]
    telemetry_endpoint: SocketAddr,

    #[arg(
        value_enum,
        long,
        global = true,
        env = "ROTEL_LOG_FORMAT",
        default_value = "text"
    )]
    /// Log format
    log_format: LogFormatArg,

    #[arg(long, global = true, env = "ROTEL_ENVIRONMENT", default_value = "dev")]
    /// Environment
    environment: String,

    // This is ignored in these options, but we keep it here to avoid an error on unknown
    // options
    #[arg(long)]
    env_file: Option<String>,

    #[command(flatten)]
    agent_args: Box<AgentRun>,
}

// Minimal option to allow us to parse out the env from a file
#[derive(Debug, Parser)]
#[clap(ignore_errors = true)]
struct EnvFileArguments {
    #[arg(long, env = "ROTEL_ENV_FILE")]
    env_file: Option<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
pub enum LogFormatArg {
    Text,
    Json,
}

fn main() -> ExitCode {
    let start_time = Instant::now();

    let env_opt = EnvFileArguments::parse();
    if let Some(env_file) = env_opt.env_file {
        if let Err(e) = load_env_file(&env_file) {
            eprintln!("Can not load envfile: {}", e);
            return ExitCode::FAILURE;
        }
    }

    let opt = Arguments::parse();

    let _logger = setup_logging(&opt.log_level);
    let agent = opt.agent_args;
    let mut port_map = match bind_endpoints(&[
        agent.otlp_grpc_endpoint,
        agent.otlp_http_endpoint,
        opt.telemetry_endpoint,
    ]) {
        Ok(ports) => ports,
        Err(e) => {
            eprintln!("ERROR: {}", e);

            return ExitCode::from(1);
        }
    };

    // Remove this, the rest are passed to the agent
    let telemetry_listener = port_map.remove(&opt.telemetry_endpoint).unwrap();

    match run_extension(
        start_time,
        agent,
        port_map,
        telemetry_listener,
        &opt.environment,
    ) {
        Ok(_) => {}
        Err(e) => {
            error!(error = ?e, "Failed to run agent.");
            return ExitCode::from(1);
        }
    }

    ExitCode::SUCCESS
}

fn load_env_file(env_file: &String) -> Result<(), BoxError> {
    let subs = load_env_file_updates(env_file)?;

    for (key, val) in subs {
        unsafe { env::set_var(key, val) }
    }

    Ok(())
}
fn load_env_file_updates(env_file: &String) -> Result<Vec<(String, String)>, BoxError> {
    let mut updates = Vec::new();
    for item in dotenvy::from_filename_iter(env_file)
        .map_err(|e| format!("failed to open env file {}: {}", env_file, e))?
    {
        let (key, val) = item.map_err(|e| format!("unable to parse line: {}", e))?;
        updates.push((key, val))
    }

    Ok(updates)
}

#[tokio::main]
async fn run_extension(
    start_time: Instant,
    agent_args: Box<AgentRun>,
    port_map: HashMap<SocketAddr, Listener>,
    telemetry_listener: Listener,
    env: &String,
) -> Result<(), BoxError> {
    let mut task_join_set = JoinSet::new();
    let client = build_hyper_client();
    let (bus_tx, mut bus_rx) = bounded(10);

    let r = match lambda::api::register(client.clone()).await {
        Ok(r) => r,
        Err(e) => return Err(format!("Failed to register extension: {}", e).into()),
    };

    let agent_cancel = CancellationToken::new();
    {
        let mut agent_args = agent_args;

        // Ensure this is set low
        agent_args.otlp_exporter.otlp_exporter_batch_timeout = "200ms".parse().unwrap();

        if agent_args.exporter == Exporter::Otlp {
            if agent_args.otlp_exporter.otlp_exporter_endpoint.is_none()
                && agent_args
                    .otlp_exporter
                    .otlp_exporter_traces_endpoint
                    .is_none()
                && agent_args
                    .otlp_exporter
                    .otlp_exporter_metrics_endpoint
                    .is_none()
                && agent_args
                    .otlp_exporter
                    .otlp_exporter_logs_endpoint
                    .is_none()
            {
                // todo: We should be able to startup with no config and not fail, identify best
                // default mode.
                info!("Automatically selecting blackhole exporter due to missing endpoint configs");
                agent_args.exporter = Exporter::Blackhole;
            }
        }

        let agent = Agent::default();
        let env = env.clone();
        let token = agent_cancel.clone();
        let agent_fut = async move {
            agent
                .run(agent_args, port_map, SENDING_QUEUE_SIZE, env, token)
                .await
        };
        task_join_set.spawn(agent_fut);
    };

    if let Err(e) = lambda::api::telemetry_subscribe(
        client.clone(),
        &r.extension_id,
        &telemetry_listener.bound_address()?,
    )
    .await
    {
        return Err(format!("Failed to subscribe to telemetry: {}", e).into());
    }

    let telemetry = TelemetryAPI::new(telemetry_listener);
    let telemetry_cancel = CancellationToken::new();
    {
        let token = telemetry_cancel.clone();
        let telemetry_fut = async move { telemetry.run(bus_tx.clone(), token).await };
        task_join_set.spawn(telemetry_fut)
    };

    info!(
        "Rotel Lambda Extension started in {}ms",
        start_time.elapsed().as_millis()
    );

    // Must perform next_request to get the first INVOKE call
    let next_evt = match lambda::api::next_request(client.clone(), &r.extension_id).await {
        Ok(evt) => evt,
        Err(e) => return Err(format!("Failed to read next event: {}", e).into()),
    };

    handle_next_response(next_evt);
    loop {
        'inner: loop {
            select! {
                msg = bus_rx.next() => {
                    if let Some(evt) = msg {
                        if let LambdaTelemetryRecord::PlatformRuntimeDone {..} = evt.record {
                            break 'inner;
                        }
                    }
                },
                e = wait::wait_for_any_task(&mut task_join_set) => {
                    match e {
                        Ok(()) => warn!("Unexpected early exit of extension."),
                        Err(e) => return Err(e),
                    }
                },
            }
        }

        // todo: this is where we would force a flush
        info!("Received a platform runtime done message, invoking next request");
        let next_evt = match lambda::api::next_request(client.clone(), &r.extension_id).await {
            Ok(evt) => evt,
            Err(e) => return Err(format!("Failed to read next event: {}", e).into()),
        };

        if handle_next_response(next_evt) {
            info!("shutdown received, exiting");
            break;
        }
    }

    telemetry_cancel.cancel();
    agent_cancel.cancel();

    // We have 2 seconds to exit
    wait::wait_for_tasks_with_timeout(&mut task_join_set, Duration::from_secs(2)).await?;

    Ok(())
}

fn handle_next_response(evt: NextEvent) -> bool {
    match evt {
        NextEvent::Invoke(invoke) => info!("Received an invoke request: {:?}", invoke),
        NextEvent::Shutdown(_) => return true,
    }

    false
}

type LoggerGuard = tracing_appender::non_blocking::WorkerGuard;

// todo: match logging to the recommended lambda extension approach
fn setup_logging(log_level: &str) -> std::io::Result<LoggerGuard> {
    let (non_blocking_writer, guard) = tracing_appender::non_blocking(std::io::stdout());

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking_writer)
        // disable printing of the module
        .with_target(false)
        // cloudwatch will add time
        .without_time()
        // cloudwatch doesn't play nice with escape codes
        .with_ansi(false)
        .compact();

    let subscriber = Registry::default()
        .with(EnvFilter::new(log_level))
        .with(file_layer);

    tracing::subscriber::set_global_default(subscriber).unwrap();
    Ok(guard)
}

fn build_hyper_client() -> Client<HttpConnector, Full<Bytes>> {
    hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        // todo: make configurable
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(5)
        .timer(TokioTimer::new())
        .build::<_, Full<Bytes>>(HttpConnector::new())
}

#[cfg(test)]
mod test {
    use crate::load_env_file_updates;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_env_var_subs() {
        let tf = write_env_file(vec![
            "ROTEL_FOO=nottouched",
            "ROTEL_SUB=\"Bearer ${TOKEN}\"",
            "ROTEL_DOUBLE_SUB=${TEAM}-${TOKEN}",
            "ROTEL_ESCAPED=\"NotMe\\${TEAM}\"",
        ]);

        unsafe { std::env::set_var("TOKEN", "123abc") };
        unsafe { std::env::set_var("TEAM", "frontend") };

        let tf_path = tf.path().to_str().unwrap().to_string();
        let updates = load_env_file_updates(&tf_path).unwrap();

        assert_eq!(
            vec![
                ("ROTEL_FOO".to_string(), "nottouched".to_string()),
                ("ROTEL_SUB".to_string(), "Bearer 123abc".to_string()),
                (
                    "ROTEL_DOUBLE_SUB".to_string(),
                    "frontend-123abc".to_string()
                ),
                ("ROTEL_ESCAPED".to_string(), "NotMe${TEAM}".to_string())
            ],
            updates
        );
    }

    fn write_env_file(envs: Vec<&str>) -> NamedTempFile {
        let mut tf = NamedTempFile::new().unwrap();

        for env in envs {
            tf.write_all(format!("{}\n", env).as_ref()).unwrap();
        }
        tf.flush().unwrap();

        tf
    }
}

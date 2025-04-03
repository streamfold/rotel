use bytes::Bytes;
use clap::{Parser, ValueEnum};
use http_body_util::Full;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use lambda_extension::{LambdaTelemetryRecord, NextEvent};
use rotel::bounded_channel::bounded;
use rotel::init::agent::Agent;
use rotel::init::args::{AgentRun, Exporter};
use rotel::init::misc::bind_endpoints;
use rotel::init::wait;
use rotel::lambda;
use rotel::lambda::telemetry_api::TelemetryAPI;
use rotel::listener::Listener;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;
use tokio::select;
use tokio::task::JoinSet;
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

    #[command(flatten)]
    agent_args: Box<AgentRun>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
pub enum LogFormatArg {
    Text,
    Json,
}

fn main() -> ExitCode {
    let opt = Arguments::parse();

    let agent = opt.agent_args;
    let port_map = match bind_endpoints(&[agent.otlp_grpc_endpoint, agent.otlp_http_endpoint]) {
        Ok(ports) => ports,
        Err(e) => {
            eprintln!("ERROR: {}", e);

            return ExitCode::from(1);
        }
    };

    let _logger = setup_logging(&opt.log_level);

    match run_agent(agent, port_map, &opt.environment) {
        Ok(_) => {}
        Err(e) => {
            error!(error = ?e, "Failed to run agent.");
            return ExitCode::from(1);
        }
    }

    ExitCode::SUCCESS
}

#[tokio::main]
async fn run_agent(
    agent_args: Box<AgentRun>,
    port_map: HashMap<SocketAddr, Listener>,
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

    let telemetry = TelemetryAPI::bind_and_listen().await?;
    if let Err(e) =
        lambda::api::telemetry_subscribe(client.clone(), &r.extension_id, &telemetry.addr()).await
    {
        return Err(format!("Failed to subscribe to telemetry: {}", e).into());
    }

    let telemetry_cancel = CancellationToken::new();
    {
        let token = telemetry_cancel.clone();
        let telemetry_fut = async move { telemetry.run(bus_tx.clone(), token).await };
        task_join_set.spawn(telemetry_fut)
    };

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

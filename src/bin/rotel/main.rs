// SPDX-License-Identifier: Apache-2.0

use crate::listener::Listener;
use clap::{Parser, ValueEnum};
use rotel::listener;
use rotel::topology::flush_control::{FlushBroadcast, FlushSender};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::ffi::CString;
use std::fs::OpenOptions;
use std::net::SocketAddr;
use std::process::{ExitCode, exit};
use std::time::Duration;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;
use tower::BoxError;
use tracing::log::warn;
use tracing::metadata::LevelFilter;
use tracing::{debug, error, info};
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};

use rotel::init::agent::Agent;
use rotel::init::args::{AgentRun, Receiver};
use rotel::init::misc::bind_endpoints;
use rotel::init::wait;

// Used when daemonized
static WORKDING_DIR: &str = "/"; // TODO

const SENDING_QUEUE_SIZE: usize = 1_000;

const FLUSH_PIPELINE_TIMEOUT_MILLIS: u64 = 500;
const FLUSH_EXPORTERS_TIMEOUT_MILLIS: u64 = 3_000;

#[derive(Debug, clap::Subcommand)]
enum Commands {
    /// Run agent
    Start(Box<AgentRun>),

    /// Return version
    Version,
}

#[derive(Debug, Parser)]
#[command(name = "rotel")]
#[command(bin_name = "rotel")]
#[command(version, about, long_about = None)]
#[command(subcommand_required = true)]
struct Arguments {
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

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
pub enum LogFormatArg {
    Text,
    Json,
}

fn main() -> ExitCode {
    let opt = Arguments::parse();

    match opt.command {
        Some(Commands::Version) => {
            println!("{}", get_version())
        }
        Some(Commands::Start(agent)) => {
            // Attempt to bind ports before we daemonize, so that when the parent process returns
            // the ports are already available for connection.
            let mut port_map = HashMap::new();
            let receivers: Vec<String> = agent
                .receivers
                .clone()
                .unwrap_or("".to_string())
                .split(",")
                .map(|s| s.to_string().to_lowercase())
                .collect();
            if (agent.receiver.is_none() && agent.receivers.is_none()) || // Defaults to OTLP
                matches!(agent.receiver, Some(Receiver::Otlp)) ||
                receivers.contains(&"otlp".to_string())
            {
                port_map = match bind_endpoints(&[
                    agent.otlp_receiver.otlp_grpc_endpoint,
                    agent.otlp_receiver.otlp_http_endpoint,
                ]) {
                    Ok(ports) => ports,
                    Err(e) => {
                        unsafe {
                            if agent.daemon && check_rotel_active(&agent.pid_file) {
                                // If we are already running, ignore the bind failure
                                return ExitCode::SUCCESS;
                            }
                        }
                        eprintln!("ERROR: {}", e);

                        return ExitCode::from(1);
                    }
                };
            }

            if agent.daemon {
                match daemonize(&agent.pid_file, &agent.log_file) {
                    Ok(Some(exitcode)) => return exitcode,
                    Err(e) => {
                        eprintln!("ERROR: failed to daemonize: {:?}", e);
                        return ExitCode::from(1);
                    }
                    _ => {}
                }
            }

            let _guard = match setup_logging(&opt.log_format) {
                Ok(guard) => guard,
                Err(e) => {
                    eprintln!("ERROR: failed to setup logging: {}", e);
                    return ExitCode::from(1);
                }
            };

            match run_agent(agent, port_map, &opt.environment) {
                Ok(_) => {}
                Err(e) => {
                    error!(error = e, "Failed to run agent.");
                    return ExitCode::from(1);
                }
            }
        }
        _ => {
            // it shouldn't be possible to get here since we mark a subcommand as
            // required
            error!("Must specify a command");
            return ExitCode::from(2);
        }
    }

    ExitCode::SUCCESS
}

#[tokio::main]
async fn run_agent(
    agent_args: Box<AgentRun>,
    port_map: HashMap<SocketAddr, Listener>,
    env: &String,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut agent_join_set = JoinSet::new();

    let (mut flush_pipeline_tx, flush_pipeline_sub) = FlushBroadcast::new().into_parts();
    let (mut flush_exporters_tx, flush_exporters_sub) = FlushBroadcast::new().into_parts();

    let cancel_token = CancellationToken::new();
    {
        let token = cancel_token.clone();
        let env = env.clone();
        let agent_fut = async move {
            let agent = Agent::new(agent_args, port_map, SENDING_QUEUE_SIZE, env)
                .with_pipeline_flush(flush_pipeline_sub)
                .with_exporters_flush(flush_exporters_sub);
            agent.run(token).await
        };

        agent_join_set.spawn(agent_fut);
    };

    let mut sig_usr1 = sig(SignalKind::user_defined1());
    loop {
        select! {
            _ = signal_wait() => {
                info!("Shutdown signal received.");
                cancel_token.cancel();
                break;
            },
            _ = sig_usr1.recv() => {
                info!("Signal SIGUSR1 received, invoking a forced flush");
                let flush_start = Instant::now();
                force_flush(&mut flush_pipeline_tx, &mut flush_exporters_tx).await;
                let duration = Instant::now().duration_since(flush_start);
                info!(duration = ?duration, "Finished forced flush request");
            },
            e = wait::wait_for_any_task(&mut agent_join_set) => {
                match e {
                    Ok(()) => warn!("Unexpected early exit of agent."),
                    Err(e) => return Err(e),
                }
                break;
            },
        }
    }

    // Wait for tasks to complete, we use a large timeout here because the agent
    // enforces lower timeouts.
    wait::wait_for_tasks_with_timeout(&mut agent_join_set, Duration::from_secs(10)).await?;

    Ok(())
}

type LoggerGuard = tracing_appender::non_blocking::WorkerGuard;

fn setup_logging(log_format: &LogFormatArg) -> Result<LoggerGuard, BoxError> {
    LogTracer::init().expect("Unable to setup log tracer!");

    let (non_blocking_writer, guard) = tracing_appender::non_blocking(std::io::stdout());

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()?
        .add_directive("opentelemetry=warn".parse()?)
        .add_directive("opentelemetry_sdk=warn".parse()?)
        .add_directive("aws_config=warn".parse()?);

    if *log_format == LogFormatArg::Json {
        let app_name = format!("{}-{}", env!("CARGO_PKG_NAME"), get_version());
        let bunyan_formatting_layer = BunyanFormattingLayer::new(app_name, non_blocking_writer);

        let subscriber = Registry::default()
            .with(filter)
            .with(JsonStorageLayer)
            .with(bunyan_formatting_layer);
        tracing::subscriber::set_global_default(subscriber).unwrap();
    } else {
        use std::io;
        use std::io::IsTerminal;

        // Skip color codes when not in a terminal
        let use_ansi = io::stdout().is_terminal();

        // Create a formatting layer that writes to the file
        let file_layer = tracing_subscriber::fmt::layer()
            .with_writer(non_blocking_writer)
            .with_target(false)
            .with_level(true)
            .with_ansi(use_ansi)
            .compact();

        let subscriber = Registry::default().with(filter).with(file_layer);
        tracing::subscriber::set_global_default(subscriber).unwrap();
    }
    Ok(guard)
}

fn daemonize(pid_file: &String, log_file: &String) -> Result<Option<ExitCode>, Box<dyn Error>> {
    // Do not using tracing logging functions in here, it is not setup until after we daemonize
    let stdout_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(log_file)
        .map_err(|e| format!("failed to open log file: {}: {}", log_file, e))?;
    let stderr_file = stdout_file.try_clone()?;

    let daemonize = daemonize::Daemonize::new()
        .pid_file(pid_file)
        .working_directory(WORKDING_DIR)
        .stdout(stdout_file)
        .stderr(stderr_file);

    match daemonize.start() {
        Ok(_) => Ok(None),
        Err(e) => match e.kind {
            daemonize::ErrorKind::LockPidfile(_) => {
                println!(
                    "Detected existing agent running, if not remove: {}",
                    pid_file
                );
                Ok(Some(ExitCode::SUCCESS))
            }
            _ => Err(e.into()),
        },
    }
}

fn get_version() -> String {
    // Set during CI
    let version_build = option_env!("BUILD_SHORT_SHA").unwrap_or("dev");

    format!("{}-{}", env!("CARGO_PKG_VERSION"), version_build)
}

// Check the lock status of the PID file to see if another version of Rotel is running.
// TODO: We should likely move this to a healthcheck on a known status port rather then
// use something more OS dependent.
unsafe fn check_rotel_active(pid_path: &String) -> bool {
    fn string_to_cstring(path: &String) -> Result<CString, Box<dyn Error>> {
        CString::new(path.clone()).map_err(|e| format!("path contains null: {e}").into())
    }
    let path_c = match string_to_cstring(pid_path) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("PID path string is invalid: {e}");
            exit(1);
        }
    };

    let ret = unwrap_errno(unsafe { libc::open(path_c.as_ptr(), libc::O_RDONLY, 0o666) });
    if ret.0 < 0 {
        return false;
    }

    let ret = unwrap_errno(unsafe { libc::flock(ret.0, libc::LOCK_EX | libc::LOCK_NB) });

    // Close the original file descriptor
    unsafe { libc::close(ret.0) };

    if ret.0 != 0 {
        // Unknown error from flock
        if ret.1 != 11 {
            eprintln!("Unknown error from pid file check: {}", ret.1)
        }
        // Treat this as if we are running
        return true;
    }

    false
}

type LibcRet = libc::c_int;
type Errno = libc::c_int;
fn unwrap_errno(ret: LibcRet) -> (LibcRet, Errno) {
    if ret >= 0 {
        return (ret, 0);
    }

    let errno = std::io::Error::last_os_error()
        .raw_os_error()
        .expect("errno");
    (ret, errno)
}

async fn signal_wait() {
    let mut sig_term = sig(SignalKind::terminate());
    let mut sig_int = sig(SignalKind::interrupt());

    select! {
        _ = sig_term.recv() => {},
        _ = sig_int.recv() => {},
    }
}

fn sig(kind: SignalKind) -> tokio::signal::unix::Signal {
    signal(kind).unwrap()
}

async fn force_flush(pipeline_tx: &mut FlushSender, exporters_tx: &mut FlushSender) {
    let start = Instant::now();
    match timeout(
        Duration::from_millis(FLUSH_PIPELINE_TIMEOUT_MILLIS),
        pipeline_tx.broadcast(None),
    )
    .await
    {
        Err(_) => {
            warn!("timeout waiting to flush pipelines");
            return;
        }
        Ok(Err(e)) => {
            warn!("failed to flush pipelines: {}", e);
            return;
        }
        _ => {}
    }
    let duration = Instant::now().duration_since(start);
    debug!(?duration, "finished flushing pipeline");

    let start = Instant::now();
    match timeout(
        Duration::from_millis(FLUSH_EXPORTERS_TIMEOUT_MILLIS),
        exporters_tx.broadcast(None),
    )
    .await
    {
        Err(_) => {
            warn!("timeout waiting to flush exporters");
            return;
        }
        Ok(Err(e)) => {
            warn!("failed to flush exporters: {}", e);
            return;
        }
        _ => {}
    }
    let duration = Instant::now().duration_since(start);
    debug!(?duration, "finished flushing exporters");
}

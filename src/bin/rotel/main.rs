// SPDX-License-Identifier: Apache-2.0

use crate::listener::Listener;
use clap::{Parser, ValueEnum};
use rotel::listener;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::ffi::CString;
use std::fs::OpenOptions;
use std::net::SocketAddr;
use std::process::{exit, ExitCode};
use tracing::error;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};

use rotel::init::agent::Agent;
use rotel::init::args::AgentRun;

// Used when daemonized
static WORKDING_DIR: &str = "/"; // TODO

const SENDING_QUEUE_SIZE: usize = 1_000;

#[derive(Debug, clap::Subcommand)]
pub enum Commands {
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
pub struct Arguments {
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
            let port_map =
                match bind_endpoints(&[agent.otlp_grpc_endpoint, agent.otlp_http_endpoint]) {
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

            let _logger = setup_logging(&opt.log_level, &opt.log_format);

            match run_agent(agent, port_map, &opt.environment) {
                Ok(_) => {}
                Err(e) => {
                    error!(error = ?e, "Failed to run agent.");
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
    let agent = Agent::default();

    agent
        .run(agent_args, port_map, SENDING_QUEUE_SIZE, env)
        .await
}

type LoggerGuard = tracing_appender::non_blocking::WorkerGuard;

fn setup_logging(log_level: &str, log_format: &LogFormatArg) -> std::io::Result<LoggerGuard> {
    LogTracer::init().expect("Unable to setup log tracer!");

    let (non_blocking_writer, guard) = tracing_appender::non_blocking(std::io::stdout());

    if *log_format == LogFormatArg::Json {
        let app_name = format!("{}-{}", env!("CARGO_PKG_NAME"), get_version());
        let bunyan_formatting_layer = BunyanFormattingLayer::new(app_name, non_blocking_writer);

        let subscriber = Registry::default()
            .with(EnvFilter::new(log_level))
            .with(JsonStorageLayer)
            .with(bunyan_formatting_layer);
        tracing::subscriber::set_global_default(subscriber).unwrap();
    } else {
        // Create a formatting layer that writes to the file
        let file_layer = tracing_subscriber::fmt::layer()
            .with_writer(non_blocking_writer)
            .with_target(false)
            .with_level(true)
            .compact();

        let subscriber = Registry::default()
            .with(EnvFilter::new(log_level))
            .with(file_layer);
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

fn bind_endpoints(
    endpoints: &[SocketAddr],
) -> Result<HashMap<SocketAddr, Listener>, Box<dyn Error + Send + Sync>> {
    endpoints
        .iter()
        .map(|endpoint| match Listener::listen_std(*endpoint) {
            Ok(l) => Ok((*endpoint, l)),
            Err(e) => Err(e),
        })
        .collect()
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

    let ret = unwrap_errno(libc::open(path_c.as_ptr(), libc::O_RDONLY, 0o666));
    if ret.0 < 0 {
        return false;
    }

    let ret = unwrap_errno(libc::flock(ret.0, libc::LOCK_EX | libc::LOCK_NB));

    // Close the original file descriptor
    libc::close(ret.0);

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

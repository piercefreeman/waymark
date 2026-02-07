#![allow(clippy::collapsible_if)]
//! Boot Waymark Singleton - Ensures a single bridge server instance is running.
//!
//! This binary:
//! 1. Probes a range of ports to find an existing Waymark bridge server via gRPC health check
//! 2. If found, outputs the existing server's port
//! 3. If not found, spawns a new server and outputs its port
//!
//! Usage:
//!   boot-waymark-singleton [--port-file <path>]
//!
//! The port file will contain the gRPC port number on success.

use std::{
    env, fs,
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;
use tracing::{debug, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Number of ports to probe
const PORT_PROBE_COUNT: u16 = 10;

/// Health check timeout
const HEALTH_TIMEOUT: Duration = Duration::from_secs(2);

/// Startup wait time for new server
const STARTUP_WAIT: Duration = Duration::from_secs(5);

/// Default gRPC port for the bridge server
const DEFAULT_GRPC_PORT: u16 = 24117;

/// The gRPC service name we check for health
const HEALTH_SERVICE_NAME: &str = "waymark.messages.WorkflowService";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "boot_waymark_singleton=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args: Vec<String> = env::args().collect();
    let port_file = parse_port_file_arg(&args);

    let (host, base_port) = grpc_host_port();
    info!(host = %host, base_port, "probing for existing waymark bridge");

    if let Some(port) = probe_existing_server(&host, base_port).await {
        info!(port, "found existing waymark bridge");
        write_port_file(&port_file, port)?;
        return Ok(());
    }

    info!(host = %host, base_port, "no existing bridge found, spawning new instance");

    let grpc_port = spawn_server(&host, base_port).await?;
    info!(port = grpc_port, "waymark bridge started");
    write_port_file(&port_file, grpc_port)?;

    Ok(())
}

fn parse_port_file_arg(args: &[String]) -> Option<PathBuf> {
    let mut iter = args.iter().peekable();
    while let Some(arg) = iter.next() {
        if arg == "--port-file" || arg == "--output-file" {
            return iter.next().map(PathBuf::from);
        }
    }
    None
}

fn grpc_host_port() -> (String, u16) {
    if let Ok(addr) = env::var("WAYMARK_BRIDGE_GRPC_ADDR") {
        if let Some((host, port)) = addr.rsplit_once(':') {
            if let Ok(port) = port.parse::<u16>() {
                return (host.to_string(), port);
            }
        }
    }

    let host = env::var("WAYMARK_BRIDGE_GRPC_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let base_port = env::var("WAYMARK_BRIDGE_GRPC_PORT")
        .ok()
        .or_else(|| env::var("WAYMARK_BRIDGE_BASE_PORT").ok())
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_GRPC_PORT);

    (host, base_port)
}

async fn probe_existing_server(host: &str, base_port: u16) -> Option<u16> {
    for offset in 0..PORT_PROBE_COUNT {
        let port = base_port.saturating_add(offset);
        debug!(port, "probing gRPC health endpoint");
        if check_grpc_health(host, port).await {
            return Some(port);
        }
    }
    None
}

async fn check_grpc_health(host: &str, port: u16) -> bool {
    let addr = format!("http://{host}:{port}");

    let channel = match tokio::time::timeout(
        HEALTH_TIMEOUT,
        Channel::from_shared(addr).unwrap().connect(),
    )
    .await
    {
        Ok(Ok(channel)) => channel,
        Ok(Err(err)) => {
            debug!(port, error = %err, "failed to connect");
            return false;
        }
        Err(_) => {
            debug!(port, "connection timed out");
            return false;
        }
    };

    let mut client = HealthClient::new(channel);
    let request = HealthCheckRequest {
        service: HEALTH_SERVICE_NAME.to_string(),
    };

    match tokio::time::timeout(HEALTH_TIMEOUT, client.check(request)).await {
        Ok(Ok(response)) => response.into_inner().status == 1,
        Ok(Err(err)) => {
            debug!(port, error = %err, "health check failed");
            false
        }
        Err(_) => {
            debug!(port, "health check timed out");
            false
        }
    }
}

async fn spawn_server(host: &str, port: u16) -> Result<u16> {
    let grpc_addr = format!("{host}:{port}");
    let server_bin = find_server_binary()?;

    info!(?server_bin, "spawning waymark-bridge");

    let mut cmd = Command::new(&server_bin);
    cmd.env("WAYMARK_BRIDGE_GRPC_ADDR", &grpc_addr)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }

    let _child = cmd.spawn().context("failed to spawn waymark-bridge")?;

    let deadline = tokio::time::Instant::now() + STARTUP_WAIT;
    while tokio::time::Instant::now() < deadline {
        if check_grpc_health(host, port).await {
            return Ok(port);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    bail!("server failed to start within {STARTUP_WAIT:?}")
}

fn find_server_binary() -> Result<PathBuf> {
    let current_exe = env::current_exe().context("failed to get current executable path")?;
    let parent = current_exe
        .parent()
        .context("failed to get parent directory")?;

    let binary_name = if cfg!(windows) {
        "waymark-bridge.exe"
    } else {
        "waymark-bridge"
    };

    let candidate = parent.join(binary_name);
    if candidate.exists() {
        return Ok(candidate);
    }

    let mut target_dirs = Vec::new();
    if let Ok(dir) = env::var("CARGO_TARGET_DIR") {
        target_dirs.push(PathBuf::from(dir));
    }
    target_dirs.push(PathBuf::from("target"));

    let target_dirs = target_dirs
        .into_iter()
        .flat_map(|dir| [dir.join("debug"), dir.join("release")])
        .collect::<Vec<_>>();

    for dir in target_dirs {
        let candidate = dir.join(binary_name);
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    bail!("could not find waymark-bridge binary")
}

fn write_port_file(port_file: &Option<PathBuf>, port: u16) -> Result<()> {
    if let Some(path) = port_file {
        fs::write(path, port.to_string())
            .with_context(|| format!("failed to write port file: {}", path.display()))?;
        info!(path = %path.display(), port, "wrote port file");
    } else {
        println!("{port}");
    }
    Ok(())
}

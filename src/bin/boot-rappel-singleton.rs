#![allow(clippy::collapsible_if)]
//! Boot Rappel Singleton - Ensures a single server instance is running.
//!
//! This binary:
//! 1. Probes a range of ports to find an existing Rappel bridge server via gRPC health check
//! 2. If found, outputs the existing server's port
//! 3. If not found, spawns a new server and outputs its port
//!
//! Usage:
//!   boot-rappel-singleton [--port-file <path>]
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

use rappel::try_get_config;

/// Number of ports to probe
const PORT_PROBE_COUNT: u16 = 10;

/// Health check timeout
const HEALTH_TIMEOUT: Duration = Duration::from_secs(2);

/// Startup wait time for new server
const STARTUP_WAIT: Duration = Duration::from_secs(5);

/// The gRPC service name we check for health
const HEALTH_SERVICE_NAME: &str = "rappel.messages.WorkflowService";

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "boot_rappel_singleton=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Parse arguments
    let args: Vec<String> = env::args().collect();
    let port_file = parse_port_file_arg(&args);

    // Try to find existing server - use config if available, otherwise default
    let base_port = try_get_config()
        .map(|c| c.base_port)
        .unwrap_or(rappel::DEFAULT_BASE_PORT);

    info!(
        base_port,
        "probing for existing rappel server via gRPC health check"
    );

    if let Some(port) = probe_existing_server(base_port).await {
        info!(port, "found existing rappel server");
        write_port_file(&port_file, port)?;
        return Ok(());
    }

    info!("no existing server found, spawning new instance");

    // Spawn new server
    let grpc_port = spawn_server(base_port).await?;

    info!(port = grpc_port, "rappel server started");
    write_port_file(&port_file, grpc_port)?;

    Ok(())
}

fn parse_port_file_arg(args: &[String]) -> Option<PathBuf> {
    let mut iter = args.iter().peekable();
    while let Some(arg) = iter.next() {
        // Support both --port-file and --output-file for compatibility
        if arg == "--port-file" || arg == "--output-file" {
            return iter.next().map(PathBuf::from);
        }
    }
    None
}

async fn probe_existing_server(base_port: u16) -> Option<u16> {
    for offset in 0..PORT_PROBE_COUNT {
        let port = base_port + offset;

        debug!(port, "probing gRPC health endpoint");

        if check_grpc_health(port).await {
            return Some(port);
        }
    }

    None
}

async fn check_grpc_health(port: u16) -> bool {
    let addr = format!("http://127.0.0.1:{port}");

    // Try to connect with timeout
    let channel = match tokio::time::timeout(
        HEALTH_TIMEOUT,
        Channel::from_shared(addr).unwrap().connect(),
    )
    .await
    {
        Ok(Ok(channel)) => channel,
        Ok(Err(e)) => {
            debug!(port, error = %e, "failed to connect");
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
        Ok(Ok(response)) => {
            let status = response.into_inner().status;
            // ServingStatus::Serving = 1
            status == 1
        }
        Ok(Err(e)) => {
            debug!(port, error = %e, "health check failed");
            false
        }
        Err(_) => {
            debug!(port, "health check timed out");
            false
        }
    }
}

async fn spawn_server(base_port: u16) -> Result<u16> {
    let grpc_addr = format!("127.0.0.1:{base_port}");

    // Find the rappel-bridge binary
    let server_bin = find_server_binary()?;

    info!(?server_bin, "spawning rappel-bridge");

    // Spawn the server process
    let mut cmd = Command::new(&server_bin);
    cmd.env("RAPPEL_BRIDGE_GRPC_ADDR", &grpc_addr)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    // Detach from parent process group on Unix
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }

    let _child = cmd.spawn().context("failed to spawn rappel-bridge")?;

    // Wait for server to start
    let deadline = tokio::time::Instant::now() + STARTUP_WAIT;

    while tokio::time::Instant::now() < deadline {
        if check_grpc_health(base_port).await {
            return Ok(base_port);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    bail!("server failed to start within {STARTUP_WAIT:?}")
}

fn find_server_binary() -> Result<PathBuf> {
    // Try to find in same directory as this binary
    let current_exe = env::current_exe().context("failed to get current executable path")?;
    let parent = current_exe
        .parent()
        .context("failed to get parent directory")?;

    let binary_name = if cfg!(windows) {
        "rappel-bridge.exe"
    } else {
        "rappel-bridge"
    };

    let candidate = parent.join(binary_name);
    if candidate.exists() {
        return Ok(candidate);
    }

    // Try cargo target directory
    let target_dirs = ["debug", "release"];
    for dir in &target_dirs {
        let candidate = PathBuf::from(format!("target/{dir}/{binary_name}"));
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    bail!("could not find rappel-bridge binary")
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

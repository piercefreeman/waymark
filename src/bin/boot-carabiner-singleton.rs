use std::{
    io,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use carabiner::server_client;
use clap::Parser;
use reqwest::Client;
use serde::Deserialize;
use tokio::time::sleep;
use tracing::{debug, error, info};

#[derive(Parser, Debug)]
#[command(
    name = "boot-carabiner-singleton",
    about = "Ensure a single carabiner server instance is running for the host"
)]
struct Args {
    /// Starting HTTP port to probe for existing servers.
    #[arg(long, default_value_t = DEFAULT_HTTP_PORT)]
    start_port: u16,
    /// Maximum number of sequential ports to try.
    #[arg(long, default_value_t = 10)]
    max_ports: u16,
    /// Address host to check and bind against.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// Executable name to spawn for the server.
    #[arg(long, default_value = "carabiner-server")]
    server_bin: String,
    /// Seconds to wait for a spawned server to report healthy.
    #[arg(long, default_value_t = 10)]
    health_timeout_secs: u64,
}

#[derive(Debug, Deserialize)]
struct HealthPayload {
    service: String,
}

const DEFAULT_HTTP_PORT: u16 = 24117;

#[derive(Debug, PartialEq, Eq)]
enum HealthStatus {
    Matching,
    Different,
    Unavailable,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let Args {
        start_port,
        max_ports,
        host,
        server_bin,
        health_timeout_secs,
    } = Args::parse();
    let client = Client::builder().build()?;

    let mut port = start_port;
    for _ in 0..max_ports {
        match probe_health(&client, &host, port).await? {
            HealthStatus::Matching => {
                println!("{port}");
                return Ok(());
            }
            HealthStatus::Different => {
                port = port
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("port overflow"))?;
                continue;
            }
            HealthStatus::Unavailable => {
                info!(port, "booting new carabiner server");
                match spawn_server(&server_bin, &host, port) {
                    Ok(mut child) => {
                        let healthy = wait_for_health(
                            &client,
                            &host,
                            port,
                            Duration::from_secs(health_timeout_secs),
                            &mut child,
                        )
                        .await?;
                        if healthy {
                            println!("{port}");
                            return Ok(());
                        } else {
                            let _ = child.kill();
                        }
                    }
                    Err(err) => error!(?err, "failed to spawn server"),
                }
                port = port
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("port overflow"))?;
            }
        }
    }

    Err(anyhow!(
        "unable to start or locate carabiner server after {max_ports} attempts"
    ))
}

async fn probe_health(client: &Client, host: &str, port: u16) -> Result<HealthStatus> {
    let url = server_client::health_url(host, port);
    let response = client.get(&url).send().await;
    match response {
        Ok(resp) => {
            if !resp.status().is_success() {
                return Ok(HealthStatus::Different);
            }
            let payload = resp.json::<HealthPayload>().await?;
            if payload.service == server_client::SERVICE_NAME {
                Ok(HealthStatus::Matching)
            } else {
                Ok(HealthStatus::Different)
            }
        }
        Err(err) => {
            if err.is_connect() {
                Ok(HealthStatus::Unavailable)
            } else {
                Ok(HealthStatus::Different)
            }
        }
    }
}

async fn wait_for_health(
    client: &Client,
    host: &str,
    port: u16,
    timeout: Duration,
    child: &mut Child,
) -> Result<bool> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        match probe_health(client, host, port).await? {
            HealthStatus::Matching => return Ok(true),
            HealthStatus::Different => return Ok(false),
            HealthStatus::Unavailable => {}
        }

        if let Some(status) = child.try_wait()? {
            debug!(?status, "carabiner server exited before becoming healthy");
            return Ok(false);
        }
        sleep(Duration::from_millis(250)).await;
    }
    Ok(false)
}

fn spawn_server(bin: &str, host: &str, http_port: u16) -> Result<Child> {
    let grpc_port = http_port.checked_add(1).context("grpc port overflow")?;
    let mut command = Command::new(bin);
    command
        .arg("--http-addr")
        .arg(format!("{host}:{http_port}"))
        .arg("--grpc-addr")
        .arg(format!("{host}:{grpc_port}"))
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    command.spawn().map_err(|err| match err.kind() {
        io::ErrorKind::NotFound => anyhow!("{bin} not found in PATH"),
        _ => anyhow!(err),
    })
}

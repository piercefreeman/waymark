use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use crate::{
    Database, PythonWorkerConfig, PythonWorkerPool,
    db::CompletionRecord,
    server_client::{self, ServerConfig},
    server_worker::WorkerBridgeServer,
    worker::{ActionDispatchPayload, RoundTripMetrics},
};
use anyhow::{Context, Result, anyhow};
use reqwest::Client;
use tokio::{task::JoinHandle, time::sleep};
mod common;
use self::common::run_in_env;
const PARTITION_ID: i32 = 91;
const INTEGRATION_MODULE: &str = "integration_module";
const INTEGRATION_MODULE_SOURCE: &str = include_str!("fixtures/integration_module.py");
const INTEGRATION_COMPLEX_MODULE: &str = include_str!("fixtures/integration_complex.py");
const INTEGRATION_EXCEPTION_MODULE: &str = include_str!("fixtures/integration_exception.py");

const REGISTER_SCRIPT: &str = r#"
import asyncio
from integration_module import IntegrationWorkflow

async def main():
    wf = IntegrationWorkflow()
    await wf.run()

asyncio.run(main())
"#;

const REGISTER_COMPLEX_SCRIPT: &str = r#"
import asyncio
from integration_complex import ComplexWorkflow

async def main():
    wf = ComplexWorkflow()
    await wf.run()

asyncio.run(main())
"#;

const REGISTER_EXCEPTION_SCRIPT: &str = r#"
import asyncio
from integration_exception import ExceptionWorkflow

async def main():
    wf = ExceptionWorkflow()
    await wf.run()

asyncio.run(main())
"#;

struct TestServer {
    http_addr: SocketAddr,
    grpc_addr: SocketAddr,
    handle: JoinHandle<Result<()>>,
}

impl TestServer {
    async fn spawn(database_url: String) -> Result<Self> {
        let http_port = reserve_port()?;
        let grpc_port = http_port + 1;
        let http_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), http_port);
        let grpc_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), grpc_port);
        let config = ServerConfig {
            http_addr,
            grpc_addr,
            database_url,
        };
        let handle = tokio::spawn(async move { server_client::run_servers(config).await });
        Ok(Self {
            http_addr,
            grpc_addr,
            handle,
        })
    }

    async fn shutdown(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

fn reserve_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

async fn wait_for_health(http_addr: SocketAddr) -> Result<()> {
    let client = Client::new();
    let url = format!("http://{http_addr}{}", server_client::HEALTH_PATH);
    for attempt in 0..100 {
        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => {
                eprintln!("health attempt {attempt}: status {}", resp.status());
            }
            Err(err) => {
                eprintln!("health attempt {attempt} failed: {err}");
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow!("server health endpoint not responding"))
}

async fn cleanup_database(db: &Database) -> Result<()> {
    sqlx::query("TRUNCATE daemon_action_ledger, workflow_instances, workflow_versions CASCADE")
        .execute(db.pool())
        .await?;
    Ok(())
}

async fn dispatch_all_actions(
    database: &Database,
    pool: &PythonWorkerPool,
    expected_actions: usize,
) -> Result<Vec<RoundTripMetrics>> {
    let mut completed = Vec::new();
    while completed.len() < expected_actions {
        let actions = database.dispatch_actions(PARTITION_ID, 16).await?;
        if actions.is_empty() {
            sleep(Duration::from_millis(50)).await;
            continue;
        }
        let mut batch_records = Vec::new();
        let mut batch_metrics = Vec::new();
        for action in actions {
            let payload = ActionDispatchPayload {
                action_id: action.id,
                instance_id: action.instance_id,
                sequence: action.action_seq,
                payload: action.payload,
            };
            let worker = pool.next_worker();
            let metrics = worker.send_action(payload).await?;
            batch_records.push(to_completion_record(metrics.clone()));
            batch_metrics.push(metrics);
        }
        database.mark_actions_batch(&batch_records).await?;
        completed.extend(batch_metrics);
    }
    Ok(completed)
}

fn to_completion_record(metrics: RoundTripMetrics) -> CompletionRecord {
    CompletionRecord {
        action_id: metrics.action_id,
        success: metrics.success,
        delivery_id: metrics.delivery_id,
        result_payload: metrics.response_payload,
    }
}

fn parse_result(payload: &[u8]) -> Result<Option<String>> {
    let value: serde_json::Value = serde_json::from_slice(payload)?;
    let result = value
        .get("result")
        .ok_or_else(|| anyhow!("missing result payload"))?;
    decode_encoded_value(result)
}

fn decode_encoded_value(value: &serde_json::Value) -> Result<Option<String>> {
    if let Some(text) = value.as_str() {
        return Ok(Some(text.to_string()));
    }
    if let Some(primitive) = value.get("primitive") {
        return Ok(primitive
            .get("value")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()));
    }
    if let Some(basemodel) = value.get("basemodel") {
        if let Some(vars) = basemodel
            .get("data")
            .and_then(|v| v.get("variables"))
            .and_then(|v| v.as_object())
        {
            for entry in vars.values() {
                if let Some(result) = decode_encoded_value(entry)? {
                    return Ok(Some(result));
                }
            }
        }
        return Ok(None);
    }
    if let Some(exception) = value.get("exception") {
        return Ok(exception
            .get("message")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()));
    }
    Ok(None)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_executes_end_to_end() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();
    let database_url = match env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping integration test: DATABASE_URL not set");
            return Ok(());
        }
    };
    let database = Database::connect(&database_url).await?;
    cleanup_database(&database).await?;

    let server = TestServer::spawn(database_url.clone()).await?;
    wait_for_health(server.http_addr).await?;

    let files = vec![
        ("integration_module.py", INTEGRATION_MODULE_SOURCE),
        ("register.py", REGISTER_SCRIPT),
    ];
    let env_pairs = vec![
        ("CARABINER_GRPC_ADDR", server.grpc_addr.to_string()),
        ("CARABINER_SERVER_PORT", server.http_addr.port().to_string()),
        ("CARABINER_SERVER_HOST", server.http_addr.ip().to_string()),
    ];
    let python_env = run_in_env(&files, &[], &env_pairs, "register.py").await?;

    let versions = database.list_workflow_versions().await?;
    let version = versions
        .iter()
        .find(|v| v.workflow_name == "integrationworkflow")
        .context("integration workflow missing")?;
    let version_detail = database
        .load_workflow_version(version.id)
        .await?
        .context("missing workflow version detail")?;
    let expected_actions = version_detail.dag.nodes.len();

    let _instance_id = database
        .create_workflow_instance(
            PARTITION_ID,
            &version.workflow_name,
            version.id,
            Some(br#"{"input":"world"}"#),
        )
        .await?;

    let worker_server: Arc<WorkerBridgeServer> = WorkerBridgeServer::start(None).await?;
    let worker_script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("python")
        .join(".venv")
        .join("bin")
        .join("carabiner-worker");
    let worker_config = PythonWorkerConfig {
        script_path: worker_script,
        user_module: INTEGRATION_MODULE.to_string(),
        extra_python_paths: vec![python_env.path().to_path_buf()],
        ..PythonWorkerConfig::default()
    };
    let pool = PythonWorkerPool::new(worker_config, 1, Arc::clone(&worker_server)).await?;

    let completed = dispatch_all_actions(&database, &pool, expected_actions).await?;
    assert_eq!(completed.len(), expected_actions);

    pool.shutdown().await?;
    worker_server.shutdown().await;

    let message = completed
        .iter()
        .rev()
        .find_map(|metrics| parse_result(&metrics.response_payload).transpose())
        .transpose()?
        .context("expected primitive result")?;
    assert_eq!(message, "hello world");

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_executes_complex_flow() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();
    let database_url = match env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping integration test: DATABASE_URL not set");
            return Ok(());
        }
    };
    let database = Database::connect(&database_url).await?;
    cleanup_database(&database).await?;

    let server = TestServer::spawn(database_url.clone()).await?;
    wait_for_health(server.http_addr).await?;

    let files = vec![
        ("integration_complex.py", INTEGRATION_COMPLEX_MODULE),
        ("register_complex.py", REGISTER_COMPLEX_SCRIPT),
    ];
    let env_pairs = vec![
        ("CARABINER_GRPC_ADDR", server.grpc_addr.to_string()),
        ("CARABINER_SERVER_PORT", server.http_addr.port().to_string()),
        ("CARABINER_SERVER_HOST", server.http_addr.ip().to_string()),
    ];
    let python_env = run_in_env(&files, &[], &env_pairs, "register_complex.py").await?;

    let versions = database.list_workflow_versions().await?;
    let version = versions
        .iter()
        .find(|v| v.workflow_name == "complexworkflow")
        .context("complex workflow missing")?;
    let version_detail = database
        .load_workflow_version(version.id)
        .await?
        .context("missing complex workflow detail")?;
    let expected_actions = version_detail.dag.nodes.len();

    let instance_id = database
        .create_workflow_instance(
            PARTITION_ID,
            &version.workflow_name,
            version.id,
            Some(br#"{"input":"unused"}"#),
        )
        .await?;

    let worker_server: Arc<WorkerBridgeServer> = WorkerBridgeServer::start(None).await?;
    let worker_script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("python")
        .join(".venv")
        .join("bin")
        .join("carabiner-worker");
    let worker_config = PythonWorkerConfig {
        script_path: worker_script,
        user_module: "integration_complex".to_string(),
        extra_python_paths: vec![python_env.path().to_path_buf()],
        ..PythonWorkerConfig::default()
    };
    let pool = PythonWorkerPool::new(worker_config, 1, Arc::clone(&worker_server)).await?;

    let completed = dispatch_all_actions(&database, &pool, expected_actions).await?;
    assert_eq!(completed.len(), expected_actions);

    pool.shutdown().await?;
    worker_server.shutdown().await;

    let message = completed
        .iter()
        .rev()
        .find_map(|metrics| parse_result(&metrics.response_payload).transpose())
        .transpose()?
        .context("expected primitive result")?;
    assert_eq!(message, "big:3,7");

    let stored_result: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT result_payload FROM workflow_instances WHERE id = $1")
            .bind(instance_id)
            .fetch_one(database.pool())
            .await?;
    let stored_payload = stored_result.context("missing workflow result payload")?;
    let stored_message = parse_result(&stored_payload)?.context("expected primitive result")?;
    assert_eq!(stored_message, "big:3,7");

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_handles_exception_flow() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();
    let database_url = match env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping integration test: DATABASE_URL not set");
            return Ok(());
        }
    };
    let database = Database::connect(&database_url).await?;
    cleanup_database(&database).await?;

    let server = TestServer::spawn(database_url.clone()).await?;
    wait_for_health(server.http_addr).await?;

    let files = vec![
        ("integration_exception.py", INTEGRATION_EXCEPTION_MODULE),
        ("register_exception.py", REGISTER_EXCEPTION_SCRIPT),
    ];
    let env_pairs = vec![
        ("CARABINER_GRPC_ADDR", server.grpc_addr.to_string()),
        ("CARABINER_SERVER_PORT", server.http_addr.port().to_string()),
        ("CARABINER_SERVER_HOST", server.http_addr.ip().to_string()),
    ];
    let python_env = run_in_env(&files, &[], &env_pairs, "register_exception.py").await?;

    let versions = database.list_workflow_versions().await?;
    let version = versions
        .iter()
        .find(|v| v.workflow_name == "exceptionworkflow")
        .context("exception workflow missing")?;
    let version_detail = database
        .load_workflow_version(version.id)
        .await?
        .context("missing exception workflow detail")?;
    let expected_actions = version_detail.dag.nodes.len();

    let instance_id = database
        .create_workflow_instance(
            PARTITION_ID,
            &version.workflow_name,
            version.id,
            Some(br#"{"mode":"exception"}"#),
        )
        .await?;

    let worker_server: Arc<WorkerBridgeServer> = WorkerBridgeServer::start(None).await?;
    let worker_script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("python")
        .join(".venv")
        .join("bin")
        .join("carabiner-worker");
    let worker_config = PythonWorkerConfig {
        script_path: worker_script,
        user_module: "integration_exception".to_string(),
        extra_python_paths: vec![python_env.path().to_path_buf()],
        ..PythonWorkerConfig::default()
    };
    let pool = PythonWorkerPool::new(worker_config, 1, Arc::clone(&worker_server)).await?;

    let completed = dispatch_all_actions(&database, &pool, expected_actions).await?;
    assert_eq!(completed.len(), expected_actions);

    pool.shutdown().await?;
    worker_server.shutdown().await;

    let message = completed
        .iter()
        .rev()
        .find_map(|metrics| parse_result(&metrics.response_payload).transpose())
        .transpose()?
        .context("expected primitive result")?;
    assert_eq!(message, "handled:fallback");

    let stored_result: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT result_payload FROM workflow_instances WHERE id = $1")
            .bind(instance_id)
            .fetch_one(database.pool())
            .await?;
    let stored_payload = stored_result.context("missing workflow result payload")?;
    let stored_message = parse_result(&stored_payload)?.context("expected primitive result")?;
    assert_eq!(stored_message, "handled:fallback");

    server.shutdown().await;
    Ok(())
}

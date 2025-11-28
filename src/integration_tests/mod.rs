use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use std::path::PathBuf;
use std::sync::Arc;

use crate::{
    Database, PythonWorkerConfig, PythonWorkerPool,
    db::CompletionRecord,
    messages::proto,
    server_client::{self, ServerConfig},
    server_worker::WorkerBridgeServer,
    worker::{ActionDispatchPayload, RoundTripMetrics},
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use prost::Message;
use reqwest::Client;
use serial_test::serial;
use tokio::{task::JoinHandle, time::sleep};
mod common;
mod harness;
use self::harness::{WorkflowHarness, WorkflowHarnessConfig};
const INTEGRATION_MODULE: &str = "integration_module";
const INTEGRATION_MODULE_SOURCE: &str = include_str!("fixtures/integration_module.py");
const INTEGRATION_COMPLEX_MODULE: &str = include_str!("fixtures/integration_complex.py");
const INTEGRATION_LOOP_MODULE: &str = "integration_loop";
const INTEGRATION_LOOP_MODULE_SOURCE: &str = include_str!("fixtures/integration_loop.py");
const INTEGRATION_LOOP_ACCUM_MODULE: &str = "integration_loop_accum";
const INTEGRATION_LOOP_ACCUM_MODULE_SOURCE: &str =
    include_str!("fixtures/integration_loop_accum.py");
const INTEGRATION_EXCEPTION_MODULE: &str = include_str!("fixtures/integration_exception.py");
const INTEGRATION_EXCEPTION_CUSTOM_MODULE: &str =
    include_str!("fixtures/integration_exception_custom.py");
const INTEGRATION_EXCEPTION_WITH_SUCCESS_MODULE: &str =
    include_str!("fixtures/integration_exception_with_success.py");
const INTEGRATION_CRASH_RECOVERY_MODULE: &str = "integration_crash_recovery";
const INTEGRATION_CRASH_RECOVERY_MODULE_SOURCE: &str =
    include_str!("fixtures/integration_crash_recovery.py");

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

const REGISTER_LOOP_SCRIPT: &str = r#"
import asyncio
from integration_loop import LoopWorkflow

async def main():
    wf = LoopWorkflow()
    await wf.run()

asyncio.run(main())
"#;

const REGISTER_LOOP_ACCUM_SCRIPT: &str = r#"
import asyncio
from integration_loop_accum import LoopAccumWorkflow

async def main():
    wf = LoopAccumWorkflow()
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

const REGISTER_EXCEPTION_CUSTOM_SCRIPT: &str = r#"
import asyncio
from integration_exception_custom import ExceptionCustomWorkflow

async def main():
    wf = ExceptionCustomWorkflow()
    await wf.run()

asyncio.run(main())
"#;

const REGISTER_EXCEPTION_WITH_SUCCESS_SCRIPT: &str = r#"
import asyncio
from integration_exception_with_success import ExceptionWithSuccessWorkflow

async def main():
    wf = ExceptionWithSuccessWorkflow()
    await wf.run(should_fail=True)

asyncio.run(main())
"#;

const REGISTER_CRASH_RECOVERY_SCRIPT: &str = r#"
import asyncio
from integration_crash_recovery import CrashRecoveryWorkflow

async def main():
    wf = CrashRecoveryWorkflow()
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

async fn purge_empty_input_instances(db: &Database) -> Result<()> {
    // In integration tests, just clear all instances and actions to start fresh
    sqlx::query("DELETE FROM daemon_action_ledger")
        .execute(db.pool())
        .await?;
    sqlx::query("DELETE FROM workflow_instances")
        .execute(db.pool())
        .await?;
    Ok(())
}

async fn dispatch_all_actions(
    database: &Database,
    pool: &PythonWorkerPool,
    target_actions: usize,
) -> Result<Vec<RoundTripMetrics>> {
    let mut completed = Vec::new();
    let mut max_iterations = target_actions.saturating_mul(20).max(100); // Safety limit to prevent infinite loops
    let mut idle_cycles = 0usize;
    while max_iterations > 0 {
        max_iterations -= 1;
        let actions = database.dispatch_actions(16).await?;
        if actions.is_empty() {
            idle_cycles = idle_cycles.saturating_add(1);
            if idle_cycles >= 3 && completed.len() >= target_actions {
                break;
            }
            sleep(Duration::from_millis(50)).await;
            continue;
        }
        idle_cycles = 0;
        let mut batch_records = Vec::new();
        let mut batch_metrics = Vec::new();
        for action in actions {
            let dispatch = proto::WorkflowNodeDispatch::decode(action.dispatch_payload.as_slice())
                .context("failed to decode workflow dispatch")?;
            let payload = ActionDispatchPayload {
                action_id: action.id,
                instance_id: action.instance_id,
                sequence: action.action_seq,
                dispatch,
                timeout_seconds: action.timeout_seconds,
                max_retries: action.max_retries,
                attempt_number: action.attempt_number,
                dispatch_token: action.delivery_token,
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
        dispatch_token: metrics.dispatch_token,
        control: metrics.control,
    }
}

/// Dispatch actions up to a limit, then stop. Returns the completed actions and any
/// actions that were dispatched but not completed (simulating in-flight work when crash happens).
/// This is used to simulate partial workflow execution before a "crash".
async fn dispatch_n_actions(
    database: &Database,
    pool: &PythonWorkerPool,
    complete_limit: usize,
) -> Result<Vec<RoundTripMetrics>> {
    let mut completed = Vec::new();
    let mut max_iterations = complete_limit.saturating_mul(20).max(50);
    while completed.len() < complete_limit && max_iterations > 0 {
        max_iterations -= 1;
        let actions = database.dispatch_actions(1).await?;
        if actions.is_empty() {
            sleep(Duration::from_millis(50)).await;
            continue;
        }
        for action in actions {
            if completed.len() >= complete_limit {
                // Don't process this action - it will remain in 'dispatched' state
                // simulating an action that was dispatched right before crash
                break;
            }
            let dispatch = proto::WorkflowNodeDispatch::decode(action.dispatch_payload.as_slice())
                .context("failed to decode workflow dispatch")?;
            let payload = ActionDispatchPayload {
                action_id: action.id,
                instance_id: action.instance_id,
                sequence: action.action_seq,
                dispatch,
                timeout_seconds: action.timeout_seconds,
                max_retries: action.max_retries,
                attempt_number: action.attempt_number,
                dispatch_token: action.delivery_token,
            };
            let worker = pool.next_worker();
            let metrics = worker.send_action(payload).await?;
            let record = to_completion_record(metrics.clone());
            database.mark_actions_batch(&[record]).await?;
            completed.push(metrics);
        }
    }
    Ok(completed)
}

/// Input value variants for workflow tests
#[derive(Clone)]
pub enum TestInputValue {
    String(&'static str),
    Bool(bool),
}

fn encode_workflow_input(pairs: &[(&str, &str)]) -> Vec<u8> {
    let mut arguments = proto::WorkflowArguments {
        arguments: Vec::new(),
    };
    for (key, value) in pairs {
        arguments.arguments.push(proto::WorkflowArgument {
            key: (*key).to_string(),
            value: Some(proto::WorkflowArgumentValue {
                kind: Some(proto::workflow_argument_value::Kind::Primitive(
                    proto::PrimitiveWorkflowArgument {
                        kind: Some(proto::primitive_workflow_argument::Kind::StringValue(
                            (*value).to_string(),
                        )),
                    },
                )),
            }),
        });
    }
    arguments.encode_to_vec()
}

fn encode_workflow_input_typed(pairs: &[(&str, TestInputValue)]) -> Vec<u8> {
    let mut arguments = proto::WorkflowArguments {
        arguments: Vec::new(),
    };
    for (key, value) in pairs {
        let primitive_kind = match value {
            TestInputValue::String(s) => {
                proto::primitive_workflow_argument::Kind::StringValue(s.to_string())
            }
            TestInputValue::Bool(b) => proto::primitive_workflow_argument::Kind::BoolValue(*b),
        };
        arguments.arguments.push(proto::WorkflowArgument {
            key: (*key).to_string(),
            value: Some(proto::WorkflowArgumentValue {
                kind: Some(proto::workflow_argument_value::Kind::Primitive(
                    proto::PrimitiveWorkflowArgument {
                        kind: Some(primitive_kind),
                    },
                )),
            }),
        });
    }
    arguments.encode_to_vec()
}

fn parse_result(payload: &[u8]) -> Result<Option<String>> {
    if payload.is_empty() {
        return Ok(None);
    }
    let arguments = proto::WorkflowArguments::decode(payload)
        .map_err(|err| anyhow!("decode workflow arguments: {err}"))?;
    for argument in arguments.arguments {
        if argument.key == "result"
            && let Some(value) = argument.value.as_ref()
        {
            return decode_argument_value(value);
        }
        if argument.key == "error"
            && let Some(value) = argument.value.as_ref()
        {
            return Ok(extract_string_from_value(value));
        }
    }
    Err(anyhow!("missing result payload"))
}

fn decode_argument_value(value: &proto::WorkflowArgumentValue) -> Result<Option<String>> {
    use proto::workflow_argument_value::Kind;
    match value.kind.as_ref() {
        Some(Kind::Primitive(primitive)) => Ok(primitive_value_to_string(primitive)),
        Some(Kind::Basemodel(model)) => {
            if let Some(dict_data) = model.data.as_ref() {
                // Look for "variables" key in the dict
                if let Some(variables_entry) =
                    dict_data.entries.iter().find(|e| e.key == "variables")
                    && let Some(variables_value) = &variables_entry.value
                {
                    // Recursively decode the variables value
                    if let Some(result) = decode_argument_value(variables_value)? {
                        return Ok(Some(result));
                    }
                }
                // Also check other entries
                for entry in &dict_data.entries {
                    if let Some(entry_value) = &entry.value
                        && let Some(result) = decode_argument_value(entry_value)?
                    {
                        return Ok(Some(result));
                    }
                }
            }
            Ok(None)
        }
        Some(Kind::Exception(err)) => Ok(Some(err.message.clone())),
        Some(Kind::ListValue(list)) => {
            for entry in &list.items {
                if let Some(result) = decode_argument_value(entry)? {
                    return Ok(Some(result));
                }
            }
            Ok(None)
        }
        Some(Kind::TupleValue(list)) => {
            for entry in &list.items {
                if let Some(result) = decode_argument_value(entry)? {
                    return Ok(Some(result));
                }
            }
            Ok(None)
        }
        Some(Kind::DictValue(dict)) => {
            for entry in &dict.entries {
                if let Some(value) = entry.value.as_ref()
                    && let Some(result) = decode_argument_value(value)?
                {
                    return Ok(Some(result));
                }
            }
            Ok(None)
        }
        None => Ok(None),
    }
}

fn extract_string_from_value(value: &proto::WorkflowArgumentValue) -> Option<String> {
    decode_argument_value(value).ok().flatten()
}

fn primitive_value_to_string(value: &proto::PrimitiveWorkflowArgument) -> Option<String> {
    use proto::primitive_workflow_argument::Kind;
    match value.kind.as_ref()? {
        Kind::StringValue(text) => Some(text.clone()),
        Kind::DoubleValue(number) => Some(number.to_string()),
        Kind::IntValue(number) => Some(number.to_string()),
        Kind::BoolValue(flag) => Some(flag.to_string()),
        Kind::NullValue(_) => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn workflow_executes_end_to_end() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();
    let Some(harness) = WorkflowHarness::new(WorkflowHarnessConfig {
        files: &[
            ("integration_module.py", INTEGRATION_MODULE_SOURCE),
            ("register.py", REGISTER_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "integrationworkflow",
        user_module: INTEGRATION_MODULE,
        inputs: &[("input", "world")],
    })
    .await?
    else {
        return Ok(());
    };

    let completed = harness.dispatch_all().await?;
    assert!(
        completed.len() >= harness.expected_actions(),
        "expected at least {} completions, saw {}",
        harness.expected_actions(),
        completed.len()
    );

    let stored_payload = harness
        .stored_result()
        .await?
        .context("missing workflow result payload")?;
    let message = parse_result(&stored_payload)?.context("expected primitive result")?;
    assert_eq!(message, "hello world");

    harness.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn workflow_executes_complex_flow() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();
    let Some(harness) = WorkflowHarness::new(WorkflowHarnessConfig {
        files: &[
            ("integration_complex.py", INTEGRATION_COMPLEX_MODULE),
            ("register_complex.py", REGISTER_COMPLEX_SCRIPT),
        ],
        entrypoint: "register_complex.py",
        workflow_name: "complexworkflow",
        user_module: "integration_complex",
        inputs: &[("input", "unused")],
    })
    .await?
    else {
        return Ok(());
    };

    let completed = harness.dispatch_all().await?;
    assert!(
        completed.len() >= harness.expected_actions(),
        "expected at least {} completions, saw {}",
        harness.expected_actions(),
        completed.len()
    );

    let stored_payload = harness
        .stored_result()
        .await?
        .context("missing workflow result payload")?;
    let stored_message = parse_result(&stored_payload)?.context("expected primitive result")?;
    assert_eq!(stored_message, "big:3,7");

    harness.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn workflow_executes_looped_actions() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();
    let Some(harness) = WorkflowHarness::new(WorkflowHarnessConfig {
        files: &[
            ("integration_loop.py", INTEGRATION_LOOP_MODULE_SOURCE),
            ("register_loop.py", REGISTER_LOOP_SCRIPT),
        ],
        entrypoint: "register_loop.py",
        workflow_name: "loopworkflow",
        user_module: INTEGRATION_LOOP_MODULE,
        inputs: &[("input", "unused")],
    })
    .await?
    else {
        return Ok(());
    };

    let completed = harness.dispatch_all().await?;
    assert!(
        completed.len() >= harness.expected_actions(),
        "expected at least {} completions, saw {}",
        harness.expected_actions(),
        completed.len()
    );

    let stored_payload = harness
        .stored_result()
        .await?
        .context("missing workflow result payload")?;
    let parsed_result =
        parse_result(&stored_payload)?.context("expected primitive workflow result")?;
    assert_eq!(parsed_result, "alpha-local-decorated,beta-local-decorated");

    harness.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn workflow_accumulates_loop_outputs() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();
    let Some(harness) = WorkflowHarness::new(WorkflowHarnessConfig {
        files: &[
            (
                "integration_loop_accum.py",
                INTEGRATION_LOOP_ACCUM_MODULE_SOURCE,
            ),
            ("register_loop_accum.py", REGISTER_LOOP_ACCUM_SCRIPT),
        ],
        entrypoint: "register_loop_accum.py",
        workflow_name: "loopaccumworkflow",
        user_module: INTEGRATION_LOOP_ACCUM_MODULE,
        inputs: &[("input", "unused")],
    })
    .await?
    else {
        return Ok(());
    };

    let completed = harness.dispatch_all().await?;
    assert!(
        completed.len() >= harness.expected_actions(),
        "expected at least {} completions, saw {}",
        harness.expected_actions(),
        completed.len()
    );

    let stored_payload = harness
        .stored_result()
        .await?
        .context("missing workflow result payload")?;
    let parsed_result =
        parse_result(&stored_payload)?.context("expected primitive workflow result")?;
    assert_eq!(
        parsed_result,
        "alpha-local-0-decorated,beta-local-1-decorated"
    );

    harness.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn workflow_handles_exception_flow() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();
    let Some(harness) = WorkflowHarness::new(WorkflowHarnessConfig {
        files: &[
            ("integration_exception.py", INTEGRATION_EXCEPTION_MODULE),
            ("register_exception.py", REGISTER_EXCEPTION_SCRIPT),
        ],
        entrypoint: "register_exception.py",
        workflow_name: "exceptionworkflow",
        user_module: "integration_exception",
        inputs: &[("mode", "exception")],
    })
    .await?
    else {
        return Ok(());
    };

    let completed = harness.dispatch_all().await?;
    assert_eq!(completed.len(), harness.expected_actions());

    let cleanup_node = harness
        .version_detail()
        .dag
        .nodes
        .iter()
        .find(|node| node.action == "cleanup")
        .context("cleanup node missing")?;
    let (cleanup_status, cleanup_success, cleanup_result): (String, bool, Option<Vec<u8>>) =
        sqlx::query_as(
            "SELECT status, success, result_payload FROM daemon_action_ledger WHERE instance_id = $1 AND workflow_node_id = $2",
        )
        .bind(harness.instance_id())
        .bind(&cleanup_node.id)
        .fetch_one(harness.database().pool())
        .await?;
    assert_eq!(cleanup_status, "completed");
    assert!(
        cleanup_success,
        "cleanup action did not succeed despite exception handling"
    );
    let cleanup_payload = cleanup_result.context("cleanup result payload missing")?;
    assert!(!cleanup_payload.is_empty(), "cleanup payload missing bytes");

    let stored_payload = harness
        .stored_result()
        .await?
        .context("missing workflow result payload")?;
    assert!(
        !stored_payload.is_empty(),
        "workflow result payload missing bytes"
    );

    harness.shutdown().await?;
    Ok(())
}

/// Test that workflows can catch custom exception types (not just built-in exceptions).
/// This reproduces the issue where `except CustomError:` fails with "dependency node_1 failed"
/// because the exception module from the actual exception doesn't match what the DAG expected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn workflow_handles_custom_exception_type() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();
    let Some(harness) = WorkflowHarness::new(WorkflowHarnessConfig {
        files: &[
            (
                "integration_exception_custom.py",
                INTEGRATION_EXCEPTION_CUSTOM_MODULE,
            ),
            (
                "register_exception_custom.py",
                REGISTER_EXCEPTION_CUSTOM_SCRIPT,
            ),
        ],
        entrypoint: "register_exception_custom.py",
        workflow_name: "exceptioncustomworkflow",
        user_module: "integration_exception_custom",
        inputs: &[("mode", "exception")],
    })
    .await?
    else {
        return Ok(());
    };

    let completed = harness.dispatch_all().await?;
    assert_eq!(
        completed.len(),
        harness.expected_actions(),
        "expected {} completions, saw {}",
        harness.expected_actions(),
        completed.len()
    );

    // Verify the cleanup action ran successfully (proof that exception was caught)
    let cleanup_node = harness
        .version_detail()
        .dag
        .nodes
        .iter()
        .find(|node| node.action == "cleanup")
        .context("cleanup node missing")?;
    let (cleanup_status, cleanup_success, cleanup_result): (String, bool, Option<Vec<u8>>) =
        sqlx::query_as(
            "SELECT status, success, result_payload FROM daemon_action_ledger WHERE instance_id = $1 AND workflow_node_id = $2",
        )
        .bind(harness.instance_id())
        .bind(&cleanup_node.id)
        .fetch_one(harness.database().pool())
        .await?;
    assert_eq!(cleanup_status, "completed");
    assert!(
        cleanup_success,
        "cleanup action did not succeed - custom exception was not caught properly"
    );
    let cleanup_payload = cleanup_result.context("cleanup result payload missing")?;
    assert!(!cleanup_payload.is_empty(), "cleanup payload missing bytes");

    // Verify final workflow result
    let stored_payload = harness
        .stored_result()
        .await?
        .context("missing workflow result payload")?;
    assert!(
        !stored_payload.is_empty(),
        "workflow result payload missing bytes"
    );

    harness.shutdown().await?;
    Ok(())
}

/// Test workflow with both success and failure paths (mirroring example app's ErrorHandlingWorkflow).
/// This tests the case where the workflow has conditional branches after catching an exception.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn workflow_handles_exception_with_success_branch() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();
    let Some(harness) = WorkflowHarness::new(WorkflowHarnessConfig {
        files: &[
            (
                "integration_exception_with_success.py",
                INTEGRATION_EXCEPTION_WITH_SUCCESS_MODULE,
            ),
            (
                "register_exception_with_success.py",
                REGISTER_EXCEPTION_WITH_SUCCESS_SCRIPT,
            ),
        ],
        entrypoint: "register_exception_with_success.py",
        workflow_name: "exceptionwithsuccessworkflow",
        user_module: "integration_exception_with_success",
        inputs: &[("should_fail", "true")],
    })
    .await?
    else {
        return Ok(());
    };

    let completed = harness.dispatch_all().await?;
    // We expect:
    // - node_0: recovered = False (python_block)
    // - node_1: message = "" (python_block)
    // - node_2: risky_action (fails)
    // - node_3: recovery_action (runs due to exception)
    // - node_4: recovered_msg variable assignment
    // - node_5: recovered = True
    // - node_6: message = recovered_msg
    // - node_7: return
    // Note: success_action should NOT run because risky_action failed
    eprintln!(
        "completed {} actions (expected {})",
        completed.len(),
        harness.expected_actions()
    );

    // Verify the recovery action ran successfully
    let recovery_node = harness
        .version_detail()
        .dag
        .nodes
        .iter()
        .find(|node| node.action == "recovery_action")
        .context("recovery_action node missing")?;
    let (recovery_status, recovery_success, recovery_result): (String, bool, Option<Vec<u8>>) =
        sqlx::query_as(
            "SELECT status, success, result_payload FROM daemon_action_ledger WHERE instance_id = $1 AND workflow_node_id = $2",
        )
        .bind(harness.instance_id())
        .bind(&recovery_node.id)
        .fetch_one(harness.database().pool())
        .await?;
    assert_eq!(recovery_status, "completed");
    assert!(
        recovery_success,
        "recovery_action did not succeed - custom exception was not caught properly"
    );
    let recovery_payload = recovery_result.context("recovery result payload missing")?;
    assert!(
        !recovery_payload.is_empty(),
        "recovery payload missing bytes"
    );

    // Verify final workflow result
    let stored_payload = harness
        .stored_result()
        .await?
        .context("missing workflow result payload")?;
    assert!(
        !stored_payload.is_empty(),
        "workflow result payload missing bytes"
    );

    harness.shutdown().await?;
    Ok(())
}

/// Test workflow success path - when should_fail=false, exception path should not run.
/// This is the inverse of workflow_handles_exception_with_success_branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn workflow_success_path_skips_exception_handler() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();
    let Some(harness) = WorkflowHarness::new_typed(harness::WorkflowHarnessConfigTyped {
        files: &[
            (
                "integration_exception_with_success.py",
                INTEGRATION_EXCEPTION_WITH_SUCCESS_MODULE,
            ),
            (
                "register_exception_with_success.py",
                REGISTER_EXCEPTION_WITH_SUCCESS_SCRIPT,
            ),
        ],
        entrypoint: "register_exception_with_success.py",
        workflow_name: "exceptionwithsuccessworkflow",
        user_module: "integration_exception_with_success",
        inputs: &[("should_fail", TestInputValue::Bool(false))],  // Success path!
    })
    .await?
    else {
        return Ok(());
    };

    let completed = harness.dispatch_all().await?;
    // We expect:
    // - node_0: recovered = False (python_block)
    // - node_1: message = "" (python_block)
    // - node_2: risky_action (succeeds)
    // - node_3: success_action (runs because risky_action succeeded)
    // - node_4: recovery_action (skipped - guard evaluates false)
    // - node_5: recovered = True (skipped - guard evaluates false)
    // - node_6: message = recovered_msg (skipped - guard evaluates false)
    // - node_7: return
    eprintln!(
        "completed {} actions (expected {})",
        completed.len(),
        harness.expected_actions()
    );

    // Verify the success_action ran (not the recovery path)
    let success_node = harness
        .version_detail()
        .dag
        .nodes
        .iter()
        .find(|node| node.action == "success_action")
        .context("success_action node missing")?;
    let (success_status, success_ok, success_result): (String, bool, Option<Vec<u8>>) =
        sqlx::query_as(
            "SELECT status, success, result_payload FROM daemon_action_ledger WHERE instance_id = $1 AND workflow_node_id = $2",
        )
        .bind(harness.instance_id())
        .bind(&success_node.id)
        .fetch_one(harness.database().pool())
        .await?;
    assert_eq!(success_status, "completed");
    assert!(
        success_ok,
        "success_action did not succeed - it should run in success path"
    );
    let success_payload = success_result.context("success result payload missing")?;
    assert!(
        !success_payload.is_empty(),
        "success payload missing bytes"
    );

    // Verify final workflow result
    let stored_payload = harness
        .stored_result()
        .await?
        .context("missing workflow result payload")?;
    assert!(
        !stored_payload.is_empty(),
        "workflow result payload missing bytes"
    );

    harness.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn stale_worker_completion_is_ignored() -> Result<()> {
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
    let dispatch = proto::WorkflowNodeDispatch {
        node: None,
        workflow_input: None,
        context: Vec::new(),
        resolved_kwargs: None,
    };
    let payload = dispatch.encode_to_vec();
    database
        .seed_actions(1, "tests", "action", &payload)
        .await?;
    let mut actions = database.dispatch_actions(1).await?;
    let mut action = actions.pop().expect("dispatched action");
    let stale_token = action.delivery_token;
    database.requeue_action(action.id).await?;
    let mut redispatched = database.dispatch_actions(1).await?;
    action = redispatched.pop().expect("redispatched action");
    let fresh_token = action.delivery_token;

    let stale_record = CompletionRecord {
        action_id: action.id,
        success: true,
        delivery_id: 1,
        result_payload: Vec::new(),
        dispatch_token: Some(stale_token),
        control: None,
    };
    database.mark_actions_batch(&[stale_record]).await?;
    let (status, payload): (String, Option<Vec<u8>>) =
        sqlx::query_as("SELECT status, result_payload FROM daemon_action_ledger WHERE id = $1")
            .bind(action.id)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(status, "dispatched");
    assert!(payload.is_none());

    let mut result_args = proto::WorkflowArguments {
        arguments: Vec::new(),
    };
    result_args.arguments.push(proto::WorkflowArgument {
        key: "result".to_string(),
        value: Some(proto::WorkflowArgumentValue {
            kind: Some(proto::workflow_argument_value::Kind::Primitive(
                proto::PrimitiveWorkflowArgument {
                    kind: Some(proto::primitive_workflow_argument::Kind::StringValue(
                        "ok".to_string(),
                    )),
                },
            )),
        }),
    });
    let valid_record = CompletionRecord {
        action_id: action.id,
        success: true,
        delivery_id: 2,
        result_payload: result_args.encode_to_vec(),
        dispatch_token: Some(fresh_token),
        control: None,
    };
    database.mark_actions_batch(&[valid_record]).await?;
    let (status, payload): (String, Option<Vec<u8>>) =
        sqlx::query_as("SELECT status, result_payload FROM daemon_action_ledger WHERE id = $1")
            .bind(action.id)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(status, "completed");
    assert!(payload.is_some());
    Ok(())
}

/// Test that simulates a cluster crash mid-workflow and verifies recovery via timeout.
///
/// Scenario:
/// 1. Start a 4-action sequential workflow (step1 -> step2 -> step3 -> step4)
///    Each action has a 2-second timeout configured in the workflow definition.
/// 2. Complete the first 2 actions successfully
/// 3. Dispatch action 3 but simulate crash before completion (action stays in 'dispatched')
/// 4. Shut down workers (simulating cluster death)
/// 5. Wait for the action's deadline to pass (2+ seconds)
/// 6. Run timeout checker to detect and requeue the stale action
/// 7. Spin up new workers
/// 8. Complete remaining actions
/// 9. Verify final workflow result is correct
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn workflow_recovers_after_crash() -> Result<()> {
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

    // Start test server
    let server = TestServer::spawn(database_url.clone()).await?;
    wait_for_health(server.http_addr).await?;

    // Register workflow via Python
    let env_pairs = vec![
        ("CARABINER_GRPC_ADDR", server.grpc_addr.to_string()),
        ("CARABINER_SERVER_PORT", server.http_addr.port().to_string()),
        ("CARABINER_SERVER_HOST", server.http_addr.ip().to_string()),
        ("CARABINER_SKIP_WAIT_FOR_INSTANCE", "1".to_string()),
    ];
    let python_env = common::run_in_env(
        &[
            (
                "integration_crash_recovery.py",
                INTEGRATION_CRASH_RECOVERY_MODULE_SOURCE,
            ),
            ("register_crash_recovery.py", REGISTER_CRASH_RECOVERY_SCRIPT),
        ],
        &[],
        &env_pairs,
        "register_crash_recovery.py",
    )
    .await?;
    purge_empty_input_instances(&database).await?;

    // Find the registered workflow version
    let versions = database.list_workflow_versions().await?;
    let version = versions
        .iter()
        .find(|v| v.workflow_name == "crashrecoveryworkflow")
        .context("crashrecoveryworkflow missing")?;
    let version_detail = database
        .load_workflow_version(version.id)
        .await?
        .context("missing workflow version detail")?;
    let total_actions = version_detail.dag.nodes.len();
    assert_eq!(
        total_actions, 4,
        "expected 4 actions in crash recovery workflow"
    );

    // Create workflow instance (no input needed - workflow uses hardcoded "start")
    let instance_id = database
        .create_workflow_instance(&version.workflow_name, version.id, None)
        .await?;

    // === PHASE 1: Start workers and complete first 2 actions ===
    let worker_server_1: Arc<WorkerBridgeServer> = WorkerBridgeServer::start(None).await?;
    let worker_script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("python")
        .join(".venv")
        .join("bin")
        .join("rappel-worker");
    let worker_config_1 = PythonWorkerConfig {
        script_path: worker_script.clone(),
        script_args: Vec::new(),
        user_module: INTEGRATION_CRASH_RECOVERY_MODULE.to_string(),
        extra_python_paths: vec![python_env.path().to_path_buf()],
    };
    let pool_1 = PythonWorkerPool::new(worker_config_1, 1, Arc::clone(&worker_server_1)).await?;

    // Complete 2 actions, then dispatch one more but don't complete it
    let completed_before_crash = dispatch_n_actions(&database, &pool_1, 2).await?;
    assert_eq!(
        completed_before_crash.len(),
        2,
        "should have completed 2 actions before simulated crash"
    );

    // Dispatch the 3rd action but simulate crash (don't complete it)
    let in_flight_actions = database.dispatch_actions(1).await?;
    assert_eq!(
        in_flight_actions.len(),
        1,
        "should have 1 action dispatched but not completed"
    );
    let in_flight_action = &in_flight_actions[0];

    // Verify action is in 'dispatched' state with a deadline set
    let (dispatched_status, deadline): (String, Option<DateTime<Utc>>) =
        sqlx::query_as("SELECT status, deadline_at FROM daemon_action_ledger WHERE id = $1")
            .bind(in_flight_action.id)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(
        dispatched_status, "dispatched",
        "action should be in dispatched state"
    );
    assert!(
        deadline.is_some(),
        "dispatched action should have a deadline set"
    );

    // === PHASE 2: Simulate crash ===
    // Shut down the worker pool (workers die without completing their work)
    pool_1.shutdown().await?;
    worker_server_1.shutdown().await;

    // Wait for the action's deadline to pass (timeout is 2 seconds, wait a bit longer)
    // The workflow configures 2-second timeouts, so we wait 3 seconds to be safe
    eprintln!("waiting for action deadline to pass (3 seconds)...");
    sleep(Duration::from_secs(3)).await;

    // Run timeout checker to requeue the stale action - this simulates what the
    // polling dispatcher does in production when it detects stale actions
    let timed_out = database.mark_timed_out_actions(100).await?;
    assert_eq!(timed_out, 1, "should have found 1 timed out action");

    // Verify action is now back in 'queued' state with incremented attempt_number
    let (status, attempt): (String, i32) =
        sqlx::query_as("SELECT status, attempt_number FROM daemon_action_ledger WHERE id = $1")
            .bind(in_flight_action.id)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(status, "queued", "action should be requeued after timeout");
    assert_eq!(
        attempt, 1,
        "attempt_number should be incremented after timeout retry"
    );

    // === PHASE 3: Start fresh workers and complete remaining actions ===
    let worker_server_2: Arc<WorkerBridgeServer> = WorkerBridgeServer::start(None).await?;
    let worker_config_2 = PythonWorkerConfig {
        script_path: worker_script,
        script_args: Vec::new(),
        user_module: INTEGRATION_CRASH_RECOVERY_MODULE.to_string(),
        extra_python_paths: vec![python_env.path().to_path_buf()],
    };
    let pool_2 = PythonWorkerPool::new(worker_config_2, 1, Arc::clone(&worker_server_2)).await?;

    // Complete remaining actions (should be 2: the retried action + the final action)
    let completed_after_recovery = dispatch_all_actions(&database, &pool_2, 2).await?;
    assert!(
        completed_after_recovery.len() >= 2,
        "should have completed at least 2 more actions after recovery, got {}",
        completed_after_recovery.len()
    );

    // === PHASE 4: Verify final result ===
    let stored_payload: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT result_payload FROM workflow_instances WHERE id = $1")
            .bind(instance_id)
            .fetch_one(database.pool())
            .await?;
    let payload = stored_payload.context("missing workflow result payload")?;
    let result = parse_result(&payload)?.context("expected primitive result")?;
    assert_eq!(
        result, "step4(step3(step2(step1(start))))",
        "workflow should complete with correct final result after crash recovery"
    );

    // Verify all actions show completed
    let completed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM daemon_action_ledger WHERE instance_id = $1 AND status = 'completed'",
    )
    .bind(instance_id)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(completed_count, 4, "all 4 actions should be completed");

    // Cleanup
    pool_2.shutdown().await?;
    worker_server_2.shutdown().await;
    server.shutdown().await;
    drop(python_env);

    Ok(())
}

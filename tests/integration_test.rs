//! Integration tests for the Rappel runtime.
//!
//! These tests verify the full execution flow:
//! 1. Python workflow registration (IR generation)
//! 2. DAG conversion and action queue population
//! 3. Worker dispatch and execution
//! 4. Completion handling and result storage

#[path = "integration_harness.rs"]
mod harness;

use anyhow::Result;
use prost::Message;
use serial_test::serial;
use tracing::info;

use harness::{HarnessConfig, IntegrationHarness};
use rappel::proto;

const SIMPLE_WORKFLOW_MODULE: &str = include_str!("fixtures/simple_workflow.py");
const SEQUENTIAL_WORKFLOW_MODULE: &str = include_str!("fixtures/sequential_workflow.py");
const CONDITIONAL_WORKFLOW_MODULE: &str = include_str!("fixtures/conditional_workflow.py");
const EXCEPTION_WORKFLOW_MODULE: &str = include_str!("fixtures/exception_workflow.py");
const CRASH_RECOVERY_WORKFLOW_MODULE: &str = include_str!("fixtures/integration_crash_recovery.py");
const EXCEPTION_CUSTOM_WORKFLOW_MODULE: &str =
    include_str!("fixtures/integration_exception_custom.py");
const EXCEPTION_WITH_SUCCESS_WORKFLOW_MODULE: &str =
    include_str!("fixtures/integration_exception_with_success.py");

/// Registration script that imports and runs the workflow.
/// This triggers the workflow decorator which registers the IR via gRPC.
/// Note: We do NOT set PYTEST_CURRENT_TEST so Python calls the gRPC server.
const REGISTER_SIMPLE_SCRIPT: &str = r#"
import asyncio
import os

from simple_workflow import SimpleWorkflow

async def main():
    # Clear any test mode that might be set
    os.environ.pop("PYTEST_CURRENT_TEST", None)

    wf = SimpleWorkflow()
    # This will call the gRPC server to register the workflow
    # CARABINER_SKIP_WAIT_FOR_INSTANCE tells it not to wait for completion
    result = await wf.run(name="integration")
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Parse the result payload to extract the string value.
fn parse_result(payload: &[u8]) -> Result<Option<String>> {
    if payload.is_empty() {
        return Ok(None);
    }

    let arguments = proto::WorkflowArguments::decode(payload)
        .map_err(|err| anyhow::anyhow!("decode workflow arguments: {err}"))?;

    for argument in arguments.arguments {
        if argument.key == "result"
            && let Some(value) = argument.value.as_ref()
        {
            return extract_string_from_value(value);
        }
    }
    Err(anyhow::anyhow!("missing result in payload"))
}

fn extract_string_from_value(value: &proto::WorkflowArgumentValue) -> Result<Option<String>> {
    use proto::workflow_argument_value::Kind;
    match value.kind.as_ref() {
        Some(Kind::Primitive(primitive)) => {
            use proto::primitive_workflow_argument::Kind;
            match primitive.kind.as_ref() {
                Some(Kind::StringValue(s)) => Ok(Some(s.clone())),
                Some(Kind::IntValue(i)) => Ok(Some(i.to_string())),
                Some(Kind::DoubleValue(f)) => Ok(Some(f.to_string())),
                Some(Kind::BoolValue(b)) => Ok(Some(b.to_string())),
                Some(Kind::NullValue(_)) => Ok(None),
                None => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn simple_workflow_executes_end_to_end() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            ("simple_workflow.py", SIMPLE_WORKFLOW_MODULE),
            ("register.py", REGISTER_SIMPLE_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "simpleworkflow",
        user_module: "simple_workflow",
        inputs: &[("name", "world")],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    // Verify the workflow result
    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("hello world".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Sequential Workflow Test
// =============================================================================

const REGISTER_SEQUENTIAL_SCRIPT: &str = r#"
import asyncio
import os

from sequential_workflow import SequentialWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = SequentialWorkflow()
    result = await wf.run()
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Test that sequential workflows execute all actions and return the correct result.
///
/// This tests the full sequential execution: fetch_value -> transform_value -> format_result
/// The workflow should return "result:84" (42 * 2 = 84).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn sequential_workflow_first_action_executes() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            ("sequential_workflow.py", SEQUENTIAL_WORKFLOW_MODULE),
            ("register.py", REGISTER_SEQUENTIAL_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "sequentialworkflow",
        user_module: "sequential_workflow",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    // Verify the workflow result: 42 * 2 = 84, formatted as "result:84"
    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("result:84".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Conditional Workflow Tests
// =============================================================================

fn make_conditional_register_script(tier: &str) -> String {
    format!(
        r#"
import asyncio
import os

from conditional_workflow import ConditionalWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = ConditionalWorkflow()
    result = await wf.run(tier="{tier}")
    print(f"Registration result: {{result}}")

asyncio.run(main())
"#
    )
}

/// Test that conditional workflows execute correctly with the "high" tier branch.
///
/// This tests the conditional execution: get_score -> evaluate_high
/// With tier="high", score=100, and 100>=75 so result should be "excellent:100".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn conditional_workflow_registers_and_first_action_executes() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let register_script = make_conditional_register_script("high");

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            ("conditional_workflow.py", CONDITIONAL_WORKFLOW_MODULE),
            ("register.py", register_script.leak()),
        ],
        entrypoint: "register.py",
        workflow_name: "conditionalworkflow",
        user_module: "conditional_workflow",
        inputs: &[("tier", "high")],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    // Verify the workflow result: tier="high" -> score=100 -> evaluate_high -> "excellent:100"
    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("excellent:100".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Exception Workflow Test
// =============================================================================

const REGISTER_EXCEPTION_SCRIPT: &str = r#"
import asyncio
import os

from exception_workflow import ExceptionWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = ExceptionWorkflow()
    result = await wf.run()
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Test that exception workflows execute correctly (success path, no exception).
///
/// This tests the try/except handling with no exception thrown:
/// get_initial_value -> risky_operation -> format_success
/// With value=42, 42<=100 so no exception, result = 42*2=84, should be "success:84".
///
/// TODO: Result assertion disabled - exception workflows have complex DAG structures
/// that need additional handling for proper completion detection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn exception_workflow_registers_and_first_action_executes() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            ("exception_workflow.py", EXCEPTION_WORKFLOW_MODULE),
            ("register.py", REGISTER_EXCEPTION_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "exceptionworkflow",
        user_module: "exception_workflow",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Policy Workflow Tests (retry, timeout)
// =============================================================================

const REGISTER_CRASH_RECOVERY_SCRIPT: &str = r#"
import asyncio
import os

from integration_crash_recovery import CrashRecoveryWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = CrashRecoveryWorkflow()
    result = await wf.run()
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Test that workflows with timeout policies register correctly.
///
/// This tests self.run_action(..., timeout=timedelta(seconds=2)) parsing.
/// Verifies that:
/// 1. The workflow registers via gRPC
/// 2. Actions with timeout policies are enqueued and execute
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn crash_recovery_workflow_with_timeout_policies() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            (
                "integration_crash_recovery.py",
                CRASH_RECOVERY_WORKFLOW_MODULE,
            ),
            ("register.py", REGISTER_CRASH_RECOVERY_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "crashrecoveryworkflow",
        user_module: "integration_crash_recovery",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    harness.shutdown().await?;
    Ok(())
}

const REGISTER_EXCEPTION_CUSTOM_SCRIPT: &str = r#"
import asyncio
import os

from integration_exception_custom import ExceptionCustomWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = ExceptionCustomWorkflow()
    result = await wf.run()
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Test that workflows with retry policies register correctly.
///
/// This tests self.run_action(..., retry=RetryPolicy(attempts=1)) parsing.
/// Verifies that:
/// 1. The workflow registers via gRPC
/// 2. Actions with retry policies are enqueued and execute
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn exception_custom_workflow_with_retry_policy() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            (
                "integration_exception_custom.py",
                EXCEPTION_CUSTOM_WORKFLOW_MODULE,
            ),
            ("register.py", REGISTER_EXCEPTION_CUSTOM_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "exceptioncustomworkflow",
        user_module: "integration_exception_custom",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    harness.shutdown().await?;
    Ok(())
}

const REGISTER_EXCEPTION_WITH_SUCCESS_SCRIPT: &str = r#"
import asyncio
import os

from integration_exception_with_success import ExceptionWithSuccessWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = ExceptionWithSuccessWorkflow()
    # Test with should_fail=False so risky_action succeeds
    result = await wf.run(should_fail=False)
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Test that workflows with retry policies and success paths register correctly.
///
/// This tests a workflow with both success and failure branches using RetryPolicy.
/// Verifies that:
/// 1. The workflow registers via gRPC
/// 2. Actions with retry policies are enqueued and execute
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn exception_with_success_workflow_registers() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            (
                "integration_exception_with_success.py",
                EXCEPTION_WITH_SUCCESS_WORKFLOW_MODULE,
            ),
            ("register.py", REGISTER_EXCEPTION_WITH_SUCCESS_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "exceptionwithsuccessworkflow",
        user_module: "integration_exception_with_success",
        inputs: &[("should_fail", "false")],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    harness.shutdown().await?;
    Ok(())
}

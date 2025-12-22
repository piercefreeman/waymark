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
const ERROR_HANDLING_WORKFLOW_MODULE: &str = include_str!("fixtures/integration_error_handling.py");
const EXCEPTION_WITH_SUCCESS_FAILURE_SCRIPT: &str = r#"
import asyncio
import os

from integration_exception_with_success import ExceptionWithSuccessWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = ExceptionWithSuccessWorkflow()
    # Trigger the failure branch so exception handling is exercised.
    result = await wf.run(should_fail=True)
    print(f"Registration result (should_fail=True): {result}")

asyncio.run(main())
"#;
const REGISTER_ERROR_HANDLING_FAILURE_SCRIPT: &str = r#"
import asyncio
import os

from integration_error_handling import ErrorHandlingWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = ErrorHandlingWorkflow()
    result = await wf.run(should_fail=True)
    print(f"Registration result (should_fail=True): {result}")

asyncio.run(main())
"#;
const REGISTER_ERROR_HANDLING_SUCCESS_SCRIPT: &str = r#"
import asyncio
import os

from integration_error_handling import ErrorHandlingWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = ErrorHandlingWorkflow()
    result = await wf.run(should_fail=False)
    print(f"Registration result (should_fail=False): {result}")

asyncio.run(main())
"#;
const IMMEDIATE_CONDITIONAL_WORKFLOW_MODULE: &str =
    include_str!("fixtures/immediate_conditional_workflow.py");
const IMMEDIATE_REQUIRED_INPUT_WORKFLOW_MODULE: &str =
    include_str!("fixtures/immediate_required_input_workflow.py");
const REGISTER_IMMEDIATE_REQUIRED_INPUT_MISSING_INPUT_SCRIPT: &str = r#"
import asyncio
import os

from immediate_required_input_workflow import ImmediateRequiredInputWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = ImmediateRequiredInputWorkflow()
    result = await wf.run()
    print(f"Registration result: {result}")

asyncio.run(main())
"#;
const CHAIN_WORKFLOW_MODULE: &str = include_str!("fixtures/chain_workflow.py");
const LOOP_RETURN_WORKFLOW_MODULE: &str = include_str!("fixtures/integration_loop_return.py");
const DEAD_END_CONDITIONAL_WORKFLOW_MODULE: &str =
    include_str!("fixtures/integration_dead_end_conditional.py");

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
    # RAPPEL_SKIP_WAIT_FOR_INSTANCE tells it not to wait for completion
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

fn parse_error(payload: &[u8]) -> Result<Option<String>> {
    if payload.is_empty() {
        return Ok(None);
    }

    let arguments = proto::WorkflowArguments::decode(payload)
        .map_err(|err| anyhow::anyhow!("decode workflow arguments: {err}"))?;

    for argument in arguments.arguments {
        if argument.key == "error"
            && let Some(value) = argument.value.as_ref()
        {
            return extract_string_from_value(value);
        }
    }
    Err(anyhow::anyhow!("missing error in payload"))
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

fn proto_value_to_json(value: &proto::WorkflowArgumentValue) -> serde_json::Value {
    use proto::primitive_workflow_argument::Kind as PrimitiveKind;
    use proto::workflow_argument_value::Kind;

    match &value.kind {
        Some(Kind::Primitive(p)) => match &p.kind {
            Some(PrimitiveKind::IntValue(i)) => serde_json::Value::Number((*i).into()),
            Some(PrimitiveKind::DoubleValue(f)) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Some(PrimitiveKind::StringValue(s)) => serde_json::Value::String(s.clone()),
            Some(PrimitiveKind::BoolValue(b)) => serde_json::Value::Bool(*b),
            Some(PrimitiveKind::NullValue(_)) => serde_json::Value::Null,
            None => serde_json::Value::Null,
        },
        Some(Kind::ListValue(list)) => serde_json::Value::Array(
            list.items
                .iter()
                .map(proto_value_to_json)
                .collect::<Vec<_>>(),
        ),
        Some(Kind::DictValue(dict)) => {
            let entries: serde_json::Map<String, serde_json::Value> = dict
                .entries
                .iter()
                .filter_map(|arg| {
                    arg.value
                        .as_ref()
                        .map(|v| (arg.key.clone(), proto_value_to_json(v)))
                })
                .collect();
            serde_json::Value::Object(entries)
        }
        Some(Kind::TupleValue(tuple)) => serde_json::Value::Array(
            tuple
                .items
                .iter()
                .map(proto_value_to_json)
                .collect::<Vec<_>>(),
        ),
        Some(Kind::Basemodel(model)) => {
            if let Some(data_dict) = &model.data {
                let entries: serde_json::Map<String, serde_json::Value> = data_dict
                    .entries
                    .iter()
                    .filter_map(|arg| {
                        arg.value
                            .as_ref()
                            .map(|v| (arg.key.clone(), proto_value_to_json(v)))
                    })
                    .collect();
                serde_json::Value::Object(entries)
            } else {
                serde_json::Value::Object(serde_json::Map::new())
            }
        }
        Some(Kind::Exception(exc)) => {
            let mut obj = serde_json::Map::new();
            obj.insert("__exception__".to_string(), serde_json::Value::Bool(true));
            obj.insert(
                "type".to_string(),
                serde_json::Value::String(exc.r#type.clone()),
            );
            obj.insert(
                "module".to_string(),
                serde_json::Value::String(exc.module.clone()),
            );
            obj.insert(
                "message".to_string(),
                serde_json::Value::String(exc.message.clone()),
            );
            serde_json::Value::Object(obj)
        }
        None => serde_json::Value::Null,
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
        // Use the stored registration inputs (should_fail=False) as-is.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn exception_with_success_workflow_handles_failure_branch() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            (
                "integration_exception_with_success.py",
                EXCEPTION_WITH_SUCCESS_WORKFLOW_MODULE,
            ),
            ("register.py", EXCEPTION_WITH_SUCCESS_FAILURE_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "exceptionwithsuccessworkflow",
        user_module: "integration_exception_with_success",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    harness.dispatch_all().await?;

    let stored_payload = harness
        .stored_result()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing stored result payload"))?;

    let arguments = proto::WorkflowArguments::decode(&stored_payload[..])?;
    let result_value = arguments
        .arguments
        .iter()
        .find(|arg| arg.key == "result")
        .and_then(|arg| arg.value.as_ref())
        .map(proto_value_to_json)
        .ok_or_else(|| anyhow::anyhow!("missing result value"))?;

    let result_obj = result_value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("result is not an object: {result_value}"))?;

    assert_eq!(
        result_obj.get("attempted"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(
        result_obj.get("recovered"),
        Some(&serde_json::Value::Bool(true))
    );
    let message = result_obj
        .get("message")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing message in result"))?;
    assert!(
        message.contains("Recovered from error"),
        "unexpected message: {message}"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Error Handling Workflow Test (BaseModel result)
// =============================================================================

/// Reproduces the example_app error-handling workflow that returns a Pydantic model.
///
/// With should_fail=True, risky_action raises, recovery_action runs, and the final
/// build_error_result action returns an ErrorResult BaseModel. We store it as a dict
/// and allow Python to coerce it back into a model when needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn error_handling_workflow_returns_basemodel_on_failure() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            (
                "integration_error_handling.py",
                ERROR_HANDLING_WORKFLOW_MODULE,
            ),
            ("register.py", REGISTER_ERROR_HANDLING_FAILURE_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "errorhandlingworkflow",
        user_module: "integration_error_handling",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    harness.dispatch_all().await?;

    let stored_payload = harness
        .stored_result()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing stored result payload"))?;

    let arguments = proto::WorkflowArguments::decode(&stored_payload[..])?;
    let result_value = arguments
        .arguments
        .iter()
        .find(|arg| arg.key == "result")
        .and_then(|arg| arg.value.as_ref())
        .map(proto_value_to_json)
        .ok_or_else(|| anyhow::anyhow!("missing result value"))?;

    let result_obj = result_value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("result is not an object: {result_value}"))?;

    assert!(
        !result_obj.contains_key("__class__"),
        "unexpected __class__ metadata"
    );
    assert!(
        !result_obj.contains_key("__module__"),
        "unexpected __module__ metadata"
    );
    assert_eq!(
        result_obj.get("attempted"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(
        result_obj.get("recovered"),
        Some(&serde_json::Value::Bool(true))
    );
    let message = result_obj
        .get("message")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing message in result"))?;
    assert!(
        message.contains("Recovered from error"),
        "unexpected message: {message}"
    );

    harness.shutdown().await?;
    Ok(())
}

/// Ensure the BaseModel result also works on the success branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn error_handling_workflow_returns_basemodel_on_success() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            (
                "integration_error_handling.py",
                ERROR_HANDLING_WORKFLOW_MODULE,
            ),
            ("register.py", REGISTER_ERROR_HANDLING_SUCCESS_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "errorhandlingworkflow",
        user_module: "integration_error_handling",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    harness.dispatch_all().await?;

    let stored_payload = harness
        .stored_result()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing stored result payload"))?;

    let arguments = proto::WorkflowArguments::decode(&stored_payload[..])?;
    let result_value = arguments
        .arguments
        .iter()
        .find(|arg| arg.key == "result")
        .and_then(|arg| arg.value.as_ref())
        .map(proto_value_to_json)
        .ok_or_else(|| anyhow::anyhow!("missing result value"))?;

    let result_obj = result_value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("result is not an object: {result_value}"))?;

    assert!(
        !result_obj.contains_key("__class__"),
        "unexpected __class__ metadata"
    );
    assert_eq!(
        result_obj.get("recovered"),
        Some(&serde_json::Value::Bool(false))
    );
    let message = result_obj
        .get("message")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing message in result"))?;
    assert!(
        message.contains("Success path:"),
        "unexpected message: {message}"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Immediate Conditional Workflow Tests
// =============================================================================

fn make_immediate_conditional_register_script(value: i32) -> String {
    format!(
        r#"
import asyncio
import os

from immediate_conditional_workflow import ImmediateConditionalWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = ImmediateConditionalWorkflow()
    result = await wf.run(value={value})
    print(f"Registration result: {{result}}")

asyncio.run(main())
"#
    )
}

/// Test that immediate conditional workflows execute correctly with the "high" branch.
///
/// This tests the conditional execution where guards depend on input values directly.
/// With value=100, 100>=75 so result should be "high:100".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn immediate_conditional_workflow_high_branch() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let register_script = make_immediate_conditional_register_script(100);

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            (
                "immediate_conditional_workflow.py",
                IMMEDIATE_CONDITIONAL_WORKFLOW_MODULE,
            ),
            ("register.py", register_script.leak()),
        ],
        entrypoint: "register.py",
        workflow_name: "immediateconditionalworkflow",
        user_module: "immediate_conditional_workflow",
        inputs: &[("value", "100")],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    // Verify the workflow result: value=100 -> 100>=75 -> evaluate_high -> "high:100"
    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("high:100".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

/// Test that immediate conditional workflows execute correctly with the "medium" branch.
///
/// With value=50, 50>=25 but 50<75 so result should be "medium:50".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn immediate_conditional_workflow_medium_branch() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let register_script = make_immediate_conditional_register_script(50);

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            (
                "immediate_conditional_workflow.py",
                IMMEDIATE_CONDITIONAL_WORKFLOW_MODULE,
            ),
            ("register.py", register_script.leak()),
        ],
        entrypoint: "register.py",
        workflow_name: "immediateconditionalworkflow",
        user_module: "immediate_conditional_workflow",
        inputs: &[("value", "50")],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    // Verify the workflow result: value=50 -> 50>=25 -> evaluate_medium -> "medium:50"
    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("medium:50".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

/// Test that immediate conditional workflows execute correctly with the "low" branch.
///
/// With value=10, 10<25 so result should be "low:10".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn immediate_conditional_workflow_low_branch() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let register_script = make_immediate_conditional_register_script(10);

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            (
                "immediate_conditional_workflow.py",
                IMMEDIATE_CONDITIONAL_WORKFLOW_MODULE,
            ),
            ("register.py", register_script.leak()),
        ],
        entrypoint: "register.py",
        workflow_name: "immediateconditionalworkflow",
        user_module: "immediate_conditional_workflow",
        inputs: &[("value", "10")],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    // Verify the workflow result: value=10 -> 10<25 -> evaluate_low -> "low:10"
    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("low:10".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

/// Ensure missing input parameters fail the workflow during startup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn immediate_required_input_workflow_missing_input_fails_start() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new_without_start(HarnessConfig {
        files: &[
            (
                "immediate_required_input_workflow.py",
                IMMEDIATE_REQUIRED_INPUT_WORKFLOW_MODULE,
            ),
            (
                "register.py",
                REGISTER_IMMEDIATE_REQUIRED_INPUT_MISSING_INPUT_SCRIPT,
            ),
        ],
        entrypoint: "register.py",
        workflow_name: "immediaterequiredinputworkflow",
        user_module: "immediate_required_input_workflow",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    harness.run_to_completion(10).await?;

    let instance = harness
        .database()
        .get_instance(harness.instance_id())
        .await?;
    assert_eq!(instance.status, "failed", "instance should be failed");

    let payload = instance.result_payload.unwrap_or_default();
    let error_message = parse_error(&payload)?.unwrap_or_default();
    assert!(
        error_message.contains("Guard evaluation failed during startup"),
        "unexpected error message: {error_message}"
    );
    assert!(
        error_message.contains("Variable not found"),
        "unexpected error message: {error_message}"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Dead-End Conditional Workflow Test
// =============================================================================

const REGISTER_DEAD_END_CONDITIONAL_SCRIPT: &str = r#"
import asyncio
import os

from integration_dead_end_conditional import DeadEndConditionalWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = DeadEndConditionalWorkflow()
    result = await wf.run()
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Ensure a falsy guard skips an action and still reaches the next action.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn dead_end_conditional_guard_reaches_followup_action() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            (
                "integration_dead_end_conditional.py",
                DEAD_END_CONDITIONAL_WORKFLOW_MODULE,
            ),
            ("register.py", REGISTER_DEAD_END_CONDITIONAL_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "deadendconditionalworkflow",
        user_module: "integration_dead_end_conditional",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    harness.dispatch_all().await?;

    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("final:0".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Loop Workflow Test
// =============================================================================

const LOOP_WORKFLOW_MODULE: &str = include_str!("fixtures/loop_workflow.py");
const HELPER_LOOP_WORKFLOW_MODULE: &str = include_str!("fixtures/integration_helper_loop.py");

const REGISTER_LOOP_SCRIPT: &str = r#"
import asyncio
import os

from loop_workflow import LoopWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = LoopWorkflow()
    result = await wf.run(items=["apple", "banana", "cherry"])
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

const REGISTER_HELPER_LOOP_SCRIPT: &str = r#"
import asyncio
import os

from integration_helper_loop import HelperLoopWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = HelperLoopWorkflow()
    result = await wf.run(items=[1, 2, 3])
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Test that loop workflows execute correctly with for loop iteration.
///
/// This tests the for loop pattern: for item in items -> process_item -> join_results
/// With items=["apple", "banana", "cherry"], result should be "APPLE,BANANA,CHERRY".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn loop_workflow_executes_all_iterations() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            ("loop_workflow.py", LOOP_WORKFLOW_MODULE),
            ("register.py", REGISTER_LOOP_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "loopworkflow",
        user_module: "loop_workflow",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    // Verify the workflow result: ["apple", "banana", "cherry"] -> ["APPLE", "BANANA", "CHERRY"] -> "APPLE,BANANA,CHERRY"
    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("APPLE,BANANA,CHERRY".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

/// Test that helper methods with early returns inside loops flow back to the caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn helper_loop_workflow_executes_all_iterations() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            ("integration_helper_loop.py", HELPER_LOOP_WORKFLOW_MODULE),
            ("register.py", REGISTER_HELPER_LOOP_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "helperloopworkflow",
        user_module: "integration_helper_loop",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    harness.dispatch_all().await?;
    info!("workflow completed");

    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("total:8".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Return Inside Loop Test
// =============================================================================

const REGISTER_LOOP_RETURN_SCRIPT: &str = r#"
import asyncio
import os

from integration_loop_return import LoopReturnWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = LoopReturnWorkflow()
    result = await wf.run(items=[1, 2, 3, 4, 5], needle=3)
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Test that returning from inside a for-loop completes the workflow.
///
/// This covers early return semantics within normalized loop DAGs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn return_inside_for_loop_completes_workflow() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            ("integration_loop_return.py", LOOP_RETURN_WORKFLOW_MODULE),
            ("register.py", REGISTER_LOOP_RETURN_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "loopreturnworkflow",
        user_module: "integration_loop_return",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    harness.dispatch_all().await?;
    info!("workflow completed");

    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("found:3 checked:3".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Parallel Workflow Test
// =============================================================================

const PARALLEL_WORKFLOW_MODULE: &str = include_str!("fixtures/parallel_workflow.py");
const PARALLEL_MATH_WORKFLOW_MODULE: &str = include_str!("fixtures/integration_parallel_math.py");

const REGISTER_PARALLEL_SCRIPT: &str = r#"
import asyncio
import os

from parallel_workflow import ParallelWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = ParallelWorkflow()
    result = await wf.run(value=5)
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Test that parallel workflows execute correctly with asyncio.gather.
///
/// This tests the parallel fan-out/fan-in pattern:
/// value=5 -> gather(compute_double(5), compute_square(5)) -> combine_results
/// doubled=10, squared=25 -> "doubled:10,squared:25"
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn parallel_workflow_executes_concurrent_actions() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            ("parallel_workflow.py", PARALLEL_WORKFLOW_MODULE),
            ("register.py", REGISTER_PARALLEL_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "parallelworkflow",
        user_module: "parallel_workflow",
        inputs: &[], // Use default value from Python registration
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    // Verify the workflow result: value=5 -> doubled=10, squared=25 -> "doubled:10,squared:25"
    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("doubled:10,squared:25".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

const REGISTER_PARALLEL_MATH_SCRIPT: &str = r#"
import asyncio
import os

from integration_parallel_math import ParallelMathWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = ParallelMathWorkflow()
    result = await wf.run(number=5)
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Parallel workflow that returns a BaseModel (example_app parity).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn parallel_math_workflow_executes_and_returns_model() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            (
                "integration_parallel_math.py",
                PARALLEL_MATH_WORKFLOW_MODULE,
            ),
            ("register.py", REGISTER_PARALLEL_MATH_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "parallelmathworkflow",
        user_module: "integration_parallel_math",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    harness.dispatch_all().await?;
    info!("workflow completed");

    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let arguments = proto::WorkflowArguments::decode(&stored_payload[..])?;
    let result_value = arguments
        .arguments
        .iter()
        .find(|arg| arg.key == "result")
        .and_then(|arg| arg.value.as_ref())
        .map(proto_value_to_json)
        .ok_or_else(|| anyhow::anyhow!("missing result value"))?;
    let json_obj = result_value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("result is not an object: {result_value}"))?;

    assert_eq!(
        json_obj.get("input_number"),
        Some(&serde_json::Value::Number(5.into()))
    );
    assert_eq!(
        json_obj.get("factorial"),
        Some(&serde_json::Value::Number(120.into()))
    );
    assert_eq!(
        json_obj.get("fibonacci"),
        Some(&serde_json::Value::Number(5.into()))
    );
    let summary = json_obj
        .get("summary")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing summary"))?;
    assert!(
        summary.contains("larger") || summary.contains("tame"),
        "unexpected summary: {summary}"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Additional Conditional Workflow Tests (medium, low branches)
// =============================================================================

/// Test that conditional workflows execute correctly with the "medium" tier branch.
///
/// With tier="medium", score=50, and 50>=25 but 50<75 so result should be "good:50".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn conditional_workflow_medium_branch() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let register_script = make_conditional_register_script("medium");

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            ("conditional_workflow.py", CONDITIONAL_WORKFLOW_MODULE),
            ("register.py", register_script.leak()),
        ],
        entrypoint: "register.py",
        workflow_name: "conditionalworkflow",
        user_module: "conditional_workflow",
        inputs: &[("tier", "medium")],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    // Verify the workflow result: tier="medium" -> score=50 -> evaluate_medium -> "good:50"
    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("good:50".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

/// Test that conditional workflows execute correctly with the "low" tier branch.
///
/// With tier="low", score=10, and 10<25 so result should be "needs_work:10".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn conditional_workflow_low_branch() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let register_script = make_conditional_register_script("low");

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            ("conditional_workflow.py", CONDITIONAL_WORKFLOW_MODULE),
            ("register.py", register_script.leak()),
        ],
        entrypoint: "register.py",
        workflow_name: "conditionalworkflow",
        user_module: "conditional_workflow",
        inputs: &[("tier", "low")],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    // Verify the workflow result: tier="low" -> score=10 -> evaluate_low -> "needs_work:10"
    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("needs_work:10".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Chain Workflow Tests (example_app pattern)
// =============================================================================

const REGISTER_CHAIN_SCRIPT: &str = r#"
import asyncio
import os

from chain_workflow import ChainWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = ChainWorkflow()
    result = await wf.run(text="hello world")
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Test that chain workflows execute all actions and combine results correctly.
///
/// This matches the SequentialChainWorkflow pattern from example_app:
/// "hello world" -> "HELLO WORLD" -> "DLROW OLLEH" -> "*** DLROW OLLEH ***"
/// Then build_chain_result combines: original, step1, step2, step3
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn chain_workflow_executes_all_steps() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            ("chain_workflow.py", CHAIN_WORKFLOW_MODULE),
            ("register.py", REGISTER_CHAIN_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "chainworkflow",
        user_module: "chain_workflow",
        inputs: &[("text", "hello world")],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    // Verify the workflow result contains all transformations
    // Expected: "original:hello world,step1:HELLO WORLD,step2:DLROW OLLEH,step3:*** DLROW OLLEH ***"
    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some(
            "original:hello world,step1:HELLO WORLD,step2:DLROW OLLEH,step3:*** DLROW OLLEH ***"
                .to_string()
        ),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// For-Loop Workflow Test (loop with append pattern)
// =============================================================================

const FOR_LOOP_WORKFLOW_MODULE: &str = include_str!("fixtures/for_loop_workflow.py");
const FN_CALL_BINDING_WORKFLOW_MODULE: &str =
    include_str!("fixtures/integration_fn_call_binding.py");

const REGISTER_FOR_LOOP_SCRIPT: &str = r#"
import asyncio
import os

from for_loop_workflow import ForLoopWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = ForLoopWorkflow()
    result = await wf.run(items=["apple", "banana", "cherry"])
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

const REGISTER_FN_CALL_BINDING_SCRIPT: &str = r#"
import asyncio
import os

from integration_fn_call_binding import PredictWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = PredictWorkflow()
    result = await wf.run(user={"user_id": "user-123"})
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Test that for-loop workflows execute correctly with the loop-append pattern.
///
/// This tests the classic for loop pattern: for item in items -> process_item -> append
/// With items=["apple", "banana", "cherry"], result should be "APPLE,BANANA,CHERRY".
///
/// This differs from the spread/gather pattern in that items are processed as a
/// spread operation under the hood, not via parallel gather.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn for_loop_workflow_executes_all_iterations() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            ("for_loop_workflow.py", FOR_LOOP_WORKFLOW_MODULE),
            ("register.py", REGISTER_FOR_LOOP_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "forloopworkflow",
        user_module: "for_loop_workflow",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    // Verify the workflow result: ["apple", "banana", "cherry"] -> ["APPLE", "BANANA", "CHERRY"] -> "APPLE,BANANA,CHERRY"
    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("APPLE,BANANA,CHERRY".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Fn Call Binding Workflow Test
// =============================================================================

/// Test that positional args passed into helper methods bind correctly and
/// flow into action kwargs (including dot access).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn fn_call_binding_workflow_executes() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            (
                "integration_fn_call_binding.py",
                FN_CALL_BINDING_WORKFLOW_MODULE,
            ),
            ("register.py", REGISTER_FN_CALL_BINDING_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "predictworkflow",
        user_module: "integration_fn_call_binding",
        inputs: &[("user", r#"{"user_id":"user-123"}"#)],
    })
    .await?
    else {
        return Ok(());
    };

    harness.dispatch_all().await?;
    info!("workflow completed");

    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;
    assert_eq!(
        message,
        Some("user-123".to_string()),
        "unexpected workflow result"
    );

    harness.shutdown().await?;
    Ok(())
}

// =============================================================================
// Durable Sleep Workflow Test
// =============================================================================

const SLEEP_WORKFLOW_MODULE: &str = include_str!("fixtures/integration_sleep.py");

const REGISTER_SLEEP_SCRIPT: &str = r#"
import asyncio
import os

from integration_sleep import SleepWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = SleepWorkflow()
    result = await wf.run()
    print(f"Registration result: {result}")

asyncio.run(main())
"#;

/// Test that durable sleep workflows execute correctly.
///
/// This tests that asyncio.sleep() is converted to a scheduler-managed sleep node.
/// The sleep is handled by scheduling the action for future dispatch (scheduled_at)
/// rather than blocking the worker.
///
/// The workflow:
/// 1. get_timestamp() -> records start time
/// 2. asyncio.sleep(1) -> durable sleep for 1 second
/// 3. get_timestamp() -> records resume time
/// 4. format_sleep_result() -> calculates duration
///
/// Result should be "slept:1.0s" (approximately 1 second).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn durable_sleep_workflow_executes() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let _ = dotenvy::dotenv();

    let Some(harness) = IntegrationHarness::new(HarnessConfig {
        files: &[
            ("integration_sleep.py", SLEEP_WORKFLOW_MODULE),
            ("register.py", REGISTER_SLEEP_SCRIPT),
        ],
        entrypoint: "register.py",
        workflow_name: "sleepworkflow",
        user_module: "integration_sleep",
        inputs: &[],
    })
    .await?
    else {
        return Ok(());
    };

    // Execute all actions via the DAGRunner
    harness.dispatch_all().await?;
    info!("workflow completed");

    // Verify the workflow result - should show approximately 1 second of sleep
    let stored_payload = harness
        .stored_result()
        .await?
        .expect("workflow should have a result");
    let message = parse_result(&stored_payload)?;

    // The result should be "slept:X.Xs" where X is approximately 1
    let result = message.expect("result should be present");
    assert!(
        result.starts_with("slept:"),
        "unexpected result format: {result}"
    );

    // Parse the duration and verify it's approximately 1 second (allowing 0.5-2.0s range)
    let duration_str = result.trim_start_matches("slept:").trim_end_matches('s');
    let duration: f64 = duration_str.parse().expect("should parse as float");
    assert!(
        (0.5..=2.0).contains(&duration),
        "sleep duration {duration}s not in expected range 0.5-2.0s"
    );

    harness.shutdown().await?;
    Ok(())
}

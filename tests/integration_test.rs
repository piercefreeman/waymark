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
        if argument.key == "result" {
            if let Some(value) = argument.value.as_ref() {
                return extract_string_from_value(value);
            }
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

    // Dispatch and execute all actions
    let completed = harness.dispatch_all().await?;
    info!(completed = completed.len(), "actions completed");

    // Should have at least 1 action (the greet call)
    assert!(
        !completed.is_empty(),
        "expected at least one action to complete"
    );

    // All actions should have succeeded
    for metrics in &completed {
        assert!(
            metrics.success,
            "action {} failed: {:?}",
            metrics.action_id, metrics.error_message
        );
    }

    // Verify the workflow result
    if let Some(stored_payload) = harness.stored_result().await? {
        let message = parse_result(&stored_payload)?;
        if let Some(msg) = message {
            assert_eq!(msg, "hello world", "unexpected workflow result");
        }
    }

    harness.shutdown().await?;
    Ok(())
}

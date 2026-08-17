//! Execution harness for generated fuzz cases.

use std::collections::HashMap;
use std::time::Duration;

use color_eyre::eyre::bail;
use waymark_ir_parser::parse_program;
use waymark_worker_core::WorkerPoolError;
use waymark_worker_inline::{InlineActionCallable, InlineWorkerPool};
use waymark_worker_inline_compat::inline_action;

use super::generator::GeneratedCase;

pub async fn run_case(
    case_index: usize,
    case: &GeneratedCase,
) -> Result<(), color_eyre::eyre::Report> {
    let program = parse_program(case.source.trim()).map_err(|err| {
        color_eyre::eyre::eyre!(
            "case {case_index} failed to parse: {err}\n--- program ---\n{}",
            case.source
        )
    })?;

    let program = waymark_vm_ast_old_proto::convert(program).map_err(|err| {
        color_eyre::eyre::eyre!(
            "case {case_index} failed to convert to the VM AST: {err}\n--- program ---\n{}",
            case.source
        )
    })?;

    let inputs = HashMap::from([(
        "base".to_string(),
        waymark_system_vm::Value::Ready(waymark_system_vm::ReadyValue::Int(case.base_input)),
    )]);

    let runtime =
        waymark_transient_execution_bringup::setup_runtime(&program, inputs).map_err(|err| {
            color_eyre::eyre::eyre!(
                "case {case_index} failed to compile: {err}\n--- program ---\n{}",
                case.source
            )
        })?;

    let worker_pool = InlineWorkerPool::new(action_registry());
    let cancel = tokio_util::sync::CancellationToken::new();
    let waymark_transient_execution_bringup::Execution {
        workflow_outcome_rx,
        driver_handle,
    } = waymark_transient_execution_worker_pool_bringup::execute(
        runtime,
        worker_pool,
        false,
        cancel.clone(),
    );

    let workflow_outcome =
        match tokio::time::timeout(Duration::from_secs(5), workflow_outcome_rx).await {
            Ok(received) => received,
            Err(_elapsed) => {
                cancel.cancel();
                let Err(driver_exit) = driver_handle.await;
                tracing::debug!(?driver_exit, "vm driver exited after cancellation");
                bail!(
                    "case {case_index} timed out\n--- program ---\n{}",
                    case.source
                )
            }
        };

    // The driver terminates right after delivering the workflow outcome —
    // including on success — so join it unconditionally for its exit report.
    let Err(driver_exit) = driver_handle.await;
    tracing::debug!(?driver_exit, "vm driver exited");

    let workflow_outcome = workflow_outcome.map_err(|_recv_error| {
        color_eyre::eyre::eyre!(
            "case {case_index}: vm driver exited without delivering a workflow outcome\n--- program ---\n{}",
            case.source
        )
    })?;

    match workflow_outcome {
        waymark_workflow_completion_core::Outcome::Completion(_value) => {}
        waymark_workflow_completion_core::Outcome::Exception(exception) => bail!(
            "case {case_index} completed with an exception: {exception:?}\n--- program ---\n{}",
            case.source
        ),
    }

    if (case_index + 1).is_multiple_of(10) {
        println!("Completed fuzz cases: {}", case_index + 1);
    }

    Ok(())
}

fn action_registry() -> HashMap<String, InlineActionCallable> {
    let mut actions: HashMap<String, InlineActionCallable> = HashMap::new();
    actions.insert("inc".to_string(), inline_action(action_inc));
    actions.insert("double".to_string(), inline_action(action_double));
    actions.insert("sum".to_string(), inline_action(action_sum));
    actions
}

async fn action_inc(
    kwargs: HashMap<String, serde_json::Value>,
) -> Result<serde_json::Value, WorkerPoolError> {
    let value = get_i64(&kwargs, "value")?;
    Ok(serde_json::Value::Number((value + 1).into()))
}

async fn action_double(
    kwargs: HashMap<String, serde_json::Value>,
) -> Result<serde_json::Value, WorkerPoolError> {
    let value = get_i64(&kwargs, "value")?;
    Ok(serde_json::Value::Number((value * 2).into()))
}

async fn action_sum(
    kwargs: HashMap<String, serde_json::Value>,
) -> Result<serde_json::Value, WorkerPoolError> {
    let values = kwargs
        .get("values")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| WorkerPoolError::new("ActionError", "sum expects array input"))?;
    let mut total = 0i64;
    for item in values {
        let value = item
            .as_i64()
            .ok_or_else(|| WorkerPoolError::new("ActionError", "sum expects integer elements"))?;
        total += value;
    }
    Ok(serde_json::Value::Number(total.into()))
}

fn get_i64(kwargs: &HashMap<String, serde_json::Value>, key: &str) -> Result<i64, WorkerPoolError> {
    kwargs
        .get(key)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| WorkerPoolError::new("ActionError", format!("missing integer '{key}'")))
}

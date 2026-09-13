//! Execution harness for generated fuzz cases.

use std::collections::HashMap;
use std::time::Duration;

use color_eyre::eyre::bail;
use waymark_ir_parser::parse_program;
use waymark_worker_inline::InlineActionCallable;
use waymark_worker_inline_compat::inline_action;

/// The supervisor of a case's tasks: their errors differ per task, so
/// they are supervised erased.
type Supervisor = waymark_task_supervisor::Supervisor<waymark_task_supervisor::BoxedError>;

/// The inline adapter pinned to this binary's flavor converter.
fn py_inline_action<F, Fut>(body: F) -> waymark_worker_inline::InlineActionCallable
where
    F: Fn(std::collections::HashMap<String, waymark_vm_value_python::ReadyValue>) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: Future<
            Output = Result<
                waymark_vm_value_python::ReadyValue,
                waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>,
            >,
        > + Send
        + 'static,
{
    inline_action::<
        waymark_vm_value_python_convert_proto::ActionArgumentsConverter,
        waymark_vm_value_python_convert_proto::ActionOutcomeConverter,
        _,
        _,
        _,
    >(body)
}

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

    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor: Supervisor = waymark_task_supervisor::start(shutdown_token.clone());

    let (worker_pool, pool_loop) = waymark_worker_inline::run(action_registry());
    supervisor.spawn("inline worker pool loop", pool_loop);

    let cancel = tokio_util::sync::CancellationToken::new();
    let waymark_transient_execution_bringup::Execution {
        workflow_outcome_rx,
        driver_handle,
    } = waymark_transient_execution_worker_pool_bringup::execute(
        runtime,
        std::sync::Arc::new(worker_pool),
        false,
        cancel.clone(),
    );

    let workflow_outcome =
        match tokio::time::timeout(Duration::from_secs(5), workflow_outcome_rx).await {
            Ok(received) => received,
            Err(_elapsed) => {
                cancel.cancel();
                shutdown_token.cancel();
                let Err(driver_exit) = driver_handle.await;
                tracing::debug!(?driver_exit, "vm driver exited after cancellation");
                let report = supervisor.drain().await;
                tracing::debug!(%report, "tasks drained");
                bail!(
                    "case {case_index} timed out\n--- program ---\n{}",
                    case.source
                )
            }
        };

    // The outcome is the end of the work, so the shutdown is requested
    // here, before the VM driver is joined: joining it drops the only
    // worker pool handle, which is what ends the worker pool loop.
    shutdown_token.cancel();

    // The driver terminates right after delivering the workflow outcome —
    // including on success — so join it unconditionally for its exit report.
    let Err(driver_exit) = driver_handle.await;
    tracing::debug!(?driver_exit, "vm driver exited");

    let report = supervisor.drain().await;
    if report.any_before_shutdown() {
        bail!(
            "case {case_index}: a task ended before the shutdown was requested:\n{report}\n--- program ---\n{}",
            case.source
        );
    }

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
    actions.insert("inc".to_string(), py_inline_action(action_inc));
    actions.insert("double".to_string(), py_inline_action(action_double));
    actions.insert("sum".to_string(), py_inline_action(action_sum));
    actions
}

/// The exception an action raises for a malformed call.
fn action_error(
    message: String,
) -> waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue> {
    waymark_vm_runtime_exception::Exception {
        type_id: "ActionError".to_owned(),
        details: waymark_vm_value_python::ReadyValue::Dict(indexmap::IndexMap::from([(
            "message".to_owned(),
            waymark_vm_value_python::Value::Ready(waymark_vm_value_python::ReadyValue::String(
                message,
            )),
        )])),
    }
}

async fn action_inc(
    kwargs: HashMap<String, waymark_vm_value_python::ReadyValue>,
) -> Result<
    waymark_vm_value_python::ReadyValue,
    waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>,
> {
    let value = get_i64(&kwargs, "value")?;
    Ok(waymark_vm_value_python::ReadyValue::Int(value + 1))
}

async fn action_double(
    kwargs: HashMap<String, waymark_vm_value_python::ReadyValue>,
) -> Result<
    waymark_vm_value_python::ReadyValue,
    waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>,
> {
    let value = get_i64(&kwargs, "value")?;
    Ok(waymark_vm_value_python::ReadyValue::Int(value * 2))
}

async fn action_sum(
    kwargs: HashMap<String, waymark_vm_value_python::ReadyValue>,
) -> Result<
    waymark_vm_value_python::ReadyValue,
    waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>,
> {
    let Some(waymark_vm_value_python::ReadyValue::List(values)) = kwargs.get("values") else {
        return Err(action_error("sum expects list input".to_owned()));
    };
    let mut total = 0i64;
    for item in values {
        let waymark_vm_value_python::Value::Ready(waymark_vm_value_python::ReadyValue::Int(value)) =
            item
        else {
            return Err(action_error("sum expects integer elements".to_owned()));
        };
        total += value;
    }
    Ok(waymark_vm_value_python::ReadyValue::Int(total))
}

fn get_i64(
    kwargs: &HashMap<String, waymark_vm_value_python::ReadyValue>,
    key: &str,
) -> Result<i64, waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>> {
    match kwargs.get(key) {
        Some(waymark_vm_value_python::ReadyValue::Int(value)) => Ok(*value),
        _ => Err(action_error(format!("missing integer '{key}'"))),
    }
}

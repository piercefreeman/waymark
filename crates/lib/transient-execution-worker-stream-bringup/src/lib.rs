//! Transient workflow execution over the worker-stream action transport —
//! the bridge's in-memory / streaming execution path.
//!
//! Instantiates [`waymark_transient_execution_bringup`] with
//! [`waymark_action_runtime_worker_stream`] channels and adapts the
//! workflow outcome onto the gRPC response stream.

#![warn(missing_docs)]

use prost::Message as _;
use tokio::sync::mpsc;
use tonic::Status;
use waymark_action_runtime_metadata::ActionCallCorrelation;
use waymark_convert_core::TryConvert as _;
use waymark_proto::messages as proto;

// ---------------------------------------------------------------------------
// Runtime setup
// ---------------------------------------------------------------------------

/// Errors that can occur in [`setup_runtime`].
#[derive(Debug, thiserror::Error)]
pub enum SetupRuntimeError {
    /// Decoding the registration's IR program failed.
    #[error("decode IR: {0}")]
    DecodeIr(#[source] prost::DecodeError),

    /// Converting the IR program into the old AST failed.
    #[error("convert IR to AST: {0}")]
    ConvertIrToAst(#[source] waymark_vm_ast_old_proto::ConvertError),

    /// Compiling the AST program into an executable failed.
    #[error("compile: {0}")]
    Compile(
        #[source]
        waymark_vm_compiler_for_ast_old::CompileErrorFor<
            waymark_system_vm::Spec,
            waymark_system_vm::Lowering,
        >,
    ),

    /// Converting the workflow arguments into entry-function arguments
    /// failed.
    #[error("convert workflow arguments: {0}")]
    ConvertWorkflowArguments(
        #[source] waymark_vm_value_python_convert_proto::WorkflowArgumentsError,
    ),

    /// The entry function was not found in the executable.
    #[error("invalid entrypoint: {0}")]
    Entrypoint(
        #[source] waymark_vm_runtime::FunctionNotFoundError<waymark_vm_bytecode_core::FunctionId>,
    ),
}

/// Compile a [`proto::WorkflowRegistration`] into a ready-to-run
/// [`waymark_system_vm::Runtime`], entirely in memory, without any database
/// backend.
///
/// The entry function (function 0, the first function in source order)
/// receives its arguments from
/// [`proto::WorkflowRegistration::arguments`] when present.  Each
/// keyword argument is mapped to a positional argument by matching the
/// entry function's input names; absent inputs are filled with
/// [`waymark_system_vm::ReadyValue::None`].
pub fn setup_runtime(
    registration: &waymark_proto::messages::WorkflowRegistration,
) -> Result<waymark_system_vm::Runtime, SetupRuntimeError> {
    let ir_program = waymark_proto::ast::Program::decode(&registration.ir[..])
        .map_err(SetupRuntimeError::DecodeIr)?;
    let ast_program =
        waymark_vm_ast_old_proto::convert(ir_program).map_err(SetupRuntimeError::ConvertIrToAst)?;

    let (executable, metadata) = waymark_vm_compiler_for_ast_old::compile_with_metadata::<
        waymark_system_vm::Spec,
        waymark_system_vm::Lowering,
    >(&ast_program)
    .map_err(SetupRuntimeError::Compile)?;
    let executable = std::sync::Arc::new(executable);

    let call_spec = waymark_workflow_initialization_convert_proto::Converter::<
        waymark_vm_value_python_convert_proto::WorkflowArgumentsConverter,
    >::try_convert((&registration.arguments[..], &metadata))
    .map_err(SetupRuntimeError::ConvertWorkflowArguments)?;

    let interpreter = waymark_system_vm::Interpreter::default();
    let runtime =
        waymark_system_vm::Runtime::with_custom_entrypoint(interpreter, executable, call_spec)
            .map_err(SetupRuntimeError::Entrypoint)?;
    Ok(runtime)
}

// ---------------------------------------------------------------------------
// Execution bringup
// ---------------------------------------------------------------------------

/// Channels returned by [`execute`].
pub struct ExecuteChannels {
    /// Receives action dispatches and the final workflow result.
    /// Convert to a gRPC response stream via
    /// [`tokio_stream::wrappers::ReceiverStream`].
    pub out_rx: mpsc::Receiver<Result<proto::WorkflowStreamResponse, Status>>,

    /// Sender for feeding [`proto::ActionResult`] messages back into the
    /// execution.  The caller should forward every action result received
    /// on the gRPC input stream into this sender.
    pub action_result_tx: mpsc::Sender<proto::ActionResult>,
}

/// Wire up and launch transient workflow execution for the given runtime.
///
/// Instantiates [`waymark_transient_execution_bringup::execute_with`] with
/// [`waymark_action_runtime_worker_stream`] channels, and spawns a
/// background task that awaits the workflow outcome (delivered via a
/// oneshot channel) and emits it on `out_rx`.
///
/// The caller must feed [`proto::ActionResult`]s into
/// [`ExecuteChannels::action_result_tx`] (typically by forwarding them from
/// the gRPC bidir input stream) and convert
/// [`ExecuteChannels::out_rx`] into the gRPC response stream.
///
/// When `skip_sleep` is true, every sleep in the workflow resolves
/// immediately instead of waiting for its deadline.
pub fn execute(runtime: waymark_system_vm::Runtime, skip_sleep: bool) -> ExecuteChannels {
    let (out_tx, out_rx) = mpsc::channel::<Result<proto::WorkflowStreamResponse, Status>>(32);
    let (action_result_tx, action_result_rx) = mpsc::channel::<proto::ActionResult>(32);

    let action_call_requester = waymark_action_runtime_worker_stream::WorkerStreamActionRequester::<
        ActionCallCorrelation,
        waymark_vm_value_python::ReadyValue,
        waymark_vm_value_python_convert_proto::ActionArgumentsConverter,
    >::new(out_tx.clone());
    let action_call_completions_provider =
        waymark_action_runtime_worker_stream::WorkerStreamActionCallCompletionsProvider::<
            ActionCallCorrelation,
            waymark_vm_value_python::ReadyValue,
            waymark_vm_value_python_convert_proto::ActionOutcomeConverter,
        >::new(action_result_rx);

    let cancellation = tokio_util::sync::CancellationToken::new();

    let waymark_transient_execution_bringup::Execution {
        workflow_outcome_rx,
        driver_handle,
    } = waymark_transient_execution_bringup::execute_with(
        runtime,
        action_call_requester,
        action_call_completions_provider,
        skip_sleep,
        cancellation.clone(),
    );

    tokio::spawn(async move {
        let response = match workflow_outcome_rx.await {
            Ok(workflow_outcome) => convert_workflow_outcome_to_stream_response(workflow_outcome),
            Err(_recv_error) => {
                tracing::error!(
                    "workflow completion sender dropped without sending the workflow outcome"
                );
                Err(Status::internal(
                    "workflow driver exited without recording a result",
                ))
            }
        };

        let _ = out_tx.send(response).await;

        cancellation.cancel();

        let Err(err) = driver_handle.await;
        tracing::warn!(?err, "vm driver exited");
    });

    ExecuteChannels {
        out_rx,
        action_result_tx,
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Convert a typed [`waymark_workflow_completion_core::Outcome`] to a
/// [`proto::WorkflowStreamResponse`].
///
/// A conversion failure is surfaced to the caller as an error [`Status`]
/// rather than being masked as an empty payload.
#[allow(
    clippy::result_large_err,
    reason = "the workflow outcome feeds a tonic::Status gRPC response stream"
)]
fn convert_workflow_outcome_to_stream_response(
    workflow_outcome: waymark_workflow_completion_core::Outcome<waymark_system_vm::ReadyValue>,
) -> Result<proto::WorkflowStreamResponse, Status> {
    let payload: Vec<u8> =
        waymark_vm_value_python_convert_proto::WorkflowOutcomeConverter::try_convert(
            workflow_outcome,
        )
        .map_err(|err| Status::internal(format!("convert workflow outcome: {err}")))?;

    Ok(proto::WorkflowStreamResponse {
        kind: Some(proto::workflow_stream_response::Kind::WorkflowResult(
            proto::WorkflowExecutionResult { payload },
        )),
    })
}

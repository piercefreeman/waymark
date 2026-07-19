//! In-memory transient workflow execution bringup — wires the
//! [`waymark_execution_effector`] with
//! [`waymark_action_runtime_worker_stream`] channels and spawns the VM
//! driver on a dedicated OS thread.
//!
//! This is the bringup counterpart to [`waymark_execution_bringup`] for
//! the bridge's in-memory / streaming execution path.

#![warn(missing_docs)]

use prost::Message as _;
use tokio::sync::mpsc;
use tonic::Status;
use waymark_action_runtime_metadata::ActionCallCorrelation;
use waymark_convert_core::TryConvert as _;
use waymark_proto::messages as proto;
use waymark_vm_driver_core::SnapshotPersister;
use waymark_workflow_completion_core::Outcome;

// ---------------------------------------------------------------------------
// No-op snapshot persister (transient execution does not persist state)
// ---------------------------------------------------------------------------

/// No-op snapshot persister for transient execution.
pub struct NoopPersister;

impl SnapshotPersister for NoopPersister {
    type Error = core::convert::Infallible;

    async fn persist_snapshot(&self, _snapshot: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Runtime setup
// ---------------------------------------------------------------------------

/// Compile a [`proto::WorkflowRegistration`] into a ready-to-run
/// [`waymark_system_vm::Runtime`], entirely in memory, without any database
/// backend.
///
/// The entry function (function 0, the first function in source order)
/// receives its arguments from [`proto::WorkflowRegistration::initial_context`]
/// when present.  Each keyword argument is mapped to a positional argument
/// by matching the entry function's input names.
pub async fn setup_runtime(
    registration: &waymark_proto::messages::WorkflowRegistration,
) -> Result<waymark_system_vm::Runtime, Box<dyn std::error::Error + Send + Sync>> {
    let ir_program = waymark_proto::ast::Program::decode(&registration.ir[..])
        .map_err(|err| anyhow::anyhow!("decode IR: {err}"))?;
    let ast_program = waymark_vm_ast_old_proto::convert(ir_program)
        .map_err(|err| anyhow::anyhow!("convert IR to AST: {err}"))?;

    let (executable, metadata) = waymark_vm_compiler_for_ast_old::compile_with_metadata::<
        waymark_system_vm::Spec,
        waymark_system_vm::Lowering,
    >(&ast_program)
    .map_err(|err| anyhow::anyhow!("compile: {err}"))?;

    let executable = std::sync::Arc::new(executable);
    let interpreter = waymark_system_vm::Interpreter::default();

    let call_spec =
        waymark_workflow_initialization_convert_proto::InitialContextConverter::try_convert((
            registration.initial_context.as_ref(),
            &metadata,
        ))?;

    let runtime =
        waymark_system_vm::Runtime::with_custom_entrypoint(interpreter, executable, call_spec)?;
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
/// Wires the [`waymark_execution_effector`] with
/// [`waymark_action_runtime_worker_stream`] channels, spawns the VM driver
/// on a dedicated OS thread, and spawns a background task that awaits the
/// workflow outcome (delivered via a oneshot channel) and emits it on `out_rx`.
///
/// The caller must feed [`proto::ActionResult`]s into
/// [`ExecuteChannels::action_result_tx`] (typically by forwarding them from
/// the gRPC bidir input stream) and convert
/// [`ExecuteChannels::out_rx`] into the gRPC response stream.
///
/// When `skip_sleep` is true, every sleep in the workflow resolves
/// immediately instead of waiting for its deadline.
pub fn execute(runtime: waymark_system_vm::Runtime, skip_sleep: bool) -> ExecuteChannels {
    let codec = waymark_vm_codec_rmp::RmpCodec;

    let (out_tx, out_rx) = mpsc::channel::<Result<proto::WorkflowStreamResponse, Status>>(32);
    let (action_result_tx, action_result_rx) = mpsc::channel::<proto::ActionResult>(32);
    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();

    let requester = waymark_action_runtime_worker_stream::WorkerStreamActionRequester::<
        ActionCallCorrelation,
    >::new(out_tx.clone());
    let provider = waymark_action_runtime_worker_stream::WorkerStreamActionCallCompletionsProvider::<
        ActionCallCorrelation,
    >::new(action_result_rx);

    let action_handler = waymark_extcall_reconciler_action_compat::EffectHandler::new(requester);
    let action_poller = waymark_extcall_reconciler_action_compat::PromiseSettler::new(provider);
    let (sleep_handler, sleep_poller) =
        waymark_sleep_reconciler::new::<waymark_sleep_compat::ReadyValueSleepProvider>(skip_sleep);
    let (extcall_handler, extcall_settler) =
        waymark_extcall_reconciler::new(action_handler, sleep_handler, action_poller, sleep_poller);
    let completion_handler = waymark_workflow_completion_direct::DirectHandler::new(completion_tx);

    let handler = waymark_fullset_effect_handler::EffectHandler {
        core: completion_handler,
        extcall: extcall_handler,
    };

    let effector = (handler, extcall_settler);

    let driver_handle = waymark_vm_driver_thread::spawn(waymark_vm_driver::Params {
        runtime,
        effector,
        persister: NoopPersister,
        codec,
        cancel: tokio_util::sync::CancellationToken::new(),
    });

    // Wait for the driver to finish, then await the completion outcome
    // and forward it to out_tx.
    tokio::spawn(async move {
        let Err(err) = driver_handle.await;
        tracing::warn!(?err, "vm driver exited");

        let response = match completion_rx.await {
            Ok(outcome) => convert_outcome_to_stream_response(outcome),
            Err(_recv_error) => {
                tracing::error!("completion sender dropped without sending outcome");
                Err(Status::internal(
                    "workflow driver exited without recording a result",
                ))
            }
        };

        let _ = out_tx.send(response).await;
    });

    ExecuteChannels {
        out_rx,
        action_result_tx,
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Convert a typed [`Outcome`] to a [`proto::WorkflowStreamResponse`].
///
/// A conversion failure is surfaced to the caller as an error [`Status`]
/// rather than being masked as an empty payload.
#[allow(
    clippy::result_large_err,
    reason = "the outcome feeds a tonic::Status gRPC response stream"
)]
fn convert_outcome_to_stream_response(
    outcome: Outcome<waymark_system_vm::ReadyValue>,
) -> Result<proto::WorkflowStreamResponse, Status> {
    let payload = waymark_workflow_completion_convert_proto::Converter::try_convert(outcome)
        .map_err(|err| Status::internal(format!("convert workflow outcome: {err}")))?
        .encode_to_vec();

    Ok(proto::WorkflowStreamResponse {
        kind: Some(proto::workflow_stream_response::Kind::WorkflowResult(
            proto::WorkflowExecutionResult { payload },
        )),
    })
}

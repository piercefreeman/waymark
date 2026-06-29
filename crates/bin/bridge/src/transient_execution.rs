//! In-memory transient workflow execution — runs a workflow in-process,
//! streaming action dispatches and collecting results via gRPC streams.
//!
//! Uses [`waymark_vm_driver_thread`] to run the VM on a dedicated OS thread,
//! and bridges VM effects/promise-settlements to/from the gRPC bidir stream.
//!
//! # Status: Temporary
//!
//! This module is a temporary bridge until the execute-workflow path is
//! properly refactored to use the workers' action execution interface.
//! Currently, it runs the VM driver on a dedicated OS thread and bridges
//! effects/promise-settlements to/from the gRPC bidir stream directly.
//!
//! The long-term plan is to route action dispatches through the standard
//! worker pool and action-result ingestion pipeline, removing the need for
//! this ad-hoc streaming effector.

use std::sync::{Arc, Mutex};

use prost::Message as _;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use waymark_action_runtime_core::ActionCallRequester as _;
use waymark_convert_core::TryConvert as _;
use waymark_proto::messages as proto;
use waymark_vm_driver_core::{
    EffectHandler, PromiseResolution, PromiseSettlement, SnapshotPersister,
};
use waymark_vm_runtime_promise_core::PromiseStateId;

// ---------------------------------------------------------------------------
// Streaming effector — bridges the VM driver to the gRPC bidir stream
// ---------------------------------------------------------------------------

/// Mutable state shared between the [`StreamEffectHandler`] and the
/// [`settlement_feeder`].
#[derive(Default)]
pub struct SharedState {
    /// The [`PromiseStateId`] of the most recently dispatched action.
    ///
    /// Written by [`StreamEffectHandler`] when an `ActionCall` effect is
    /// emitted, read by [`settlement_feeder`] to tag the corresponding
    /// [`PromiseSettlement`].
    pub last_promise_state_id: Option<PromiseStateId>,

    /// When `true`, [`ExtEffect::Sleep`] promises are resolved immediately
    /// instead of waiting for the client to send a settlement.
    ///
    /// Set from the `skip_sleep` flag on the initial
    /// [`proto::WorkflowStreamRequest`] (enabled under pytest).
    pub skip_sleep: bool,
}

/// Handles VM effects by converting them to [`proto::WorkflowStreamResponse`]
/// messages and sending them through the output channel.
pub struct StreamEffectHandler {
    action_requester: waymark_action_runtime_worker_stream::WorkerStreamActionRequester,
    tx: mpsc::Sender<Result<proto::WorkflowStreamResponse, Status>>,
    settlement_tx: mpsc::Sender<PromiseSettlement<waymark_system_vm::ReadyValue, ()>>,
    cancel: CancellationToken,
    shared: Arc<Mutex<SharedState>>,
}

impl StreamEffectHandler {
    pub fn new(
        tx: mpsc::Sender<Result<proto::WorkflowStreamResponse, Status>>,
        settlement_tx: mpsc::Sender<PromiseSettlement<waymark_system_vm::ReadyValue, ()>>,
        cancel: CancellationToken,
        shared: Arc<Mutex<SharedState>>,
    ) -> Self {
        Self {
            action_requester: waymark_action_runtime_worker_stream::WorkerStreamActionRequester {
                tx: tx.clone(),
                instance_id: String::new(),
            },
            tx,
            settlement_tx,
            cancel,
            shared,
        }
    }
}

impl EffectHandler for StreamEffectHandler {
    type Effect = <waymark_system_vm::Interpreter as waymark_vm_interpreter::Interpreter>::Effect;
    type Error = mpsc::error::SendError<Result<proto::WorkflowStreamResponse, Status>>;

    async fn handle_effect(
        &mut self,
        emitted_effect: waymark_vm_runtime_effect::EmittedEffect<Self::Effect>,
    ) -> Result<(), Self::Error> {
        use waymark_vm_interpreter_coreset::Effect as CoreEffect;
        use waymark_vm_interpreter_extcallset::Effect as ExtEffect;
        use waymark_vm_interpreter_fullset::Effect;

        let waymark_vm_runtime_effect::EmittedEffect {
            effect,
            number: effect_number,
        } = emitted_effect;

        match effect {
            Effect::ExtCallSet(ExtEffect::ActionCall {
                action_ref,
                promise_state_id,
                args,
            }) => {
                self.action_requester
                    .request_action_call(waymark_action_runtime_core::ActionCallRequest {
                        action_ref,
                        effect_number,
                        promise_state_id,
                        arguments: args,
                    })
                    .await
            }
            Effect::ExtCallSet(ExtEffect::Sleep {
                promise_state_id, ..
            }) => {
                if self.shared.lock().unwrap().skip_sleep {
                    // Resolve the sleep promise immediately.
                    let _ = self
                        .settlement_tx
                        .send(PromiseSettlement {
                            promise_state_id,
                            resolution: PromiseResolution::Resolved(
                                waymark_system_vm::ReadyValue::None,
                            ),
                            ack: (),
                        })
                        .await;
                }
                Ok(())
            }
            Effect::CoreSet(CoreEffect::Complete(value)) => {
                let arguments =
                    waymark_workflow_completion_convert_proto::Converter::try_convert(value)
                        .map_err(|err| {
                            mpsc::error::SendError(Err(Status::internal(format!(
                                "failed to convert completion value: {err}"
                            ))))
                        })?;
                let payload = arguments.encode_to_vec();
                let response = proto::WorkflowStreamResponse {
                    kind: Some(proto::workflow_stream_response::Kind::WorkflowResult(
                        proto::WorkflowExecutionResult { payload },
                    )),
                };
                let _ = self.tx.send(Ok(response)).await;
                self.cancel.cancel();
                Ok(())
            }
            Effect::CoreSet(CoreEffect::UnhandledException(exception)) => {
                let arguments =
                    waymark_workflow_completion_convert_proto::Converter::try_convert(exception)
                        .map_err(|err| {
                            mpsc::error::SendError(Err(Status::internal(format!(
                                "failed to convert exception: {err}"
                            ))))
                        })?;
                let payload = arguments.encode_to_vec();
                let _ = self
                    .tx
                    .send(Ok(proto::WorkflowStreamResponse {
                        kind: Some(proto::workflow_stream_response::Kind::WorkflowResult(
                            proto::WorkflowExecutionResult { payload },
                        )),
                    }))
                    .await;
                self.cancel.cancel();
                Ok(())
            }
            Effect::PureSet(infallible) => match infallible {},
        }
    }
}

// ---------------------------------------------------------------------------
// No-op snapshot persister (transient execution does not persist state)
// ---------------------------------------------------------------------------

pub struct NoopPersister;

impl SnapshotPersister for NoopPersister {
    type Error = core::convert::Infallible;

    async fn persist_snapshot(&self, _snapshot: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// gRPC input stream reader — feeds action results as promise settlements
// ---------------------------------------------------------------------------

/// Read [`proto::WorkflowStreamRequest`] messages from the gRPC input stream
/// and convert [`proto::ActionResult`] variants into promise settlements,
/// sending them to the driver via the settlement channel.
///
/// The promise state ID is read from the shared state, which is updated by
/// the effect handler whenever an `ActionCall` effect is emitted.
pub async fn settlement_feeder(
    in_stream: &mut tonic::Streaming<proto::WorkflowStreamRequest>,
    settlement_tx: &mpsc::Sender<PromiseSettlement<waymark_system_vm::ReadyValue, ()>>,
    shared: Arc<Mutex<SharedState>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let msg = in_stream.message().await?.ok_or("input stream closed")?;

        let action_result = match msg.kind {
            Some(proto::workflow_stream_request::Kind::ActionResult(result)) => result,
            // Ignore any non-action-result messages (e.g. a repeated
            // registration, which should not happen but is harmless).
            _ => continue,
        };

        let outcome = waymark_action_runtime_convert::Converter::try_convert(&action_result)?;

        let resolution = match outcome {
            waymark_action_runtime_core::ActionCallOutcome::Value(value) => {
                PromiseResolution::Resolved(value)
            }
            waymark_action_runtime_core::ActionCallOutcome::Exception(exception) => {
                PromiseResolution::Rejected(exception)
            }
        };

        // Correlate with the most recently emitted ActionCall.
        let promise_state_id = shared
            .lock()
            .unwrap()
            .last_promise_state_id
            .unwrap_or(PromiseStateId(0));

        let settlement = PromiseSettlement {
            promise_state_id,
            resolution,
            ack: (),
        };

        if settlement_tx.send(settlement).await.is_err() {
            // Driver has shut down; stop feeding.
            return Ok(());
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Compile a workflow registration and create a [`SystemRuntime`] entirely
/// in memory, without any database backend.
///
/// The entry function (function 0, the first function in source order)
/// receives its arguments from [`proto::WorkflowRegistration::initial_context`]
/// when present.  Each keyword argument is mapped to a positional argument
/// by matching the entry function's input names.
pub async fn setup_runtime(
    registration: &proto::WorkflowRegistration,
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

    let executable = Arc::new(executable);
    let interpreter = waymark_system_vm::Interpreter::default();

    let entry_input_names: Vec<String> = metadata
        .input_names(Default::default())
        .map(<[String]>::to_vec)
        .unwrap_or_default();

    let call_spec = crate::utils::build_entry_call_spec(
        registration.initial_context.clone(),
        &entry_input_names,
    )?;

    let runtime =
        waymark_system_vm::Runtime::with_custom_entrypoint(interpreter, executable, call_spec)?;
    Ok(runtime)
}

type Effector = (
    StreamEffectHandler,
    mpsc::Receiver<PromiseSettlement<waymark_system_vm::ReadyValue, ()>>,
);

/// Spawn the VM driver on a dedicated OS thread via
/// [`waymark_vm_driver_thread::spawn`].
///
/// Returns a join handle for the driver thread. The driver runs until
/// cancelled (by workflow completion, unhandled exception, or external
/// cancellation), or until a fatal error occurs.
pub fn spawn_driver(
    runtime: waymark_system_vm::Runtime,
    out_tx: mpsc::Sender<Result<proto::WorkflowStreamResponse, Status>>,
    settlement_rx: mpsc::Receiver<PromiseSettlement<waymark_system_vm::ReadyValue, ()>>,
    settlement_tx: mpsc::Sender<PromiseSettlement<waymark_system_vm::ReadyValue, ()>>,
    shared: Arc<Mutex<SharedState>>,
) -> waymark_vm_driver_thread::HandleFor<
    waymark_system_vm::Value,
    waymark_system_vm::Interpreter,
    waymark_vm_codec_rmp::RmpCodec,
    NoopPersister,
    Effector,
> {
    let cancel = CancellationToken::new();
    let handler =
        StreamEffectHandler::new(out_tx, settlement_tx, cancel.clone(), Arc::clone(&shared));
    let persister = NoopPersister;
    let codec = waymark_vm_codec_rmp::RmpCodec;

    waymark_vm_driver_thread::spawn(waymark_vm_driver::Params {
        runtime,
        effector: (handler, settlement_rx),
        persister,
        codec,
        cancel,
    })
}

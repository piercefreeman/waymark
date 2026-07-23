//! In-memory transient workflow execution bringup — assembles the VM
//! effect-reconciler stack over a caller-provided action transport and
//! spawns the VM driver on a dedicated OS thread.
//!
//! This is the bringup counterpart to [`waymark_execution_bringup`] for
//! transient execution: no snapshot persistence and no backend — the
//! workflow outcome is delivered in-process.

#![warn(missing_docs)]

use waymark_vm_driver_core::SnapshotPersister;

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

/// Errors that can occur in [`setup_runtime`].
#[derive(Debug, thiserror::Error)]
pub enum SetupRuntimeError {
    /// Compiling the AST program into an executable failed.
    #[error("compile: {0}")]
    Compile(
        #[source]
        waymark_vm_compiler_for_ast_old::CompileErrorFor<
            waymark_system_vm::Spec,
            waymark_system_vm::Lowering,
        >,
    ),

    /// Selecting the entry function failed.
    #[error("select entry function: {0}")]
    SelectEntryFunction(#[source] waymark_vm_runtime_builder::NoFunctionsError),

    /// Matching the entry function arguments failed.
    #[error("match entry function arguments: {0}")]
    MatchArguments(#[source] waymark_vm_runtime_builder::MissingArgumentsError),

    /// The entry function was not found in the executable.
    #[error("invalid entrypoint: {0}")]
    Entrypoint(
        #[source] waymark_vm_runtime::FunctionNotFoundError<waymark_vm_bytecode_core::FunctionId>,
    ),
}

/// Compile a [`waymark_vm_ast_old::Program`] into a ready-to-run
/// [`waymark_system_vm::Runtime`], entirely in memory, without any database
/// backend.
///
/// The entry function (function 0, the first function in source order)
/// receives its arguments from `arguments`, matched by input name; every
/// input name must be present.
pub fn setup_runtime(
    program: &waymark_vm_ast_old::Program,
    arguments: std::collections::HashMap<String, waymark_system_vm::Value>,
) -> Result<waymark_system_vm::Runtime, SetupRuntimeError> {
    let (executable, metadata) = waymark_vm_compiler_for_ast_old::compile_with_metadata::<
        waymark_system_vm::Spec,
        waymark_system_vm::Lowering,
    >(program)
    .map_err(SetupRuntimeError::Compile)?;
    let executable = std::sync::Arc::new(executable);

    let call_spec = waymark_vm_runtime_builder::builder(&metadata)
        .first_fn()
        .map_err(SetupRuntimeError::SelectEntryFunction)?
        .args(arguments)
        .map_err(SetupRuntimeError::MatchArguments)?;

    let interpreter = waymark_system_vm::Interpreter::default();
    let runtime =
        waymark_system_vm::Runtime::with_custom_entrypoint(interpreter, executable, call_spec)
            .map_err(SetupRuntimeError::Entrypoint)?;
    Ok(runtime)
}

// ---------------------------------------------------------------------------
// Execution bringup
// ---------------------------------------------------------------------------

/// The effector assembled by [`execute_with`]: the workflow completion is
/// delivered in-process through a oneshot channel, action calls go through
/// the caller-provided transport, and sleeps are handled by the transient
/// sleep reconciler.
pub type EffectorFor<ActionCallRequester, ActionCallCompletionsProvider> = (
    waymark_fullset_effect_handler::EffectHandler<
        waymark_workflow_completion_direct::DirectHandler<waymark_system_vm::ReadyValue>,
        waymark_extcall_reconciler::EffectHandler<
            waymark_extcall_reconciler_action_compat::EffectHandler<ActionCallRequester>,
            waymark_transient_sleep_reconciler::Handler,
        >,
    >,
    waymark_extcall_reconciler::PromiseSettler<
        waymark_extcall_reconciler_action_compat::PromiseSettler<ActionCallCompletionsProvider>,
        waymark_transient_sleep_reconciler::Poller<waymark_sleep_compat::ReadyValueSleepProvider>,
    >,
);

/// The VM driver thread handle type produced by [`execute_with`] for
/// the given action transport.
pub type DriverHandleFor<ActionCallRequester, ActionCallCompletionsProvider> =
    waymark_vm_driver_thread::HandleFor<
        waymark_system_vm::Interpreter,
        waymark_vm_codec_rmp::RmpCodec,
        NoopPersister,
        EffectorFor<ActionCallRequester, ActionCallCompletionsProvider>,
    >;

/// A launched transient workflow execution.
pub struct Execution<DriverHandle> {
    /// Resolves with the workflow outcome when the workflow completes.
    ///
    /// If this resolves with a receive error, the driver terminated before
    /// producing a workflow outcome — await [`Self::driver_handle`] for the
    /// terminal error.
    pub workflow_outcome_rx: tokio::sync::oneshot::Receiver<
        waymark_workflow_completion_core::Outcome<waymark_system_vm::ReadyValue>,
    >,

    /// Join handle for the VM driver thread.
    ///
    /// The driver loop only ever terminates with an error — including after
    /// a successful workflow completion — so awaiting this handle always
    /// produces an `Err`.
    pub driver_handle: DriverHandle,
}

/// Wire up and launch transient workflow execution for the given runtime
/// over a caller-provided action transport.
///
/// Assembles the effect-reconciler stack (action calls via
/// `action_call_requester` / `action_call_completions_provider`, sleeps via
/// the transient sleep reconciler, workflow completion via a oneshot
/// channel) and spawns the VM driver on a dedicated OS thread.
///
/// When `skip_sleep` is true, every sleep in the workflow resolves
/// immediately instead of waiting for its deadline.
///
/// Cancelling `cancel` requests the driver loop to stop.
pub fn execute_with<ActionCallRequester, ActionCallCompletionsProvider>(
    runtime: waymark_system_vm::Runtime,
    action_call_requester: ActionCallRequester,
    action_call_completions_provider: ActionCallCompletionsProvider,
    skip_sleep: bool,
    cancel: tokio_util::sync::CancellationToken,
) -> Execution<DriverHandleFor<ActionCallRequester, ActionCallCompletionsProvider>>
where
    ActionCallRequester: waymark_action_runtime_core::ActionCallRequester<
            Argument = waymark_system_vm::ReadyValue,
            Metadata = waymark_action_runtime_metadata::ActionCallCorrelation,
        >,
    ActionCallRequester: Send + Sync + 'static,
    ActionCallRequester::Error: Send,
    ActionCallCompletionsProvider: waymark_action_runtime_core::ActionCallCompletionsProvider<
            Value = waymark_system_vm::ReadyValue,
            Metadata = waymark_action_runtime_metadata::ActionCallCorrelation,
        >,
    ActionCallCompletionsProvider: Send + Sync + 'static,
    ActionCallCompletionsProvider::Error: Send,
{
    let codec = waymark_vm_codec_rmp::RmpCodec;

    let (workflow_outcome_tx, workflow_outcome_rx) = tokio::sync::oneshot::channel();

    let action_handler =
        waymark_extcall_reconciler_action_compat::EffectHandler::new(action_call_requester);
    let action_poller = waymark_extcall_reconciler_action_compat::PromiseSettler::new(
        action_call_completions_provider,
    );
    let (sleep_handler, sleep_poller) = waymark_transient_sleep_reconciler::new::<
        waymark_sleep_compat::ReadyValueSleepProvider,
    >(skip_sleep);
    let (extcall_handler, extcall_settler) =
        waymark_extcall_reconciler::new(action_handler, sleep_handler, action_poller, sleep_poller);
    let workflow_completion_handler =
        waymark_workflow_completion_direct::DirectHandler::new(workflow_outcome_tx);

    let handler = waymark_fullset_effect_handler::EffectHandler {
        core: workflow_completion_handler,
        extcall: extcall_handler,
    };

    let effector = (handler, extcall_settler);

    let driver_handle = waymark_vm_driver_thread::spawn(waymark_vm_driver::Params {
        runtime,
        effector,
        persister: NoopPersister,
        codec,
        cancel,
    });

    Execution {
        workflow_outcome_rx,
        driver_handle,
    }
}

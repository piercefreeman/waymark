//! Effector for the fullset interpreter.
//!
//! Bridges the [`waymark_vm_interpreter_fullset::FullSetInterpreter`]'s effects
//! with external systems — dispatching extcalls to a worker pool, tracking sleep
//! deadlines, and recording core completion effects via
//! [`waymark_workflow_completion`].
//!
//! # Architecture
//!
//! The fullset effector decomposes the three-variant
//! [`waymark_vm_interpreter_fullset::Effect`] into:
//!
//! - **Core effects** ([`waymark_vm_interpreter_coreset::Effect`]): delegated
//!   to [`waymark_workflow_completion::EffectHandler`] for backend persistence.
//! - **Extcall effects** ([`waymark_vm_interpreter_extcallset::Effect`]):
//!   delegated to [`waymark_extcall_reconciler`] for dispatch to a worker pool
//!   and sleep tracking.
//! - **Pure effects** (`Infallible`): statically unreachable.

#![warn(missing_docs)]

use std::marker::PhantomData;
use std::sync::Arc;

use nonempty_collections::NEVec;
use waymark_action_core::ActionRef;
use waymark_vm_interpreter_fullset::Effect as FullSetEffect;
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_worker_core::BaseWorkerPool;

/// Re-export the acknowledgement type from the extcall reconciler.
pub use waymark_extcall_reconciler::Ack;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Error returned when handling a fullset effect fails.
#[derive(Debug, thiserror::Error)]
pub enum HandleEffectError<
    ConvertArgError,
    CodecError,
    BackendCompletionError,
    BackendExceptionError,
> {
    /// An extcall effect handling error (action dispatch failure).
    #[error("extcall effect handling failed: {0}")]
    ExtCall(#[from] waymark_extcall_reconciler::HandleEffectError<ConvertArgError>),

    /// A core-effect (completion) handling error.
    #[error("core-effect handling failed: {0}")]
    Completion(
        #[from]
        waymark_workflow_completion::HandleEffectError<
            CodecError,
            BackendCompletionError,
            BackendExceptionError,
        >,
    ),
}

/// Error returned when there are no pending promises to settle.
#[derive(Debug, thiserror::Error)]
pub enum NoSettlementsError {
    /// No pending actions or sleeps remain — nothing to wait for.
    #[error("no pending promises to settle")]
    NoPendingPromises,
}

// ---------------------------------------------------------------------------
// Effect handler
// ---------------------------------------------------------------------------

/// Handles [`FullSetEffect`] values emitted by the VM driver.
///
/// - Core effects (`Complete`, `UnhandledException`) are delegated to a
///   [`waymark_workflow_completion::EffectHandler`] for backend persistence.
/// - Extcall effects (`ActionCall`, `Sleep`) are delegated to the
///   wrapped [`waymark_extcall_reconciler::EffectHandler`].
/// - Pure effects are statically impossible.
pub struct EffectHandler<Backend, WorkerPool, Converter, Codec, ActionCallArgument, CompletionValue>
where
    Backend: waymark_workflow_completion_backend::HasVmId,
{
    /// Handles core effects (completion / unhandled exception).
    completion: waymark_workflow_completion::EffectHandler<Backend, Codec, CompletionValue>,

    /// Handles extcall effects (action dispatch and sleep recording).
    extcall: waymark_extcall_reconciler::EffectHandler<
        Backend::VmId,
        WorkerPool,
        Converter,
        ActionCallArgument,
    >,
}

impl<Backend, WorkerPool, Converter, Codec, ActionCallArgument, CompletionValue>
    EffectHandler<Backend, WorkerPool, Converter, Codec, ActionCallArgument, CompletionValue>
where
    Backend: waymark_workflow_completion_backend::HasVmId,
{
    /// Create an effect handler from already-initialized sub-handlers.
    pub fn new(
        completion: waymark_workflow_completion::EffectHandler<Backend, Codec, CompletionValue>,
        extcall: waymark_extcall_reconciler::EffectHandler<
            Backend::VmId,
            WorkerPool,
            Converter,
            ActionCallArgument,
        >,
    ) -> Self {
        Self {
            completion,
            extcall,
        }
    }
}

impl<Backend, WorkerPool, Converter, Codec, ActionCallArgument, CompletionValue>
    waymark_vm_driver_core::EffectHandler
    for EffectHandler<Backend, WorkerPool, Converter, Codec, ActionCallArgument, CompletionValue>
where
    Backend: waymark_workflow_completion_backend::RecordCompletion,
    Backend: waymark_workflow_completion_backend::RecordException,
    Backend: Send + Sync,
    Backend::VmId: Clone + Send,
    WorkerPool: BaseWorkerPool + Send + Sync,
    Converter: waymark_convert_core::TryConvert<
            ActionCallArgument,
            serde_json::Value,
            Error: core::fmt::Debug,
        > + Send,
    Codec: waymark_vm_codec_core::SerializerProvider + Send,
    ActionCallArgument: Send,
    CompletionValue: serde::Serialize + Send,
    waymark_vm_runtime_exception::Exception<CompletionValue>: serde::Serialize,
{
    type Effect = FullSetEffect<CompletionValue, ActionRef, ActionCallArgument>;
    type Error = HandleEffectError<
        Converter::Error,
        Codec::Error,
        <Backend as waymark_workflow_completion_backend::RecordCompletion>::Error,
        <Backend as waymark_workflow_completion_backend::RecordException>::Error,
    >;

    async fn handle_effect(
        &mut self,
        emitted_effect: waymark_vm_runtime_effect::EmittedEffect<Self::Effect>,
    ) -> Result<(), Self::Error> {
        let waymark_vm_runtime_effect::EmittedEffect { effect, number } = emitted_effect;
        match effect {
            FullSetEffect::CoreSet(core_effect) => self
                .completion
                .handle_effect(waymark_vm_runtime_effect::EmittedEffect {
                    effect: core_effect,
                    number,
                })
                .await
                .map_err(HandleEffectError::Completion),
            FullSetEffect::ExtCallSet(extcall_effect) => self
                .extcall
                .handle_effect(waymark_vm_runtime_effect::EmittedEffect {
                    effect: extcall_effect,
                    number,
                })
                .await
                .map_err(HandleEffectError::ExtCall),
            FullSetEffect::PureSet(infallible) => match infallible {},
        }
    }
}

// ---------------------------------------------------------------------------
// Promise settler
// ---------------------------------------------------------------------------

/// Produces promise settlements for the fullset interpreter.
///
/// Delegates to the wrapped [`waymark_extcall_reconciler::PromiseSettler`] for
/// both action-call completions and elapsed sleep deadlines.
pub struct PromiseSettler<VmId, WorkerPool, Converter, PromiseValue> {
    /// Polls for completed action calls and elapsed sleep deadlines.
    extcall: waymark_extcall_reconciler::PromiseSettler<VmId, WorkerPool, Converter, PromiseValue>,
}

impl<VmId, WorkerPool, Converter, PromiseValue>
    PromiseSettler<VmId, WorkerPool, Converter, PromiseValue>
{
    /// Create a promise settler from an already-initialized extcall settler.
    pub fn new(
        extcall: waymark_extcall_reconciler::PromiseSettler<
            VmId,
            WorkerPool,
            Converter,
            PromiseValue,
        >,
    ) -> Self {
        Self { extcall }
    }
}

impl<VmId, WorkerPool, Converter, PromiseValue> waymark_vm_driver_core::PromiseSettler
    for PromiseSettler<VmId, WorkerPool, Converter, PromiseValue>
where
    VmId: Send + core::fmt::Debug,
    WorkerPool: BaseWorkerPool + Send + Sync,
    Converter: waymark_convert_core::Convert<serde_json::Value, PromiseValue> + Send,
    Converter: waymark_convert_core::Convert<
            serde_json::Value,
            waymark_vm_runtime_exception::Exception<PromiseValue>,
        > + Send,
    PromiseValue: Send,
{
    type Value = PromiseValue;
    type Error = NoSettlementsError;
    type Ack = Ack;

    async fn get_promise_settlements(
        &mut self,
        waiting_ids: NEVec<PromiseStateId>,
    ) -> Result<NEVec<waymark_vm_driver_core::PromiseSettlement<PromiseValue, Ack>>, Self::Error>
    {
        self.extcall
            .get_promise_settlements(waiting_ids)
            .await
            .map_err(|_: waymark_extcall_reconciler::NoSettlementsError| {
                NoSettlementsError::NoPendingPromises
            })
    }
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

/// Create a paired fullset effect handler and promise settler.
///
/// The returned handler and settler can be combined into a tuple
/// `(handler, settler)` that satisfies both
/// [`waymark_vm_driver_core::EffectHandler`] and
/// [`waymark_vm_driver_core::PromiseSettler`], suitable for passing as the
/// effector to [`waymark_state_vm_runtimes::SpawningFactory`].
///
/// Core effects (`Complete`, `UnhandledException`) are recorded via the
/// provided `backend` under the given `vm_id`.
#[expect(clippy::type_complexity)]
pub fn new<
    Backend,
    WorkerPool,
    Converter,
    Codec,
    ActionCallArgument,
    CompletionValue,
    PromiseValue,
>(
    vm_id: Backend::VmId,
    backend: Arc<Backend>,
    worker_pool: WorkerPool,
    codec: Codec,
) -> (
    EffectHandler<Backend, WorkerPool, Converter, Codec, ActionCallArgument, CompletionValue>,
    PromiseSettler<Backend::VmId, WorkerPool, Converter, PromiseValue>,
)
where
    Backend: waymark_workflow_completion_backend::RecordCompletion,
    Backend: waymark_workflow_completion_backend::RecordException,
    Backend: Send + Sync + 'static,
    Backend::VmId: Clone + Send + 'static,
    Codec: Send + 'static,
    WorkerPool: BaseWorkerPool + Send + Sync + 'static,
{
    let (extcall_handler, extcall_settler) =
        waymark_extcall_reconciler::new(vm_id.clone(), worker_pool);
    let completion_handler = waymark_workflow_completion::EffectHandler::new(backend, vm_id, codec);

    let handler = EffectHandler::new(completion_handler, extcall_handler);
    let settler = PromiseSettler::new(extcall_settler);

    (handler, settler)
}

// ---------------------------------------------------------------------------
// Effector provider
// ---------------------------------------------------------------------------

/// An [`waymark_state_vm_runtimes_core::EffectorProvider`] that creates
/// per-VM fullset effectors.
///
/// Holds shared backend and worker-pool references. Each call to
/// [`provide_effector`](waymark_state_vm_runtimes_core::EffectorProvider::provide_effector)
/// creates a fresh effector pair bound to the given VM identifier.
pub struct EffectorProvider<
    Backend,
    WorkerPool,
    Converter,
    Codec,
    ActionCallArgument,
    CompletionValue,
    PromiseValue,
> {
    backend: Arc<Backend>,
    worker_pool: WorkerPool,
    codec: Codec,
    _phantom: PhantomData<(Converter, ActionCallArgument, CompletionValue, PromiseValue)>,
}

impl<Backend, WorkerPool, Converter, Codec, ActionCallArgument, CompletionValue, PromiseValue>
    EffectorProvider<
        Backend,
        WorkerPool,
        Converter,
        Codec,
        ActionCallArgument,
        CompletionValue,
        PromiseValue,
    >
{
    /// Create a new effector provider.
    pub fn new(backend: Arc<Backend>, worker_pool: WorkerPool, codec: Codec) -> Self {
        Self {
            backend,
            worker_pool,
            codec,
            _phantom: PhantomData,
        }
    }
}

impl<Backend, WorkerPool, Converter, Codec, ActionCallArgument, CompletionValue, PromiseValue>
    waymark_state_vm_runtimes_core::EffectorProvider
    for EffectorProvider<
        Backend,
        WorkerPool,
        Converter,
        Codec,
        ActionCallArgument,
        CompletionValue,
        PromiseValue,
    >
where
    Backend: waymark_workflow_completion_backend::RecordCompletion,
    Backend: waymark_workflow_completion_backend::RecordException,
    Backend: Send + Sync + 'static,
    Backend::VmId: Clone + Send + Sync + 'static,
    WorkerPool: BaseWorkerPool + Clone + Send + Sync + 'static,
    Codec: waymark_vm_codec_core::SerializerProvider + Clone + Send + 'static,
    Converter: waymark_convert_core::TryConvert<
            ActionCallArgument,
            serde_json::Value,
            Error: core::fmt::Debug,
        > + Send
        + 'static,
    Converter: waymark_convert_core::Convert<serde_json::Value, PromiseValue> + Send + 'static,
    Converter: waymark_convert_core::Convert<
            serde_json::Value,
            waymark_vm_runtime_exception::Exception<PromiseValue>,
        > + Send
        + 'static,
    ActionCallArgument: Send + 'static,
    CompletionValue: Send + 'static,
    PromiseValue: Send + 'static,
{
    type VmId = Backend::VmId;
    type Effector = (
        EffectHandler<Backend, WorkerPool, Converter, Codec, ActionCallArgument, CompletionValue>,
        PromiseSettler<Backend::VmId, WorkerPool, Converter, PromiseValue>,
    );

    fn provide_effector(&self, vm_id: &Self::VmId) -> Self::Effector {
        new(
            vm_id.clone(),
            Arc::clone(&self.backend),
            self.worker_pool.clone(),
            self.codec.clone(),
        )
    }
}

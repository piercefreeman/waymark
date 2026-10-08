//! Transient workflow execution over the worker-pool action transport.
//!
//! Instantiates [`waymark_transient_execution_bringup::execute_with`] with
//! [`waymark_action_runtime_worker_pool`] as the action transport: action
//! calls are dispatched to a
//! [`waymark_worker_core::QueueActionDispatch`] and
//! action call completions are polled from a
//! [`waymark_worker_core::PollActionResults`].

#![warn(missing_docs)]

/// The action call requester [`execute`] instantiates over the given
/// worker pool requests handle.
pub type ActionCallRequesterFor<WorkerPoolRequests> =
    waymark_action_runtime_worker_pool::WorkerPoolActionRequester<
        WorkerPoolRequests,
        waymark_action_runtime_metadata::ActionCallCorrelation,
        waymark_vm_value_python::ReadyValue,
        waymark_vm_value_python_convert_proto::ActionArgumentsConverter,
    >;

/// The action call completions provider [`execute`] instantiates over the
/// given worker pool completions handle.
pub type ActionCallCompletionsProviderFor<WorkerPoolCompletions> =
    waymark_action_runtime_worker_pool::WorkerPoolActionCallCompletionsProvider<
        WorkerPoolCompletions,
        waymark_action_runtime_metadata::ActionCallCorrelation,
        waymark_vm_value_python::ReadyValue,
        waymark_vm_value_python::RaisedException,
        waymark_vm_value_python_convert_proto::ActionOutcomeConverter,
    >;

/// The [`waymark_transient_execution_bringup::Execution`] type produced by
/// [`execute`] for the given worker pool.
pub type ExecutionFor<WorkerPoolRequests, WorkerPoolCompletions> =
    waymark_transient_execution_bringup::Execution<
        waymark_transient_execution_bringup::DriverHandleFor<
            ActionCallRequesterFor<WorkerPoolRequests>,
            ActionCallCompletionsProviderFor<WorkerPoolCompletions>,
        >,
    >;

/// Wire up and launch transient workflow execution for the given runtime
/// over the worker-pool action transport.
///
/// Action calls are dispatched to `worker_pool_requests` and action call
/// completions are polled from `worker_pool_completions`, with the
/// correlation metadata round-tripped verbatim — there is no per-VM
/// demultiplexing, so this is suitable for running a single VM at a time.
///
/// When `skip_sleep` is true, every sleep in the workflow resolves
/// immediately instead of waiting for its deadline.
///
/// Cancelling `cancel` requests the driver loop to stop; `hooks` observe
/// the driver's run.
pub fn execute<WorkerPoolRequests, WorkerPoolCompletions, Hooks>(
    runtime: waymark_system_vm::Runtime,
    worker_pool_requests: WorkerPoolRequests,
    worker_pool_completions: WorkerPoolCompletions,
    skip_sleep: bool,
    cancel: tokio_util::sync::CancellationToken,
    hooks: Hooks,
) -> ExecutionFor<WorkerPoolRequests, WorkerPoolCompletions>
where
    WorkerPoolRequests: waymark_worker_core::QueueActionDispatch + Send + Sync + 'static,
    WorkerPoolRequests::Error: core::fmt::Debug + Send + 'static,
    WorkerPoolCompletions: waymark_worker_core::PollActionResults + Send + Sync + 'static,
    WorkerPoolCompletions::Error: core::fmt::Debug + Send + 'static,
    Hooks: waymark_transient_execution_bringup::DriverHooksFor<
            ActionCallRequesterFor<WorkerPoolRequests>,
            ActionCallCompletionsProviderFor<WorkerPoolCompletions>,
        >,
    Hooks: Send + Sync + 'static,
{
    let action_call_requester = ActionCallRequesterFor::new(worker_pool_requests);
    let action_call_completions_provider =
        ActionCallCompletionsProviderFor::new(worker_pool_completions);

    waymark_transient_execution_bringup::execute_with(
        runtime,
        action_call_requester,
        action_call_completions_provider,
        skip_sleep,
        cancel,
        hooks,
    )
}

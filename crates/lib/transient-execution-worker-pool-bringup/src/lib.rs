//! Transient workflow execution over the worker-pool action transport.
//!
//! Instantiates [`waymark_transient_execution_bringup::execute_with`] with
//! [`waymark_action_runtime_worker_pool`] as the action transport: action
//! calls are dispatched to a
//! [`waymark_worker_core::QueueActionDispatch`] and
//! action call completions are polled directly from it.

#![warn(missing_docs)]

/// The [`waymark_transient_execution_bringup::Execution`] type produced by
/// [`execute`] for the given worker pool.
pub type ExecutionFor<Pool> = waymark_transient_execution_bringup::Execution<
    waymark_transient_execution_bringup::DriverHandleFor<
        waymark_action_runtime_worker_pool::WorkerPoolActionRequester<
            Pool,
            waymark_action_runtime_metadata::ActionCallCorrelation,
        >,
        waymark_action_runtime_worker_pool::WorkerPoolActionCallCompletionsProvider<
            Pool,
            waymark_action_runtime_metadata::ActionCallCorrelation,
        >,
    >,
>;

/// Wire up and launch transient workflow execution for the given runtime
/// over the worker-pool action transport.
///
/// Action calls are dispatched to `worker_pool` and action call completions
/// are polled directly from it, with the correlation metadata round-tripped
/// verbatim — there is no per-VM demultiplexing, so this is suitable for
/// running a single VM at a time. Launching the pool is the caller's
/// responsibility.
///
/// When `skip_sleep` is true, every sleep in the workflow resolves
/// immediately instead of waiting for its deadline.
///
/// Cancelling `cancel` requests the driver loop to stop.
pub fn execute<Pool>(
    runtime: waymark_system_vm::Runtime,
    worker_pool: Pool,
    skip_sleep: bool,
    cancel: tokio_util::sync::CancellationToken,
) -> ExecutionFor<Pool>
where
    Pool: waymark_worker_core::QueueActionDispatch
        + waymark_worker_core::PollActionResults
        + Clone
        + Send
        + Sync
        + 'static,
    <Pool as waymark_worker_core::QueueActionDispatch>::Error: core::fmt::Debug + Send + 'static,
    <Pool as waymark_worker_core::PollActionResults>::Error: core::fmt::Debug + Send + 'static,
{
    let action_call_requester =
        waymark_action_runtime_worker_pool::WorkerPoolActionRequester::new(worker_pool.clone());
    let action_call_completions_provider =
        waymark_action_runtime_worker_pool::WorkerPoolActionCallCompletionsProvider::new(
            worker_pool,
        );

    waymark_transient_execution_bringup::execute_with(
        runtime,
        action_call_requester,
        action_call_completions_provider,
        skip_sleep,
        cancel,
    )
}

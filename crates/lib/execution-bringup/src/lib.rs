//! Bringup for the execution subsystem — wires the workload-pinning manager
//! and execution-driver together, spawning their run loops.
//!
//! [`start`] takes a backend, a pre-built state manager, and configuration,
//! then spawns all the loops needed for VM execution.

#![warn(missing_docs)]

use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_worker_core::{BaseWorkerPool, WorkerPoolError};

/// Configuration for [`start`].
pub struct Config<NodeId> {
    /// The node identifier for this executor instance.
    pub node_id: NodeId,

    /// How many VMs can be pinned concurrently.
    pub max_pinned: NonZeroUsize,

    /// How long a pinning lasts before it needs to be refreshed.
    pub pinning_ttl: NonZeroDuration,

    /// How often to refresh pinnings on active workloads.
    pub pinning_heartbeat: NonZeroDuration,

    /// How long to retain a VM state after eviction.
    pub vm_retention: NonZeroDuration,

    /// How often to sweep expired VM states.
    pub vm_sweep_interval: NonZeroDuration,

    /// How long to retain an executable after last use.
    pub executable_retention: NonZeroDuration,

    /// How often to sweep expired executables.
    pub executable_sweep_interval: NonZeroDuration,
}

/// Spawned execution subsystem handles.
pub struct Handles {
    /// Join handle for the workload pinning manager.
    pub pinning_manager: tokio::task::JoinHandle<()>,

    /// Join handle for the execution driver.
    pub execution_driver: tokio::task::JoinHandle<()>,

    /// Join handle for the executable state sweeper.
    pub executable_sweeper: tokio::task::JoinHandle<()>,

    /// Join handle for the VM state sweeper.
    pub vm_sweeper: tokio::task::JoinHandle<()>,

    /// Join handle for the completion poll-and-route background task.
    pub poll_route: tokio::task::JoinHandle<()>,
}

/// Start the execution subsystem.
///
/// Spawns the workload-pinning manager and the execution driver, wiring
/// them together via an mpsc channel. The caller is responsible for
/// constructing the [`waymark_state_manager::State`] with the appropriate
/// [`waymark_state_vm_runtimes::SpawningFactory`].
///
/// Returns an error if the worker pool fails to launch; nothing is spawned
/// in that case.
///
/// `shutdown_token` requests a graceful stop — new workloads are refused while
/// the maintenance loop keeps running until all active instances drain.
/// `force_shutdown_token` breaks out of that drain immediately, so shutdown
/// can't hang forever on a workload that never evicts.
pub async fn start<Backend, WorkerPool>(
    config: Config<Backend::NodeId>,
    backend: Arc<Backend>,
    worker_pool: WorkerPool,
    shutdown_token: CancellationToken,
    force_shutdown_token: CancellationToken,
) -> Result<Handles, WorkerPoolError>
where
    Backend: waymark_workload_pinning_backend::PollUnpinnedInstances,
    Backend: waymark_workload_pinning_backend::KeepaliveInstancePinnings,
    Backend: waymark_workload_pinning_backend::ReleasePinnings,
    Backend:
        waymark_workload_pinning_backend::HasTimestamp<Timestamp = chrono::DateTime<chrono::Utc>>,
    Backend: waymark_state_vm_executables_backend::LoadExecutable,
    <Backend as waymark_state_vm_executables_backend::LoadExecutable>::Error: Send + 'static,
    Backend: Send + Sync + 'static,
    Backend::NodeId: Clone + Send,
    Backend: waymark_workload_pinning_backend::HasInstanceId<
            InstanceId = <Backend as waymark_state_vm_runtimes_backend::HasVmId>::VmId,
        >,
    Backend: waymark_state_vm_runtimes_backend::HasVmId<VmId = waymark_ids::InstanceId>,
    <Backend as waymark_state_vm_runtimes_backend::HasVmId>::VmId:
        Eq + Hash + Clone + Send + Sync + core::fmt::Debug + 'static,
    Backend: waymark_state_vm_runtimes_backend::StoreSnapshot,
    Backend: waymark_state_vm_runtimes_backend::LoadForRevive,
    Backend: waymark_state_vm_runtimes_backend::HasExecutableId<
            ExecutableId = waymark_ids::WorkflowVersionId,
        >,
    Backend: waymark_state_vm_executables_backend::HasExecutableId<
            ExecutableId = waymark_ids::WorkflowVersionId,
        >,
    Backend: waymark_workflow_completion_backend::RecordCompletion,
    Backend: waymark_workflow_completion_backend::RecordException,
    Backend: waymark_workflow_completion_backend::HasVmId<
            VmId = <Backend as waymark_state_vm_runtimes_backend::HasVmId>::VmId,
        >,
    Backend: waymark_action_reconciler_backend::StorePendingActionCall,
    Backend: waymark_action_reconciler_backend::RemovePendingActionCall,
    Backend: waymark_action_reconciler_backend::LoadPendingActionCalls,
    <Backend as waymark_action_reconciler_backend::StorePendingActionCall>::Error: Send,
    <Backend as waymark_action_reconciler_backend::RemovePendingActionCall>::Error: Send,
    <Backend as waymark_action_reconciler_backend::LoadPendingActionCalls>::Error: Send,
    Backend: waymark_action_reconciler_backend::HasVmId<
            VmId = <Backend as waymark_state_vm_runtimes_backend::HasVmId>::VmId,
        >,
    <Backend as waymark_workload_pinning_backend::PollUnpinnedInstances>::Error: Send,
    <Backend as waymark_workload_pinning_backend::KeepaliveInstancePinnings>::Error: Send,
    <Backend as waymark_workload_pinning_backend::ReleasePinnings>::Error: Send,
    <Backend as waymark_state_vm_runtimes_backend::StoreSnapshot>::Error: Send + 'static,
    <Backend as waymark_state_vm_runtimes_backend::LoadForRevive>::Error: Send + 'static,
    <Backend as waymark_workflow_completion_backend::RecordCompletion>::Error: Send + 'static,
    <Backend as waymark_workflow_completion_backend::RecordException>::Error: Send + 'static,
    WorkerPool: BaseWorkerPool + Clone + Send + Sync + 'static,
{
    let Config {
        node_id,
        max_pinned,
        pinning_ttl,
        pinning_heartbeat,
        vm_retention,
        vm_sweep_interval,
        executable_retention,
        executable_sweep_interval,
    } = config;

    // Launch the worker pool so it can start routing action requests
    // to Python workers and collecting completions. Done before spawning
    // anything so a failure doesn't leave background tasks behind.
    // TODO: worker pool needs a better API, more in-line with the rest of
    // the new code and exposing the handles.
    worker_pool.launch().await?;

    let interpreter_provider = waymark_state_vm_runtimes_core::DefaultInterpreterProvider::<
        waymark_vm_interpreter_fullset::FullSetInterpreter<
            waymark_system_vm::Spec,
            Arc<waymark_system_vm::Executable>,
            waymark_system_vm::Value,
        >,
        <Backend as waymark_state_vm_runtimes_backend::HasVmId>::VmId,
    >::new();

    let (pinned_tx, pinned_rx) = tokio::sync::mpsc::channel(1);

    let codec = Arc::new(waymark_vm_codec_rmp::RmpCodec);

    let executable_factory = waymark_state_vm_executables::ExecutablesFactory::<
        _,
        waymark_vm_codec_rmp::RmpCodec,
        waymark_system_vm::Executable,
    >::new(Arc::clone(&backend), Arc::clone(&codec));
    let (executable_state, executable_sweeper) =
        waymark_state_manager::State::new(executable_retention, executable_factory);
    let executable_sweeper_handle = spawn_state_sweeper(
        executable_sweeper,
        executable_sweep_interval,
        shutdown_token.child_token(),
    );

    // Routed completions provider — polls the pool (via a direct provider
    // that stamps each completion with its owning VM id) and demultiplexes
    // completions into per-VM channels so each VM only receives its own
    // results.
    let mut router = waymark_action_runtime_completions_router::RoutedCompletionsProvider::new(
        waymark_action_runtime_worker_pool::WorkerPoolActionCallCompletionsProvider::new(
            worker_pool.clone(),
        ),
    );
    let registrar = router.registrar();

    // Background task: poll the pool and route completions to VMs.
    //
    // This is the only path completions take to reach VMs, so if this task
    // stops while the subsystem is still running, every VM with an in-flight
    // action stalls waiting on a completion that can never be routed. The
    // drop guard escalates any exit — completions source failure, panic —
    // into a subsystem-wide shutdown instead of parking silently.
    let poll_route_handle = tokio::spawn({
        let shutdown = shutdown_token.child_token();
        let shutdown_guard = shutdown_token.clone().drop_guard();
        async move {
            let _shutdown_guard = shutdown_guard;
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    result = router.poll_and_route() => {
                        if let Err(error) = result {
                            tracing::error!(?error, "completions source failed, stopping poll router and shutting down");
                            break;
                        }
                    }
                }
            }
        }
    });

    let effector_provider = waymark_state_vm_runtimes_core::FnEffectorProvider::new({
        let backend = Arc::clone(&backend);
        let codec = Arc::clone(&codec);
        let registrar = registrar.clone();
        move |vm_id: &<Backend as waymark_state_vm_runtimes_backend::HasVmId>::VmId| {
            let action_call_requester =
                waymark_action_runtime_worker_pool::WorkerPoolActionRequester {
                    pool: worker_pool.clone(),
                    vm_id: *vm_id,
                };

            let action_call_complations_provider = registrar.register(*vm_id);

            let (action_handler, action_poller) = waymark_action_reconciler::new(
                action_call_requester,
                action_call_complations_provider,
                Arc::clone(&backend),
                Arc::clone(&codec),
                *vm_id,
            );
            let (sleep_handler, sleep_poller) = waymark_sleep_reconciler::new(false);
            let (extcall_handler, extcall_settler) = waymark_extcall_reconciler::new(
                action_handler,
                sleep_handler,
                action_poller,
                sleep_poller,
            );
            let completion_handler = waymark_workflow_completion::EffectHandler::new(
                Arc::clone(&backend),
                *vm_id,
                Arc::clone(&codec),
            );

            let handler = waymark_fullset_effect_handler::EffectHandler {
                core: completion_handler,
                extcall: extcall_handler,
            };

            (handler, extcall_settler)
        }
    });

    let vm_runtimes_factory = waymark_state_vm_runtimes::SpawningFactory::new(
        Arc::clone(&backend),
        Arc::clone(&codec),
        executable_state,
        interpreter_provider,
        effector_provider,
    );

    let (vm_runtimes_state, vm_runtimes_sweeper) =
        waymark_state_manager::State::new(vm_retention, vm_runtimes_factory);
    let vm_runtimes_sweeper_handle = spawn_state_sweeper(
        vm_runtimes_sweeper,
        vm_sweep_interval,
        shutdown_token.child_token(),
    );

    let pinning_params = waymark_workload_pinning_manager::Params {
        shutdown_token: shutdown_token.child_token(),
        force_shutdown_token,
        backend: Arc::clone(&backend),
        node_id,
        pinned_tx,
        max_pinned,
        pinning_ttl,
        pinning_heartbeat,
    };

    let pinning_manager = tokio::spawn(async move {
        let outcome = waymark_workload_pinning_manager::run(pinning_params).await;
        if let Some(error) = outcome.poll_error {
            tracing::error!(?error, "workload pinning manager poll loop failed");
        }
        if let Some(error) = outcome.maintenance_error {
            tracing::error!(?error, "workload pinning manager maintenance loop failed");
        }
        if let Some(error) = outcome.cleanup_error {
            tracing::warn!(?error, "workload pinning manager cleanup failed");
        }
    });

    let execution_driver = tokio::spawn(waymark_execution_driver::run(
        pinned_rx,
        Arc::new(vm_runtimes_state),
        shutdown_token.child_token(),
    ));

    Ok(Handles {
        pinning_manager,
        execution_driver,
        executable_sweeper: executable_sweeper_handle,
        vm_sweeper: vm_runtimes_sweeper_handle,
        poll_route: poll_route_handle,
    })
}

fn spawn_state_sweeper<Key, Value>(
    mut sweeper: waymark_state_manager::Sweeper<Key, Value>,
    interval: waymark_nonzero_duration::NonZeroDuration,
    shutdown: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()>
where
    Key: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    Value: Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval.get());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if !sweeper.associated_state_exists() {
                        break;
                    }
                    sweeper.sweep();
                }
                () = shutdown.cancelled() => {
                    break;
                }
            }
        }
    })
}

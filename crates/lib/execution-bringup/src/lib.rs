//! Bringup for the execution subsystem — wires the workload-pinning
//! manager, the execution driver, the worker pool, the durable
//! action-call pipelines (requests and completions), and the durable
//! sleeps pipeline together, spawning their run loops.
//!
//! [`start`] takes a backend, a worker pool, and configuration, then
//! spawns all the loops needed for VM execution.

#![warn(missing_docs)]

use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_worker_core::{BaseWorkerPool, WorkerPoolError};

/// Configuration for [`start`].
pub struct Config<NodeId> {
    /// The identifier of this node.
    ///
    /// Also the identity this process owns action-call request locks
    /// under.
    pub node_id: NodeId,

    /// How long an action-call request lock lasts before it needs to be
    /// renewed.
    pub action_effect_reconciler_lock_ttl: NonZeroDuration,

    /// How often to renew the held action-call request locks.
    pub action_effect_reconciler_lock_heartbeat: NonZeroDuration,

    /// How many VMs can be pinned concurrently.
    pub max_pinned: NonZeroUsize,

    /// How long a pinning lasts before it needs to be refreshed.
    pub pinning_ttl: NonZeroDuration,

    /// How often to refresh pinnings on active workloads.
    pub pinning_heartbeat: NonZeroDuration,

    /// How much earlier than the pinning ttl a pinning is fenced when it
    /// cannot be re-confirmed — the margin budgets the eviction latency
    /// between the fence signal and the workload actually stopping.
    pub pinning_fencing_margin: NonZeroDuration,

    /// Minimum interval between unpinned-workload poll queries.
    ///
    /// Polling is a spin by design; this only floors how tight it can get.
    pub workload_poll_interval: NonZeroDuration,

    /// Maximum number of VM snapshots coalesced into one batched write.
    pub snapshot_batch_max: NonZeroUsize,

    /// Longest a snapshot waits to be batched before its write is flushed.
    pub snapshot_batch_delay: NonZeroDuration,

    /// Maximum number of action-call requests coalesced into one batched
    /// insert.
    pub action_effect_reconciler_request_batch_max: NonZeroUsize,

    /// Longest an action-call request waits to be batched before its
    /// insert is flushed.
    pub action_effect_reconciler_request_batch_delay: NonZeroDuration,

    /// Maximum number of workflow terminal outcomes coalesced into one
    /// batched upsert.
    pub workflow_completion_batch_max: NonZeroUsize,

    /// Longest a workflow terminal outcome waits to be batched before its
    /// upsert is flushed.
    pub workflow_completion_batch_delay: NonZeroDuration,

    /// Maximum number of revival-reconcile lock statements coalesced into
    /// one batched statement pair.
    pub action_effect_reconciler_lock_batch_max: NonZeroUsize,

    /// Longest a revival reconcile waits to be batched before its lock
    /// statement is flushed.
    pub action_effect_reconciler_lock_batch_delay: NonZeroDuration,

    /// How long the durable-sleeps demand poller waits between polls
    /// while demand is registered.
    pub sleep_poll_interval: NonZeroDuration,

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

    /// Join handle for the durable action-call completions writer.
    pub durable_action_completions_writer: tokio::task::JoinHandle<()>,

    /// Join handle for the durable action-call completions demand poller.
    pub durable_action_completions_poller: tokio::task::JoinHandle<()>,

    /// Join handle for the durable action-call completions acker.
    pub durable_action_completions_acker: tokio::task::JoinHandle<()>,

    /// Join handle for the durable sleeps demand poller.
    pub durable_sleeps_poller: tokio::task::JoinHandle<()>,

    /// Join handle for the durable sleeps acker.
    pub durable_sleeps_acker: tokio::task::JoinHandle<()>,

    /// Join handle for the action-call request lock renewal heartbeat.
    pub action_effect_reconciler_lock_renewal: tokio::task::JoinHandle<()>,

    /// Join handle for the VM snapshot write batcher.
    pub snapshot_batcher: tokio::task::JoinHandle<()>,

    /// Join handle for the action-call request write batcher.
    pub action_effect_reconciler_request_batcher: tokio::task::JoinHandle<()>,

    /// Join handle for the workflow terminal-outcome write batcher.
    pub workflow_completion_batcher: tokio::task::JoinHandle<()>,

    /// Join handle for the revival-reconcile lock batcher.
    pub action_effect_reconciler_lock_batcher: tokio::task::JoinHandle<()>,
}

/// Start the execution subsystem.
///
/// Launches the worker pool, assembles the VM runtime state (spawning
/// factory, effectors, and the durable action-call request reconcile),
/// and spawns every run loop: the workload-pinning manager, the execution
/// driver, the state sweepers, the durable action-call completions
/// pipeline, the durable sleeps pipeline, and the action-call request
/// lock renewal heartbeat.
///
/// Returns an error if the worker pool fails to launch; nothing is spawned
/// in that case.
///
/// `shutdown_token` requests a graceful stop — new workloads are refused while
/// the maintenance loop keeps running until all active workloads drain.
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
    Backend: waymark_workload_pinning_backend::PollUnpinnedWorkloads,
    Backend: waymark_workload_pinning_backend::KeepalivePinnings,
    Backend: waymark_workload_pinning_backend::UnpinWorkloads,
    Backend:
        waymark_workload_pinning_backend::HasTimestamp<Timestamp = chrono::DateTime<chrono::Utc>>,
    Backend: waymark_state_vm_executables_backend::LoadExecutable,
    <Backend as waymark_state_vm_executables_backend::LoadExecutable>::Error: Send + 'static,
    Backend: Send + Sync + 'static,
    Backend::NodeId: Clone + Send + Sync + 'static,
    Backend: waymark_workload_pinning_backend::HasWorkloadId<
            WorkloadId = <Backend as waymark_state_vm_runtimes_backend::HasVmId>::VmId,
        >,
    Backend: waymark_state_vm_runtimes_backend::HasVmId<VmId = waymark_ids::InstanceId>,
    <Backend as waymark_state_vm_runtimes_backend::HasVmId>::VmId:
        Eq + Hash + Clone + Send + Sync + core::fmt::Debug + 'static,
    Backend: waymark_state_vm_runtimes_backend::StoreSnapshots,
    Backend: waymark_state_vm_runtimes_backend::LoadForRevive,
    Backend: waymark_state_vm_runtimes_backend::HasExecutableId<
            ExecutableId = waymark_ids::WorkflowVersionId,
        >,
    Backend: waymark_state_vm_executables_backend::HasExecutableId<
            ExecutableId = waymark_ids::WorkflowVersionId,
        >,
    Backend: waymark_action_completions_reconciler_backend::RecordCompletions,
    Backend: waymark_action_completions_reconciler_backend::PollCompletions,
    Backend: waymark_action_completions_reconciler_backend::AckCompletions,
    Backend: waymark_action_completions_reconciler_backend::HasVmId<VmId = waymark_ids::InstanceId>,
    <Backend as waymark_action_completions_reconciler_backend::RecordCompletions>::Error:
        Send + 'static,
    <Backend as waymark_action_completions_reconciler_backend::PollCompletions>::Error:
        Send + 'static,
    <Backend as waymark_action_completions_reconciler_backend::AckCompletions>::Error:
        Send + 'static,
    Backend: waymark_sleep_reconciler_backend::RecordSleeps,
    Backend: waymark_sleep_reconciler_backend::PollDueSleeps,
    Backend: waymark_sleep_reconciler_backend::AckSleeps,
    Backend: waymark_sleep_reconciler_backend::HasVmId<VmId = waymark_ids::InstanceId>,
    Backend:
        waymark_sleep_reconciler_backend::HasTimestamp<Timestamp = chrono::DateTime<chrono::Utc>>,
    <Backend as waymark_sleep_reconciler_backend::RecordSleeps>::Error: Send + 'static,
    <Backend as waymark_sleep_reconciler_backend::PollDueSleeps>::Error: Send + 'static,
    <Backend as waymark_sleep_reconciler_backend::AckSleeps>::Error: Send + 'static,
    Backend: waymark_action_effect_reconciler_backend::RecordActionCallRequests,
    Backend: waymark_action_effect_reconciler_backend::LockActionCallRequests,
    Backend: waymark_action_effect_reconciler_backend::RenewActionCallRequestLocks,
    Backend: waymark_action_effect_reconciler_backend::HasVmId<VmId = waymark_ids::InstanceId>,
    Backend:
        waymark_action_effect_reconciler_backend::HasLockOwnerId<LockOwnerId = Backend::NodeId>,
    Backend: waymark_action_effect_reconciler_backend::HasTimestamp<
            Timestamp = chrono::DateTime<chrono::Utc>,
        >,
    <Backend as waymark_action_effect_reconciler_backend::RecordActionCallRequests>::Error:
        Send + 'static,
    <Backend as waymark_action_effect_reconciler_backend::LockActionCallRequests>::Error:
        Send + 'static,
    Backend: waymark_workflow_completion_backend::RecordOutcomes,
    Backend: waymark_workflow_completion_backend::HasVmId<
            VmId = <Backend as waymark_state_vm_runtimes_backend::HasVmId>::VmId,
        >,
    <Backend as waymark_workload_pinning_backend::PollUnpinnedWorkloads>::Error: Send,
    <Backend as waymark_workload_pinning_backend::KeepalivePinnings>::Error: Send,
    <Backend as waymark_workload_pinning_backend::UnpinWorkloads>::Error: Send,
    <Backend as waymark_state_vm_runtimes_backend::StoreSnapshots>::Error: Send + 'static,
    <Backend as waymark_state_vm_runtimes_backend::LoadForRevive>::Error: Send + 'static,
    <Backend as waymark_workflow_completion_backend::RecordOutcomes>::Error: Send + 'static,
    WorkerPool: BaseWorkerPool + Clone + Send + Sync + 'static,
{
    let Config {
        node_id,
        action_effect_reconciler_lock_ttl,
        action_effect_reconciler_lock_heartbeat,
        max_pinned,
        pinning_ttl,
        pinning_heartbeat,
        pinning_fencing_margin,
        workload_poll_interval,
        snapshot_batch_max,
        snapshot_batch_delay,
        action_effect_reconciler_request_batch_max,
        action_effect_reconciler_request_batch_delay,
        workflow_completion_batch_max,
        workflow_completion_batch_delay,
        action_effect_reconciler_lock_batch_max,
        action_effect_reconciler_lock_batch_delay,
        sleep_poll_interval,
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

    // Durable action-call completions pipeline (not to be confused with
    // workflow completions) — action-call completions are recorded durably
    // as they arrive from the worker pool and removed only once their
    // promise settlements have been durably applied.  Three background loops, each
    // holding a drop guard on the subsystem shutdown token: these are the
    // only path completions take to reach VMs, so any loop dying escalates
    // to a subsystem-wide shutdown instead of stranding in-flight promises
    // silently.
    let writer_params = waymark_action_completions_reconciler::writer::Params {
        provider: waymark_action_runtime_worker_pool::WorkerPoolActionCallCompletionsProvider::<
            _,
            waymark_action_runtime_metadata::WithVmId<
                waymark_ids::InstanceId,
                waymark_action_runtime_metadata::ActionCallCorrelation,
            >,
        >::new(worker_pool.clone()),
        backend: Arc::clone(&backend),
        codec: Arc::clone(&codec),
    };
    let durable_action_completions_writer_handle = tokio::spawn({
        let shutdown = shutdown_token.child_token();
        let shutdown_guard = shutdown_token.clone().drop_guard();
        async move {
            let _shutdown_guard = shutdown_guard;
            tokio::select! {
                _ = shutdown.cancelled() => {}
                result = waymark_action_completions_reconciler::writer::run(writer_params) => {
                    let Err(error) = result;
                    tracing::error!(
                        ?error,
                        "durable action-call completions writer failed, shutting down"
                    );
                }
            }
        }
    });

    let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();
    let acker_params = waymark_action_completions_reconciler::acker::Params {
        backend: Arc::clone(&backend),
        ack_rx,
    };
    let durable_action_completions_acker_handle = tokio::spawn({
        let shutdown = shutdown_token.child_token();
        let shutdown_guard = shutdown_token.clone().drop_guard();
        async move {
            let _shutdown_guard = shutdown_guard;
            tokio::select! {
                _ = shutdown.cancelled() => {}
                () = waymark_action_completions_reconciler::acker::run(acker_params) => {}
            }
        }
    });

    let (registrar, poller_state) = waymark_action_completions_reconciler::poller::state(ack_tx);
    let poller_params = waymark_action_completions_reconciler::poller::Params {
        backend: Arc::clone(&backend),
        codec: Arc::clone(&codec),
        state: poller_state,
    };
    let durable_action_completions_poller_handle = tokio::spawn({
        let shutdown = shutdown_token.child_token();
        let shutdown_guard = shutdown_token.clone().drop_guard();
        async move {
            let _shutdown_guard = shutdown_guard;
            tokio::select! {
                _ = shutdown.cancelled() => {}
                result = waymark_action_completions_reconciler::poller::run(poller_params) => {
                    let Err(error) = result;
                    tracing::error!(
                        ?error,
                        "durable action-call completions demand poller failed, shutting down"
                    );
                }
            }
        }
    });

    // Durable sleeps pipeline — sleep requests are recorded durably as the
    // VMs emit them (inline in the per-VM effect handler, so there is no
    // writer loop) and removed only once their settlements have been
    // durably applied.  Two background loops, each holding a drop guard on
    // the subsystem shutdown token, mirroring the completions pipeline
    // above.
    let (sleep_ack_tx, sleep_ack_rx) = tokio::sync::mpsc::unbounded_channel();
    let sleep_acker_params = waymark_sleep_reconciler::acker::Params {
        backend: Arc::clone(&backend),
        ack_rx: sleep_ack_rx,
    };
    let durable_sleeps_acker_handle = tokio::spawn({
        let shutdown = shutdown_token.child_token();
        let shutdown_guard = shutdown_token.clone().drop_guard();
        async move {
            let _shutdown_guard = shutdown_guard;
            tokio::select! {
                _ = shutdown.cancelled() => {}
                () = waymark_sleep_reconciler::acker::run(sleep_acker_params) => {}
            }
        }
    });

    let (sleep_registrar, sleep_poller_state) =
        waymark_sleep_reconciler::poller::state(sleep_ack_tx);
    let sleep_poller_params = waymark_sleep_reconciler::poller::Params {
        backend: Arc::clone(&backend),
        state: sleep_poller_state,
        poll_interval: sleep_poll_interval,
    };
    let durable_sleeps_poller_handle = tokio::spawn({
        let shutdown = shutdown_token.child_token();
        let shutdown_guard = shutdown_token.clone().drop_guard();
        async move {
            let _shutdown_guard = shutdown_guard;
            tokio::select! {
                _ = shutdown.cancelled() => {}
                result = waymark_sleep_reconciler::poller::run(sleep_poller_params) => {
                    let Err(error) = result;
                    tracing::error!(
                        ?error,
                        "durable sleeps demand poller failed, shutting down"
                    );
                }
            }
        }
    });

    // Durable action-call requests: emitted action calls are recorded as
    // born-locked request rows before delivery to the local pool, and the
    // renewal heartbeat keeps the held locks alive while the attempts run.
    // A held lock is the authorization to execute its attempt; a lock that
    // cannot be renewed in time is a fence breach, and with no per-attempt
    // termination primitive the drop guard escalates to subsystem shutdown,
    // force-terminating every local attempt with the process.
    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    let renewal_params = waymark_action_effect_reconciler::renewal::Params {
        backend: Arc::clone(&backend),
        lock_owner_id: node_id.clone(),
        lock_time_to_live: action_effect_reconciler_lock_ttl,
        heartbeat: action_effect_reconciler_lock_heartbeat,
        held_locks_rx,
    };
    let action_effect_reconciler_lock_renewal_handle = tokio::spawn({
        let shutdown = shutdown_token.child_token();
        let shutdown_guard = shutdown_token.clone().drop_guard();
        async move {
            let _shutdown_guard = shutdown_guard;
            tokio::select! {
                _ = shutdown.cancelled() => {}
                result = waymark_action_effect_reconciler::renewal::run(renewal_params) => {
                    match result {
                        Ok(()) => {
                            tracing::info!("action-call request lock renewal drained");
                        }
                        Err(error) => {
                            tracing::error!(
                                ?error,
                                "action-call request lock fence breached, shutting down"
                            );
                        }
                    }
                }
            }
        }
    });

    let (request_recorder, action_effect_reconciler_request_batcher_loop) =
        waymark_action_effect_reconciler::request_batcher(
            Arc::clone(&backend),
            node_id.clone(),
            action_effect_reconciler_lock_ttl,
            waymark_batcher::Policy {
                max_batch: action_effect_reconciler_request_batch_max,
                max_delay: action_effect_reconciler_request_batch_delay,
            },
            {
                let shutdown = shutdown_token.child_token();
                async move { shutdown.cancelled_owned().await }
            },
        );
    let action_effect_reconciler_request_batcher_handle =
        tokio::spawn(action_effect_reconciler_request_batcher_loop);

    let (outcome_recorder, workflow_completion_batcher_loop) =
        waymark_workflow_completion::outcome_batcher::outcome_batcher(
            Arc::clone(&backend),
            waymark_batcher::Policy {
                max_batch: workflow_completion_batch_max,
                max_delay: workflow_completion_batch_delay,
            },
            {
                let shutdown = shutdown_token.child_token();
                async move { shutdown.cancelled_owned().await }
            },
        );
    let workflow_completion_batcher_handle = tokio::spawn(workflow_completion_batcher_loop);

    let requests_factory_worker_pool = worker_pool.clone();
    let effector_provider = waymark_state_vm_runtimes_core::FnEffectorProvider::new({
        let backend = Arc::clone(&backend);
        let codec = Arc::clone(&codec);
        let registrar = registrar.clone();
        let sleep_registrar = sleep_registrar.clone();
        let request_recorder = request_recorder.clone();
        let outcome_recorder = outcome_recorder.clone();
        let held_locks_tx = held_locks_tx.clone();
        move |vm_id: &<Backend as waymark_state_vm_runtimes_backend::HasVmId>::VmId| {
            let action_call_requester =
                waymark_action_runtime_worker_pool::WorkerPoolActionRequester::new(
                    worker_pool.clone(),
                );
            let action_call_requester =
                waymark_action_runtime_metadata_compat::WithVmIdActionCallRequester {
                    vm_id: *vm_id,
                    action_call_requester,
                };

            let action_handler = waymark_action_effect_reconciler::EffectHandler {
                recorder: request_recorder.clone(),
                codec: Arc::clone(&codec),
                held_locks_tx: held_locks_tx.clone(),
                vm_id: *vm_id,
                requester: action_call_requester,
            };
            let action_settler = registrar.subscribe(*vm_id);

            let sleep_handler = waymark_sleep_reconciler::EffectHandler {
                backend: Arc::clone(&backend),
                vm_id: *vm_id,
            };
            let sleep_settler =
                sleep_registrar.subscribe::<waymark_sleep_compat::ReadyValueSleepProvider>(*vm_id);
            let (extcall_handler, extcall_settler) = waymark_extcall_reconciler::new(
                action_handler,
                sleep_handler,
                action_settler,
                sleep_settler,
            );
            let completion_handler = waymark_workflow_completion::EffectHandler::new(
                outcome_recorder.clone(),
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

    let (snapshot_batcher, snapshot_batcher_loop) = waymark_state_vm_runtimes::snapshot_batcher(
        Arc::clone(&backend),
        waymark_batcher::Policy {
            max_batch: snapshot_batch_max,
            max_delay: snapshot_batch_delay,
        },
        {
            let shutdown = shutdown_token.child_token();
            async move { shutdown.cancelled_owned().await }
        },
    );
    let snapshot_batcher_handle = tokio::spawn(snapshot_batcher_loop);

    let vm_runtimes_factory = waymark_state_vm_runtimes::SpawningFactory::new(
        Arc::clone(&backend),
        Arc::clone(&codec),
        executable_state,
        interpreter_provider,
        effector_provider,
        snapshot_batcher,
    );

    // Reconcile-before-produce: at VM revival, pending request rows whose
    // locks lapsed are relocked and their calls redelivered to the local
    // pool, before the VM exists.
    let (vm_locker, action_effect_reconciler_lock_batcher_loop) =
        waymark_action_effect_reconciler::lock_batcher(
            Arc::clone(&backend),
            node_id.clone(),
            action_effect_reconciler_lock_ttl,
            waymark_batcher::Policy {
                max_batch: action_effect_reconciler_lock_batch_max,
                max_delay: action_effect_reconciler_lock_batch_delay,
            },
            {
                let shutdown = shutdown_token.child_token();
                async move { shutdown.cancelled_owned().await }
            },
        );
    let action_effect_reconciler_lock_batcher_handle =
        tokio::spawn(action_effect_reconciler_lock_batcher_loop);

    let vm_runtimes_factory = waymark_action_effect_reconciler::ReconcilingFactory {
        inner: vm_runtimes_factory,
        locker: vm_locker,
        codec: Arc::clone(&codec),
        held_locks_tx: held_locks_tx.clone(),
        requester_provider: move |vm_id: &waymark_ids::InstanceId| {
            let action_call_requester =
                waymark_action_runtime_worker_pool::WorkerPoolActionRequester::new(
                    requests_factory_worker_pool.clone(),
                );
            waymark_action_runtime_metadata_compat::WithVmIdActionCallRequester {
                vm_id: *vm_id,
                action_call_requester,
            }
        },
    };

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
        poll_interval: workload_poll_interval,
        pinning_heartbeat,
        // TODO: surface through `Config` and the env.
        unpin_retry_interval: NonZeroDuration::new(std::time::Duration::from_secs(5))
            .expect("five seconds is non-zero"),
        pinning_fencing_margin,
    };

    let pinning_manager = tokio::spawn(async move {
        let outcome = waymark_workload_pinning_manager::run(pinning_params).await;
        if let Some(error) = outcome.poll_error {
            tracing::error!(?error, "workload pinning manager poll loop failed");
        }
        if let Some(error) = outcome.maintenance_error {
            tracing::error!(?error, "workload pinning manager maintenance loop failed");
        }
        if let Some(error) = outcome.unpin_error {
            tracing::warn!(?error, "workload pinning manager unpin loop failed");
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
        durable_action_completions_writer: durable_action_completions_writer_handle,
        durable_action_completions_poller: durable_action_completions_poller_handle,
        durable_action_completions_acker: durable_action_completions_acker_handle,
        durable_sleeps_poller: durable_sleeps_poller_handle,
        durable_sleeps_acker: durable_sleeps_acker_handle,
        action_effect_reconciler_lock_renewal: action_effect_reconciler_lock_renewal_handle,
        snapshot_batcher: snapshot_batcher_handle,
        action_effect_reconciler_request_batcher: action_effect_reconciler_request_batcher_handle,
        workflow_completion_batcher: workflow_completion_batcher_handle,
        action_effect_reconciler_lock_batcher: action_effect_reconciler_lock_batcher_handle,
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

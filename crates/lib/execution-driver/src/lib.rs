//! Execution driver — bridges the workload-pinning manager and the
//! state-vm-runtimes systems.
//!
//! [`run`] subscribes to batches of
//! [`waymark_workload_pinning_manager::PinnedHandle`]s dispatched by the
//! workload pinning manager's poll loop, revives (or retrieves) each VM
//! via [`waymark_state_manager::State`], and unpins them when the VM
//! driver exits or on global shutdown.

#![warn(missing_docs)]

use std::hash::Hash;
use std::sync::Arc;

use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;
use waymark_state_vm_runtimes::Evicted;

/// The exit error of the VMs the execution driver drives.
type DriverErrorFor<
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> = waymark_vm_driver_thread::Error<
    waymark_vm_driver::Error<
        ExecutionError,
        SnapshotSerializationError,
        SnapshotPersistenceError,
        EffectHandlingError,
        GettingPromiseSettlementsError,
    >,
>;

/// The state manager the execution driver operates on.
type StateFor<
    Factory,
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> = waymark_state_manager::State<
    <Factory as waymark_state_manager_core::Factory>::Key,
    Arc<
        waymark_state_vm_runtimes::Spawned<
            DriverErrorFor<
                ExecutionError,
                SnapshotSerializationError,
                SnapshotPersistenceError,
                EffectHandlingError,
                GettingPromiseSettlementsError,
            >,
        >,
    >,
    Factory,
>;

/// Run the execution driver loop — receives batches of pinned handles from
/// the workload pinning manager, activates each VM, and keeps them alive
/// while the spawned runtime drives to completion.
///
/// Returns when the [`CancellationToken`] fires or the pinning channel closes.
#[tracing::instrument(skip_all)]
pub async fn run<
    Factory,
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
>(
    mut pinned_rx: mpsc::Receiver<
        NEVec<waymark_workload_pinning_manager::PinnedHandle<Factory::Key>>,
    >,
    state: Arc<
        StateFor<
            Factory,
            ExecutionError,
            SnapshotSerializationError,
            SnapshotPersistenceError,
            EffectHandlingError,
            GettingPromiseSettlementsError,
        >,
    >,
    shutdown_token: CancellationToken,
) where
    Factory: waymark_state_manager_core::Factory<
            Value = Arc<
                waymark_state_vm_runtimes::Spawned<
                    DriverErrorFor<
                        ExecutionError,
                        SnapshotSerializationError,
                        SnapshotPersistenceError,
                        EffectHandlingError,
                        GettingPromiseSettlementsError,
                    >,
                >,
            >,
        > + Send
        + Sync
        + 'static,
    Factory::Key: Eq + Hash + Clone + std::fmt::Debug + Send + Sync + 'static,
    Factory::Error: std::fmt::Debug,
    ExecutionError: Send + 'static,
    SnapshotSerializationError: Send + 'static,
    SnapshotPersistenceError: Send + 'static,
    EffectHandlingError: Send + 'static,
    GettingPromiseSettlementsError: Send + 'static,
{
    let shutdown = shutdown_token.clone().cancelled_owned();
    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        let batch = tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("execution driver shutting down");
                break;
            }
            Some(batch) = pinned_rx.recv() => batch,
            else => {
                tracing::info!("pinned channel closed");
                break;
            }
        };

        for pinned in batch {
            let state = Arc::clone(&state);
            let shutdown = shutdown_token.child_token();
            tokio::spawn(drive_one(pinned, state, shutdown).in_current_span());
        }
    }
}

/// Revive a pinned VM from its snapshot, then hold both handles alive until
/// the VM driver exits or global shutdown fires. Dropping the handles on
/// return releases the pinning and decrements the state-manager refcount.
#[tracing::instrument(skip_all, fields(instance_id = ?pinned.id()))]
async fn drive_one<
    Factory,
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
>(
    pinned: waymark_workload_pinning_manager::PinnedHandle<Factory::Key>,
    state: Arc<
        StateFor<
            Factory,
            ExecutionError,
            SnapshotSerializationError,
            SnapshotPersistenceError,
            EffectHandlingError,
            GettingPromiseSettlementsError,
        >,
    >,
    shutdown: CancellationToken,
) where
    Factory: waymark_state_manager_core::Factory<
            Value = Arc<
                waymark_state_vm_runtimes::Spawned<
                    DriverErrorFor<
                        ExecutionError,
                        SnapshotSerializationError,
                        SnapshotPersistenceError,
                        EffectHandlingError,
                        GettingPromiseSettlementsError,
                    >,
                >,
            >,
        > + Send
        + Sync
        + 'static,
    Factory::Key: Eq + Hash + Clone + std::fmt::Debug + Send + Sync + 'static,
    Factory::Error: std::fmt::Debug,
    ExecutionError: Send + 'static,
    SnapshotSerializationError: Send + 'static,
    SnapshotPersistenceError: Send + 'static,
    EffectHandlingError: Send + 'static,
    GettingPromiseSettlementsError: Send + 'static,
{
    let vm = match state.get(pinned.id().clone()).await {
        Ok(vm) => vm,
        Err(error) => {
            tracing::error!(?error, "failed to revive VM");
            return;
        }
    };

    let _driven = waymark_metrics_util::counted_scope(
        metrics::counter!("waymark_execution_driver_instances_revived_total"),
        metrics::counter!("waymark_execution_driver_instances_evicted_total"),
    );

    tracing::debug!("VM running");

    let mut fenced = false;
    let evicted = tokio::select! {
        evicted = vm.evicted() => evicted,
        _ = pinned.fenced() => {
            // The pinning lapsed or was lost to another node — this node
            // can no longer prove it holds it, so stop driving the VM.
            tracing::warn!("pinning fenced; evicting VM");
            fenced = true;
            vm.trigger_eviction();
            vm.evicted().await
        }
        _ = shutdown.cancelled() => {
            tracing::debug!("shutting down: evicting VM");
            vm.trigger_eviction();
            vm.evicted().await
        }
    };

    // The pin is lifted only after `evicted()` has resolved — the VM
    // driver has fully exited by then.

    // A fenced pinning is lost: another node may already hold it, so it
    // is not ours to park. Release it unconditionally.
    if fenced {
        tracing::debug!("VM evicted after fence, releasing");
        drop(pinned);
        return;
    }

    match evicted {
        Evicted::DriverError(waymark_vm_driver_thread::Error::Driver(
            waymark_vm_driver::Error::NoReadyFramesOrWaitingPromises,
        )) => {
            // The workflow has no ready frames and no waiting promises:
            // its terminal outcome is durably recorded, so an unpark will
            // never be needed — which justifies parking.
            tracing::debug!("VM evicted, parking workload");
            pinned.unpin(waymark_workload_pinning_core::UnpinMode::Park);
        }
        Evicted::DriverError(_) => {
            tracing::error!("VM driver failed");
            drop(pinned);
        }
        Evicted::HandledElsewhere => {
            tracing::debug!("VM evicted, unpinning");
            drop(pinned);
        }
    }
}

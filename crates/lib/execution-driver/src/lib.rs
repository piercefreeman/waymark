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

/// Run the execution driver loop — receives batches of pinned handles from
/// the workload pinning manager, activates each VM, and keeps them alive
/// while the spawned runtime drives to completion.
///
/// Returns when the [`CancellationToken`] fires or the pinning channel closes.
#[tracing::instrument(skip_all)]
pub async fn run<Factory>(
    mut pinned_rx: mpsc::Receiver<
        NEVec<waymark_workload_pinning_manager::PinnedHandle<Factory::Key>>,
    >,
    state: Arc<
        waymark_state_manager::State<
            Factory::Key,
            Arc<waymark_state_vm_runtimes::Spawned>,
            Factory,
        >,
    >,
    shutdown_token: CancellationToken,
) where
    Factory: waymark_state_manager_core::Factory<Value = Arc<waymark_state_vm_runtimes::Spawned>>
        + Send
        + Sync
        + 'static,
    Factory::Key: Eq + Hash + Clone + std::fmt::Debug + Send + Sync + 'static,
    Factory::Error: std::fmt::Debug,
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
async fn drive_one<Factory>(
    pinned: waymark_workload_pinning_manager::PinnedHandle<Factory::Key>,
    state: Arc<
        waymark_state_manager::State<
            Factory::Key,
            Arc<waymark_state_vm_runtimes::Spawned>,
            Factory,
        >,
    >,
    shutdown: CancellationToken,
) where
    Factory: waymark_state_manager_core::Factory<Value = Arc<waymark_state_vm_runtimes::Spawned>>
        + Send
        + Sync
        + 'static,
    Factory::Key: Eq + Hash + Clone + std::fmt::Debug + Send + Sync + 'static,
    Factory::Error: std::fmt::Debug,
{
    let vm = match state.get(pinned.id().clone()).await {
        Ok(vm) => vm,
        Err(error) => {
            tracing::error!(?error, "failed to revive VM");
            return;
        }
    };

    tracing::debug!("VM running");

    tokio::select! {
        _ = vm.evicted() => {
            tracing::debug!("VM evicted, unpinning");
        }
        _ = shutdown.cancelled() => {
            tracing::debug!("shutting down: evicting VM and unpinning");
            vm.trigger_eviction();
            vm.evicted().await;
        }
    }

    // `vm` drops before `pinned` on scope exit; the pin is held until the VM
    // driver has fully exited.
}

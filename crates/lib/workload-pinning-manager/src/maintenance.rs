//! Maintenance loop — handles evictions, pinnings refresh, and batch registration.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use nonempty_collections::{IntoIteratorExt as _, NEVec};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use waymark_nonzero_duration::NonZeroDuration;
use waymark_workload_pinning_backend::{
    HasInstanceId, HasNodeId, KeepaliveInstancePinnings, Pinning, ReleasePinnings,
};

use crate::Error;

pub(super) struct MaintainParams<Backend>
where
    Backend: HasNodeId + HasInstanceId,
{
    pub backend: Arc<Backend>,
    pub node_id: Backend::NodeId,
    pub batch_rx: mpsc::Receiver<(
        NEVec<Backend::InstanceId>,
        tokio::sync::oneshot::Sender<usize>,
    )>,
    pub evict_rx: mpsc::UnboundedReceiver<Backend::InstanceId>,
    pub count_tx: mpsc::UnboundedSender<usize>,
    pub cleanup_tx: tokio::sync::oneshot::Sender<HashSet<Backend::InstanceId>>,
    pub shutdown_token: CancellationToken,
    pub pinning_heartbeat: NonZeroDuration,
    pub pinning_ttl: NonZeroDuration,
}

pub(super) async fn run_maintenance_loop<Backend>(
    params: MaintainParams<Backend>,
) -> Result<
    (),
    Error<
        <Backend as waymark_workload_pinning_backend::PollUnpinnedInstances>::Error,
        <Backend as KeepaliveInstancePinnings>::Error,
        <Backend as ReleasePinnings>::Error,
    >,
>
where
    Backend: waymark_workload_pinning_backend::PollUnpinnedInstances,
    Backend: KeepaliveInstancePinnings<Timestamp = chrono::DateTime<chrono::Utc>>,
    Backend: ReleasePinnings,
    Backend::NodeId: Clone,
    Backend::InstanceId: Clone + std::hash::Hash + Eq,
{
    let MaintainParams {
        backend,
        node_id,
        mut batch_rx,
        mut evict_rx,
        count_tx,
        cleanup_tx,
        shutdown_token,
        pinning_heartbeat,
        pinning_ttl,
    } = params;

    let mut active_ids: HashSet<Backend::InstanceId> = HashSet::new();
    let shutdown = shutdown_token.child_token().cancelled_owned();
    let mut shutdown = std::pin::pin!(shutdown);
    let mut heartbeat_tick = tokio::time::interval(pinning_heartbeat.get());
    heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let result = loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("maintenance loop shutting down");
                break Ok(());
            }
            Some((ids, reply)) = batch_rx.recv() => {
                for id in ids.iter() {
                    active_ids.insert(id.clone());
                }
                let _ = reply.send(active_ids.len());
            }
            Some(id) = evict_rx.recv() => {
                active_ids.remove(&id);
                let _ = count_tx.send(active_ids.len());
                if let Err(error) = (*backend).release_pinnings(node_id.clone(), NEVec::new(id)).await {
                    break Err(Error::Release(error));
                }
            }
            _ = heartbeat_tick.tick() => {
                let Some(ids) = active_ids.iter().cloned().try_into_nonempty_iter() else {
                    continue;
                };
                if let Err(error) = refresh_active_pinnings(&*backend, node_id.clone(), ids, pinning_ttl).await {
                    break Err(Error::Refresh(error));
                }
            }
        }
    };

    let _ = cleanup_tx.send(active_ids);
    result
}

/// Refresh pinnings on all active workloads.
pub(super) async fn refresh_active_pinnings<Backend>(
    backend: &Backend,
    node_id: Backend::NodeId,
    ids: impl nonempty_collections::IntoNonEmptyIterator<Item = Backend::InstanceId>,
    pinning_ttl: NonZeroDuration,
) -> Result<(), <Backend as KeepaliveInstancePinnings>::Error>
where
    Backend: KeepaliveInstancePinnings<Timestamp = chrono::DateTime<chrono::Utc>>,
{
    let expires_at = Utc::now() + chrono::Duration::from_std(pinning_ttl.get()).unwrap();

    let pinning = Pinning {
        node_id,
        expires_at,
    };

    backend.refresh_pinnings(pinning, ids).await?;

    debug!("refreshed workload pinnings");

    Ok(())
}

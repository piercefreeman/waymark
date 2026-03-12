use std::collections::HashSet;

use uuid::Uuid;

use crate::{
    commit_barrier::CommitBarrier,
    runloop::{ShardStep, channel_utils::send_with_stop},
};

pub struct Params<'a> {
    pub shutdown_signal: tokio_util::sync::WaitForCancellationFuture<'a>,
    pub persist_tx: &'a tokio::sync::mpsc::Sender<crate::runloop::PersistCommand>,
    pub commit_barrier: &'a mut CommitBarrier<ShardStep>,
    pub all_steps: Vec<ShardStep>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to submit persist batch to persistence task")]
    SubmittingPersistBatch,
}

/// Routes completed shard steps through the commit barrier and into persistence.
///
/// **Why this part exists:** When shards emit steps (graph updates, action completion acks),
/// they must be atomically persisted alongside other state changes. The commit barrier
/// ensures that instances blocked by unmet prerequisites defer their steps until ready.
///
/// **What it does:**
/// - Collects action completion acks and graph updates from all steps
/// - Separates graph updates by instance (some instances may have updates, others not)
/// - Routes through commit barrier to identify which steps can proceed immediately
///   vs. which must defer until commits happen
/// - Submits persisted/deferred steps to a persistence task for atomic durability
/// - Returns errors only if communication with the persistence task fails
pub async fn handle(params: Params<'_>) -> Result<(), Error> {
    let Params {
        commit_barrier,
        shutdown_signal,
        persist_tx,
        all_steps,
    } = params;

    if all_steps.is_empty() {
        return Ok(());
    }

    let instance_ids: HashSet<Uuid> = all_steps.iter().map(|step| step.executor_id).collect();
    let (actions_done, graph_updates) = collect_step_updates(&all_steps);
    let graph_instance_ids: HashSet<Uuid> = graph_updates
        .iter()
        .map(|update| update.instance_id)
        .collect();
    let batch_id = commit_barrier.register_batch(instance_ids.clone(), all_steps);

    let sent = send_with_stop(
        persist_tx,
        crate::runloop::PersistCommand {
            batch_id,
            instance_ids,
            graph_instance_ids,
            actions_done,
            graph_updates,
        },
        shutdown_signal,
        "persist command",
    )
    .await;
    if !sent {
        if let Some(batch) = commit_barrier.take_batch(batch_id) {
            for instance_id in batch.instance_ids {
                commit_barrier.remove_instance(instance_id);
            }
        }
        return Err(Error::SubmittingPersistBatch);
    }

    Ok(())
}

fn collect_step_updates(
    steps: &[ShardStep],
) -> (
    Vec<waymark_core_backend::ActionDone>,
    Vec<waymark_core_backend::GraphUpdate>,
) {
    let mut actions_done: Vec<waymark_core_backend::ActionDone> = Vec::new();
    let mut graph_updates: Vec<waymark_core_backend::GraphUpdate> = Vec::new();
    for step in steps {
        if let Some(updates) = &step.updates {
            if !updates.actions_done.is_empty() {
                actions_done.extend(updates.actions_done.clone());
            }
            if !updates.graph_updates.is_empty() {
                graph_updates.extend(updates.graph_updates.clone());
            }
        }
    }
    (actions_done, graph_updates)
}

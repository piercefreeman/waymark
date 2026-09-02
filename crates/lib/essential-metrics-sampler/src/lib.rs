//! The essential-metrics sampler: a `metrics::Recorder` that materializes
//! exactly the bound metric names, and the task that snapshots them into
//! `NodeSample`s on an interval.
//!
//! The recorder joins the process-global fanout; every metric outside the
//! binding gets no-op handles and costs nothing here.

#![warn(missing_docs)]

use std::sync::Arc;

use waymark_essential_metrics_core::NodeSample;
use waymark_nonzero_duration::NonZeroDuration;

pub mod bindings;
mod counter;
mod gauge;
mod histogram;
pub mod recorder;
pub mod summary;

pub use recorder::Recorder;

/// One cell per bound metric.
#[derive(Debug)]
struct Cells {
    worker_pool_size: Arc<gauge::Cell>,
    max_in_flight_actions: Arc<gauge::Cell>,
    queued_action_dispatches: Arc<gauge::Cell>,
    last_action_completed: Arc<gauge::Cell>,
    actions_acquired: Arc<counter::Cell>,
    actions_released: Arc<counter::Cell>,
    instances_revived: Arc<counter::Cell>,
    instances_evicted: Arc<counter::Cell>,
    actions_completed: Arc<counter::Cell>,
    dropped: Arc<counter::Cell>,
    action_dequeue_seconds: Arc<
        histogram::Cell<
            { waymark_essential_metrics_core::ACTION_DEQUEUE_SECONDS_BOUNDS.len() + 1 },
        >,
    >,
    action_handling_seconds: Arc<
        histogram::Cell<
            { waymark_essential_metrics_core::ACTION_HANDLING_SECONDS_BOUNDS.len() + 1 },
        >,
    >,
}

impl Default for Cells {
    fn default() -> Self {
        Self {
            worker_pool_size: Arc::default(),
            max_in_flight_actions: Arc::default(),
            queued_action_dispatches: Arc::default(),
            last_action_completed: Arc::default(),
            actions_acquired: Arc::default(),
            actions_released: Arc::default(),
            instances_revived: Arc::default(),
            instances_evicted: Arc::default(),
            actions_completed: Arc::default(),
            dropped: Arc::default(),
            action_dequeue_seconds: Arc::new(histogram::Cell::new(
                &waymark_essential_metrics_core::ACTION_DEQUEUE_SECONDS_BOUNDS,
            )),
            action_handling_seconds: Arc::new(histogram::Cell::new(
                &waymark_essential_metrics_core::ACTION_HANDLING_SECONDS_BOUNDS,
            )),
        }
    }
}

/// The sampling task: every `sample_interval` it snapshots `handle`'s
/// cells into a [`NodeSample`] and pushes it into `batcher`; it ends when
/// `shutdown` resolves.
pub async fn run<NodeId, Shutdown>(
    handle: recorder::Handle,
    node_id: NodeId,
    sample_interval: NonZeroDuration,
    batcher: waymark_lossy_batcher::BatcherHandle<NodeSample<NodeId>>,
    shutdown: Shutdown,
) where
    NodeId: Clone,
    Shutdown: Future<Output = ()>,
{
    let mut ticks = tokio::time::interval(sample_interval.get());
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The first tick fires immediately; skip it so the first sample
    // covers a full interval.
    ticks.tick().await;
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => break,
            _ = ticks.tick() => batcher.push(recorder::sample(&handle, node_id.clone())),
        }
    }
}

#[cfg(test)]
mod tests;

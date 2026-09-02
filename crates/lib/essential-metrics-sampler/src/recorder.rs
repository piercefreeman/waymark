//! The `metrics::Recorder` and the read handle on it.

use std::sync::Arc;

use crate::{Cells, bindings};

/// The sampler's `metrics::Recorder`: bound names get live handles,
/// everything else gets no-ops. Permits recording into the cells —
/// only its paired [`Handle`] can read them.
#[derive(Debug)]
pub struct Recorder {
    cells: Arc<Cells>,
}

/// Read access to a [`Recorder`]'s cells. Permits sampling — it cannot
/// record.
///
/// Sampling drains the histogram cells, so a handle backs exactly one
/// sampler: two would divide each interval's observations between them
/// and neither would see the node whole.
#[derive(Debug)]
pub struct Handle {
    cells: Arc<Cells>,
}

/// Snapshot `handle`'s cells into one sample.
///
/// Counters and gauges are read as they stand — they carry their own
/// since-boot or point-in-time meaning. The histograms are drained, so
/// their quantiles describe the interval since the previous call and
/// nothing before it.
pub(crate) fn sample<NodeId>(
    handle: &Handle,
    node_id: NodeId,
) -> waymark_essential_metrics_core::NodeSample<NodeId> {
    let cells = &handle.cells;
    let acquired = cells.actions_acquired.get();
    let released = cells.actions_released.get();
    let revived = cells.instances_revived.get();
    let evicted = cells.instances_evicted.get();
    waymark_essential_metrics_core::NodeSample {
        node_id,
        sampled_at: chrono::Utc::now(),
        worker_pool_size: cells.worker_pool_size.get() as u64,
        max_in_flight_actions: cells.max_in_flight_actions.get() as u64,
        // The pair is read non-atomically; a release slipping between the
        // reads must not underflow.
        in_flight_actions: acquired.saturating_sub(released),
        queued_action_dispatches: cells.queued_action_dispatches.get() as u64,
        driven_vm_runtimes: revived.saturating_sub(evicted),
        actions_completed_total: cells.actions_completed.get(),
        last_action_completed_at: last_action_completed_at(cells.last_action_completed.get()),
        action_dequeue_seconds: cells.action_dequeue_seconds.drain(),
        action_handling_seconds: cells.action_handling_seconds.drain(),
        essential_metrics_dropped_total: cells.dropped.get(),
    }
}

/// Decode the last-action gauge — unix seconds of the last completion,
/// with the cell's `0.0` default meaning "no completion yet".
fn last_action_completed_at(unix_seconds: f64) -> Option<chrono::DateTime<chrono::Utc>> {
    if unix_seconds <= 0.0 {
        return None;
    }
    chrono::DateTime::from_timestamp_micros((unix_seconds * 1e6) as i64)
}

/// Create a linked [`Recorder`]/[`Handle`] pair over one set of cells:
/// exactly one side that records, exactly one that samples.
pub fn new() -> (Recorder, Handle) {
    let cells = Arc::new(Cells::default());
    let recorder = Recorder {
        cells: Arc::clone(&cells),
    };
    let handle = Handle { cells };
    (recorder, handle)
}

impl metrics::Recorder for Recorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn register_counter(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Counter {
        let cell = match key.name() {
            bindings::ACTIONS_ACQUIRED => &self.cells.actions_acquired,
            bindings::ACTIONS_RELEASED => &self.cells.actions_released,
            bindings::INSTANCES_REVIVED => &self.cells.instances_revived,
            bindings::INSTANCES_EVICTED => &self.cells.instances_evicted,
            bindings::ACTIONS_COMPLETED => &self.cells.actions_completed,
            bindings::LOSSY_BATCHER_DROPPED
                if key.labels().any(|label| {
                    label.key() == "batcher" && label.value() == bindings::BATCHER_NAME
                }) =>
            {
                &self.cells.dropped
            }
            _ => return metrics::Counter::noop(),
        };
        metrics::Counter::from_arc(Arc::clone(cell))
    }

    fn register_gauge(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        let cell = match key.name() {
            bindings::WORKER_POOL_SIZE => &self.cells.worker_pool_size,
            bindings::MAX_IN_FLIGHT_ACTIONS => &self.cells.max_in_flight_actions,
            bindings::QUEUED_ACTION_DISPATCHES => &self.cells.queued_action_dispatches,
            bindings::LAST_ACTION_COMPLETED => &self.cells.last_action_completed,
            _ => return metrics::Gauge::noop(),
        };
        metrics::Gauge::from_arc(Arc::clone(cell))
    }

    fn register_histogram(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        // Each bucketed cell is its own type, one per bound count, so the
        // arms erase to the handle separately rather than through a
        // shared binding.
        match key.name() {
            bindings::ACTION_DEQUEUE_SECONDS => {
                metrics::Histogram::from_arc(Arc::clone(&self.cells.action_dequeue_seconds))
            }
            bindings::ACTION_HANDLING_SECONDS => {
                metrics::Histogram::from_arc(Arc::clone(&self.cells.action_handling_seconds))
            }
            _ => metrics::Histogram::noop(),
        }
    }
}

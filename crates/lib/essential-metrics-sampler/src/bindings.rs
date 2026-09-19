//! The binding of `NodeSample` fields to metric names on the `metrics::*`
//! stream.
//!
//! This is the single place where the two vocabularies meet: the sampler
//! materializes exactly these names; nothing downstream of the sample
//! (stores, query backends, the API) knows them.

/// Gauge behind `NodeSample::worker_pool_size`.
pub const WORKER_POOL_SIZE: &str = "waymark_worker_process_pool_workers";

/// Gauge behind `NodeSample::max_in_flight_actions`.
pub const MAX_IN_FLIGHT_ACTIONS: &str = "waymark_worker_process_pool_action_capacity";

/// Counter minuend of `NodeSample::in_flight_actions`.
pub const ACTIONS_ACQUIRED: &str = "waymark_worker_process_pool_actions_acquired_total";

/// Counter subtrahend of `NodeSample::in_flight_actions`.
pub const ACTIONS_RELEASED: &str = "waymark_worker_process_pool_actions_released_total";

/// Gauge behind `NodeSample::queued_action_dispatches`.
pub const QUEUED_ACTION_DISPATCHES: &str = "waymark_worker_remote_pool_dispatch_queue_length";

/// Counter minuend of `NodeSample::driven_vm_runtimes`.
pub const INSTANCES_REVIVED: &str = "waymark_execution_driver_instances_revived_total";

/// Counter subtrahend of `NodeSample::driven_vm_runtimes`.
pub const INSTANCES_EVICTED: &str = "waymark_execution_driver_instances_evicted_total";

/// Counter behind `NodeSample::actions_completed_total`.
pub const ACTIONS_COMPLETED: &str = "waymark_worker_process_pool_actions_completed_total";

/// Gauge behind `NodeSample::last_action_completed_at`: unix seconds of the last
/// completion.
pub const LAST_ACTION_COMPLETED: &str =
    "waymark_worker_process_pool_last_action_completed_timestamp_seconds";

/// Histogram behind `NodeSample::action_dequeue_seconds`.
pub const ACTION_DEQUEUE_SECONDS: &str =
    "waymark_worker_remote_execute_remote_request_worker_wait_seconds";

/// Histogram behind `NodeSample::action_handling_seconds`.
pub const ACTION_HANDLING_SECONDS: &str = "waymark_worker_remote_pool_action_handling_seconds";

/// Counter behind `NodeSample::essential_metrics_dropped_total`, filtered
/// to the `batcher` label value [`BATCHER_NAME`], all `reason`s summed.
pub const LOSSY_BATCHER_DROPPED: &str = "waymark_lossy_batcher_dropped_total";

/// The `batcher` label value of the essential-metrics pipeline's own
/// lossy batcher.
pub const BATCHER_NAME: &str = "essential_metrics";

use nonempty_collections::IntoNonEmptyIterator;
use waymark_ids::InstanceId;
use waymark_runner_executor_core::CheckedExecutionResult;

/// The ability to mark a workflow instance as completed, release
/// the held locks and clean up the associated execution state.
pub trait InstanceQueueCompletion {
    /// Error returned when finalizing completed instances fails.
    type Error;

    /// Persist completed workflow instances.
    fn save_instances_done<'a>(
        &'a self,
        instances: impl IntoNonEmptyIterator<Item = InstanceDone> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Completed instance payload with result or exception.
#[derive(Clone, Debug)]
pub struct InstanceDone {
    /// Identifier of the completed workflow instance.
    pub executor_id: InstanceId,

    /// Final workflow result or terminal exception.
    pub result: CheckedExecutionResult,
}

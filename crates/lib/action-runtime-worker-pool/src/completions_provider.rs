use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use waymark_action_runtime_core::ActionCallCompletion;
use waymark_convert_core::TryConvert as _;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Provides action outcomes by polling a [`waymark_worker_core::BaseWorkerPool`].
pub struct WorkerPoolCompletionsProvider<Pool> {
    /// The worker pool to poll for completed actions.
    pub pool: Pool,
}

/// Errors that can occur when waiting for action completions from
/// the worker pool.
#[derive(Debug, thiserror::Error)]
pub enum WaitForCompletionsError {
    /// The worker pool has shut down and can no longer provide
    /// completions.
    #[error("worker pool gone")]
    WorkerPoolGone,

    /// Failed to convert a raw worker-pool result into an
    /// [`waymark_action_runtime_core::ActionCallOutcome`].
    #[error("action result conversion: {0}")]
    ActionResultConversion(
        #[source]
        waymark_convert_core::ConvertErrorFor<
            waymark_action_runtime_convert::Converter,
            waymark_runner_executor_core::UncheckedExecutionResult,
            waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value::ReadyValue>,
        >,
    ),
}

impl<Pool> waymark_action_runtime_core::ActionCallCompletionsProvider
    for WorkerPoolCompletionsProvider<Pool>
where
    Pool: waymark_worker_core::BaseWorkerPool + Send + Sync + 'static,
{
    type Value = waymark_vm_value::ReadyValue;
    type Error = WaitForCompletionsError;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletion<Self::Value>>, Self::Error> {
        let maybe_completions = self.pool.poll_complete().await;

        let completions = maybe_completions.ok_or(WaitForCompletionsError::WorkerPoolGone)?;

        completions
            .into_nonempty_iter()
            .map(|completion| {
                let waymark_worker_core::ActionCompletion {
                    executor_id: _,
                    execution_id: _,
                    attempt_number: _,
                    dispatch_token: _,
                    result,
                } = completion;

                let outcome = waymark_action_runtime_convert::Converter::try_convert(result)
                    .map_err(WaitForCompletionsError::ActionResultConversion)?;

                Ok(ActionCallCompletion {
                    // TODO: fix effect number and promise state id
                    effect_number: EffectNumber(0),
                    promise_state_id: PromiseStateId(0),
                    outcome,
                })
            })
            .collect()
    }
}

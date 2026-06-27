use std::marker::PhantomData;

use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use waymark_action_runtime_core::ActionCallOutcome;
use waymark_convert_core::{Convert, TryConvert};
use waymark_runner_executor_core::{ExecutionException, ExecutionSuccess};

/// Provides action outcomes by polling a [`waymark_worker_core::BaseWorkerPool`].
pub struct WorkerPoolOutcomesProvider<Pool, OutcomeConverter> {
    /// The worker pool to poll for completed actions.
    pub pool: Pool,

    phantom_data: PhantomData<OutcomeConverter>,
}

/// Default converter from [`waymark_worker_core::ActionCompletion`]
/// to [`waymark_action_runtime_core::ActionCallOutcome`].
pub struct DefaultOutcomeConverter;

impl
    waymark_convert_core::TryConvert<
        waymark_worker_core::ActionCompletion,
        ActionCallOutcome<waymark_vm_value::ReadyValue>,
    > for DefaultOutcomeConverter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        completion: waymark_worker_core::ActionCompletion,
    ) -> Result<ActionCallOutcome<waymark_vm_value::ReadyValue>, Self::Error> {
        match completion.result.check() {
            Ok(ExecutionSuccess(success)) => {
                let value = waymark_extcall_convert::Converter::convert(success);
                Ok(ActionCallOutcome::Value(value))
            }
            Err(ExecutionException(exception)) => {
                let error_type = exception
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("ActionError")
                    .to_owned();
                let details = waymark_extcall_convert::Converter::convert(exception);
                Ok(ActionCallOutcome::Exception(
                    waymark_vm_runtime_exception::Exception {
                        type_id: error_type,
                        details,
                    },
                ))
            }
        }
    }
}

impl<Pool, OutcomeConverter> waymark_action_runtime_core::ActionCallOutcomesProvider
    for WorkerPoolOutcomesProvider<Pool, OutcomeConverter>
where
    Pool: waymark_worker_core::BaseWorkerPool + Send + Sync + 'static,
    OutcomeConverter: TryConvert<
            waymark_worker_core::ActionCompletion,
            ActionCallOutcome<waymark_vm_value::ReadyValue>,
        >,
    OutcomeConverter: Send + 'static,
    OutcomeConverter::Error: core::fmt::Display,
{
    type Value = waymark_vm_value::ReadyValue;
    type Error = waymark_worker_core::WorkerPoolError;

    async fn wait_for_outcomes(
        &mut self,
    ) -> Result<NEVec<ActionCallOutcome<Self::Value>>, Self::Error> {
        loop {
            let Some(completions) = self.pool.poll_complete().await else {
                continue;
            };

            return completions
                .into_nonempty_iter()
                .map(|completion| {
                    OutcomeConverter::try_convert(completion).map_err(|err| {
                        waymark_worker_core::WorkerPoolError::new(
                            "WorkerPoolOutcomesProvider",
                            format!("outcome conversion: {err}"),
                        )
                    })
                })
                .collect::<Result<NEVec<_>, _>>();
        }
    }
}

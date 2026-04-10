use std::sync::Arc;

use dashmap::DashMap;
use waymark_runner_executor_core::UncheckedExecutionResult;
use waymark_vcr_core::CorrelationId;
use waymark_vcr_playback_worker_pool_core::execution_correlator::CorrelatedActionCompletion;
use waymark_worker_core::ActionRequest;

#[derive(Debug)]
struct PreparedCorrelation {
    pub delay: std::time::Duration,
    pub result: UncheckedExecutionResult,
    pub params: waymark_vcr_file::action::Params,
}

#[derive(Debug, Default)]
pub struct ExecutionCorrelator {
    prepared_correlations: DashMap<CorrelationId, PreparedCorrelation>,
}

#[derive(Debug)]
pub struct PrepHandle(Arc<ExecutionCorrelator>);

impl ExecutionCorrelator {
    pub fn prep_handle(self: &Arc<Self>) -> PrepHandle {
        PrepHandle(Arc::clone(self))
    }
}

impl PrepHandle {
    pub fn prepare_correlation(
        &self,
        id: CorrelationId,
        log_item: waymark_vcr_file::action::LogItem,
    ) {
        let waymark_vcr_file::action::LogItem {
            execution_time,
            params,
            result,
        } = log_item;

        let prepared_correlation = PreparedCorrelation {
            delay: execution_time,
            params,
            result,
        };

        self.0
            .prepared_correlations
            .insert(id, prepared_correlation);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("execution to correlate not found")]
pub struct NotFoundError;

impl waymark_vcr_playback_worker_pool_core::ExecutionCorrelator for ExecutionCorrelator {
    type Error = NotFoundError;

    fn correlate(&self, request: ActionRequest) -> Result<CorrelatedActionCompletion, Self::Error> {
        let (id, params, dispatch_token) = waymark_vcr_file::action::deconstruct_request(request);
        let (_, prepared_correlation) = self
            .prepared_correlations
            .remove(&id)
            .ok_or(NotFoundError)?;

        let PreparedCorrelation {
            delay,
            params: expected_params,
            result,
        } = prepared_correlation;

        if expected_params != params {
            // We want to pay attention to this, so this is a panic for now.
            panic!(
                "correlation assumption violated;\nexpected:\n{params:?}\nactual:\n{expected_params:?}"
            );
        }

        let CorrelationId {
            executor_id,
            execution_id,
            attempt_number,
        } = id;

        let completion = waymark_worker_core::ActionCompletion {
            executor_id,
            execution_id,
            attempt_number,
            dispatch_token,
            result,
        };

        Ok(CorrelatedActionCompletion { completion, delay })
    }
}

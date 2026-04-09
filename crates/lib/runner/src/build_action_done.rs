use chrono::{DateTime, Utc};
use waymark_core_backend::{ActionAttemptStatus, ActionDone};
use waymark_ids::ExecutionId;
use waymark_runner_executor_core::CheckedExecutionResult;

pub fn build_action_done(
    execution_id: ExecutionId,
    attempt: i32,
    status: ActionAttemptStatus,
    started_at: Option<DateTime<Utc>>,
    completed_at: DateTime<Utc>,
    result: CheckedExecutionResult,
) -> ActionDone {
    ActionDone {
        execution_id,
        attempt,
        status,
        started_at,
        completed_at: Some(completed_at),
        duration_ms: compute_action_duration_ms(started_at, completed_at),
        result: waymark_runner_executor_core::uncheck_execution_result(result),
    }
}

fn compute_action_duration_ms(
    started_at: Option<DateTime<Utc>>,
    completed_at: DateTime<Utc>,
) -> Option<i64> {
    started_at
        .map(|started_at| {
            completed_at
                .signed_duration_since(started_at)
                .num_milliseconds()
        })
        .filter(|duration| *duration >= 0)
}

#[cfg(test)]
mod tests {
    use waymark_runner_executor_core::ExecutionSuccess;

    use super::*;

    #[test]
    fn test_build_action_done_sets_duration_from_started_and_completed() {
        let execution_id = ExecutionId::new_uuid_v4();
        let started_at = Utc::now();
        let completed_at = started_at + chrono::Duration::milliseconds(275);
        let done = build_action_done(
            execution_id,
            2,
            ActionAttemptStatus::Completed,
            Some(started_at),
            completed_at,
            ExecutionSuccess(serde_json::json!({"ok": true})).into(),
        );

        assert_eq!(done.execution_id, execution_id);
        assert_eq!(done.attempt, 2);
        assert_eq!(done.status, ActionAttemptStatus::Completed);
        assert_eq!(done.started_at, Some(started_at));
        assert_eq!(done.completed_at, Some(completed_at));
        assert_eq!(done.duration_ms, Some(275));
    }
}

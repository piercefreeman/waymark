use waymark_core_backend::ActionAttemptStatus;
use waymark_runner_executor_core::ExecutionException;

pub fn for_exception(value: &ExecutionException) -> ActionAttemptStatus {
    match waymark_synthetic_exception::Type::from_value(&value.0) {
        Some(waymark_synthetic_exception::Type::ExecutorResume)
        | Some(waymark_synthetic_exception::Type::ActionTimeout) => ActionAttemptStatus::TimedOut,
        Some(waymark_synthetic_exception::Type::RunnerExecutorError) | None => {
            ActionAttemptStatus::Failed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_done_status_for_resume_exception_is_timed_out() {
        let value = ExecutionException(serde_json::json!({
            "type": "ExecutorResume",
            "message": "resumed action timed out",
        }));
        assert_eq!(for_exception(&value), ActionAttemptStatus::TimedOut);
    }

    #[test]
    fn test_action_done_status_for_action_timeout_exception_is_timed_out() {
        let value = ExecutionException(serde_json::json!({
            "type": "ActionTimeout",
            "message": "action timed out",
            "timeout_seconds": 1,
            "attempt": 1,
        }));
        assert_eq!(for_exception(&value), ActionAttemptStatus::TimedOut);
    }

    #[test]
    fn test_action_done_status_for_generic_exception_is_failed() {
        let value = ExecutionException(serde_json::json!({
            "type": "ValueError",
            "message": "boom",
        }));
        assert_eq!(for_exception(&value), ActionAttemptStatus::Failed);
    }

    #[test]
    fn test_action_done_status_for_non_synthetic_timeout_error_is_failed() {
        let value = ExecutionException(serde_json::json!({
            "type": "TimeoutError",
            "message": "user action raised timeout",
        }));
        assert_eq!(for_exception(&value), ActionAttemptStatus::Failed);
    }
}

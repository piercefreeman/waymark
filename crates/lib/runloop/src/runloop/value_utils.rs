use uuid::Uuid;

pub fn action_timeout_value(
    execution_id: Uuid,
    attempt_number: u32,
    timeout_seconds: u32,
) -> serde_json::Value {
    waymark_runner::synthetic_exceptions::build_synthetic_exception_value(
        waymark_runner::synthetic_exceptions::SyntheticExceptionType::ActionTimeout,
        format!(
            "action {execution_id} attempt {attempt_number} timed out after {timeout_seconds}s"
        ),
        vec![
            (
                "timeout_seconds".to_string(),
                serde_json::Value::Number(serde_json::Number::from(timeout_seconds)),
            ),
            (
                "attempt".to_string(),
                serde_json::Value::Number(serde_json::Number::from(attempt_number)),
            ),
        ],
    )
}

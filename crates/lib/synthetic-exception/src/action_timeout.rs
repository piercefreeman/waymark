#[derive(Debug, Clone)]
pub struct ActionTimeout {
    pub execution_id: waymark_ids::ExecutionId,
    pub attempt_number: u32,
    pub timeout_seconds: u32,
}

impl core::fmt::Display for ActionTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            execution_id,
            attempt_number,
            timeout_seconds,
        } = self;
        write!(
            f,
            "action {execution_id} attempt {attempt_number} timed out after {timeout_seconds}s"
        )
    }
}

impl From<&ActionTimeout> for crate::Value {
    fn from(value: &ActionTimeout) -> Self {
        let message = value.to_string();
        let ActionTimeout {
            attempt_number,
            timeout_seconds,
            execution_id: _,
        } = value;
        Self {
            r#type: "ActionTimeout".into(),
            message,
            fields: [
                (
                    "timeout_seconds",
                    serde_json::Value::Number(serde_json::Number::from(*timeout_seconds)),
                ),
                (
                    "attempt",
                    serde_json::Value::Number(serde_json::Number::from(*attempt_number)),
                ),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        }
    }
}

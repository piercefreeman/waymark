#[derive(Debug, Clone)]
pub struct ExecutorResume {
    pub node_id: waymark_ids::ExecutionId,
}

impl core::fmt::Display for ExecutorResume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { node_id } = self;
        write!(
            f,
            "action {node_id} was running during resume and is treated as failed"
        )
    }
}

impl From<&ExecutorResume> for crate::Value {
    fn from(value: &ExecutorResume) -> Self {
        let message = value.to_string();
        let ExecutorResume { node_id: _ } = value;
        Self {
            r#type: "ExecutorResume".into(),
            message,
            fields: Default::default(),
        }
    }
}

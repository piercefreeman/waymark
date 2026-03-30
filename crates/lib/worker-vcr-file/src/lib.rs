use std::collections::HashMap;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ActionParams {
    pub action_name: String,
    pub module_name: Option<String>,
    pub kwargs: HashMap<String, serde_json::Value>,
    pub timeout_seconds: u32,
}

impl From<waymark_worker_core::ActionRequest> for ActionParams {
    fn from(value: waymark_worker_core::ActionRequest) -> Self {
        Self {
            action_name: value.action_name,
            module_name: value.module_name,
            kwargs: value.kwargs,
            timeout_seconds: value.timeout_seconds,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LogItem {
    pub execution_time: std::time::Duration,
    pub params: ActionParams,
    pub result: serde_json::Value,
}

pub type Reader = waymark_jsonlines::Reader<LogItem>;
pub type Writer = waymark_jsonlines::Writer<LogItem>;

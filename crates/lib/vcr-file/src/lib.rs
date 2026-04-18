pub mod action;
pub mod instance;
pub mod workflow_version;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum LogItem {
    Instance(instance::LogItem),
    WorkflowVersion(workflow_version::LogItem),
}

pub type Reader = waymark_jsonlines::Reader<tokio::io::BufReader<tokio::fs::File>, LogItem>;
pub type Writer = waymark_jsonlines::Writer<tokio::fs::File, LogItem>;

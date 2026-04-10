use waymark_ids::WorkflowVersionId;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LogItem {
    pub workflow_version_id: WorkflowVersionId,
    pub actions: Vec<crate::action::LogItem>,
}

use waymark_ids::WorkflowVersionId;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LogItem {
    pub id: WorkflowVersionId,
    pub workflow_name: String,
    pub workflow_version: String,
    pub ir_hash: String,
    pub program_proto: Vec<u8>,
    pub concurrent: bool,
}

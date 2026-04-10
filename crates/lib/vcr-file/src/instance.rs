#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LogItem {
    pub actions: Vec<crate::action::LogItem>,
}

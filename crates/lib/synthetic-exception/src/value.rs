#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Value {
    #[serde(rename = "type")]
    pub r#type: String,

    pub message: String,

    #[serde(flatten)]
    pub fields: serde_json::Map<String, serde_json::Value>,
}

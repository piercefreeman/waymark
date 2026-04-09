#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Value {
    #[serde(rename = "type")]
    pub r#type: String,

    pub message: String,

    #[serde(flatten)]
    pub fields: serde_json::Map<String, serde_json::Value>,
}

impl Value {
    pub fn new(r#type: &'static str, message: impl core::fmt::Display) -> Self {
        Self::with_fields(r#type, message, Default::default())
    }

    pub fn with_fields(
        r#type: &'static str,
        message: impl core::fmt::Display,
        fields: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        Self {
            r#type: r#type.into(),
            message: message.to_string(),
            fields,
        }
    }
}

pub fn error_value(kind: &str, message: &str) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    map.insert(
        "type".to_string(),
        serde_json::Value::String(kind.to_string()),
    );
    map.insert(
        "message".to_string(),
        serde_json::Value::String(message.to_string()),
    );
    serde_json::Value::Object(map)
}

pub fn is_exception_value(value: &serde_json::Value) -> bool {
    if let serde_json::Value::Object(map) = &value {
        return map.contains_key("type") && map.contains_key("message");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_exception_value_happy_path() {
        let value = serde_json::json!({
            "type": "RuntimeError",
            "message": "bad",
        });
        assert!(is_exception_value(&value));
    }
}

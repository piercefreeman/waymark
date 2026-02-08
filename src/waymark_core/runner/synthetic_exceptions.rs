//! Synthetic exception helpers produced by Rust runtime coordination paths.

use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SyntheticExceptionType {
    ExecutorResume,
    ActionTimeout,
}

impl SyntheticExceptionType {
    pub(crate) fn as_type_str(self) -> &'static str {
        match self {
            Self::ExecutorResume => "ExecutorResume",
            Self::ActionTimeout => "ActionTimeout",
        }
    }

    fn from_type_str(value: &str) -> Option<Self> {
        match value {
            "ExecutorResume" => Some(Self::ExecutorResume),
            "ActionTimeout" => Some(Self::ActionTimeout),
            _ => None,
        }
    }

    pub(crate) fn from_value(value: &Value) -> Option<Self> {
        let Value::Object(map) = value else {
            return None;
        };
        map.get("type")
            .and_then(Value::as_str)
            .and_then(Self::from_type_str)
    }
}

pub(crate) fn build_synthetic_exception_value(
    exception_type: SyntheticExceptionType,
    message: impl Into<String>,
    fields: Vec<(String, Value)>,
) -> Value {
    let mut map = serde_json::Map::new();
    map.insert(
        "type".to_string(),
        Value::String(exception_type.as_type_str().to_string()),
    );
    map.insert("message".to_string(), Value::String(message.into()));
    for (key, value) in fields {
        map.insert(key, value);
    }
    Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_exception_from_value_happy_path() {
        let value = serde_json::json!({"type": "ActionTimeout", "message": "x"});
        assert_eq!(
            SyntheticExceptionType::from_value(&value),
            Some(SyntheticExceptionType::ActionTimeout)
        );
    }

    #[test]
    fn build_synthetic_exception_value_happy_path() {
        let value = build_synthetic_exception_value(
            SyntheticExceptionType::ExecutorResume,
            "resume",
            vec![(
                "attempt".to_string(),
                Value::Number(serde_json::Number::from(2)),
            )],
        );
        let Value::Object(map) = value else {
            panic!("expected object value");
        };
        assert_eq!(
            map.get("type"),
            Some(&Value::String("ExecutorResume".to_string()))
        );
        assert_eq!(
            map.get("message"),
            Some(&Value::String("resume".to_string()))
        );
        assert_eq!(map.get("attempt"), Some(&Value::Number(2.into())));
    }
}

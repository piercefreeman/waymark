//! Synthetic exception helpers produced by Rust runtime coordination paths.

mod action_timeout;
mod any;
mod executor_resume;
mod value;

pub use self::action_timeout::*;
pub use self::any::*;
pub use self::executor_resume::*;
pub use self::value::*;

pub fn build_value(exception: impl Into<Value>) -> serde_json::Value {
    let value = exception.into();
    serde_json::to_value(value).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_value_happy_path() {
        let value = build_value(Value {
            r#type: "ExecutorResume".into(),
            message: "resume".into(),
            fields: [(
                "attempt".to_string(),
                serde_json::Value::Number(serde_json::Number::from(2)),
            )]
            .into_iter()
            .collect(),
        });

        let serde_json::Value::Object(map) = value else {
            panic!("expected object value");
        };
        assert_eq!(
            map.get("type"),
            Some(&serde_json::Value::String("ExecutorResume".to_string()))
        );
        assert_eq!(
            map.get("message"),
            Some(&serde_json::Value::String("resume".to_string()))
        );
        assert_eq!(
            map.get("attempt"),
            Some(&serde_json::Value::Number(2.into()))
        );
    }
}

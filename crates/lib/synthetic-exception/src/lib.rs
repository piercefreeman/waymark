//! Synthetic exception helpers produced by Rust runtime coordination paths.

mod action_timeout;
mod any;
mod executor_resume;
mod value;

use waymark_runner_executor_core::ExecutionException;

pub use self::action_timeout::*;
pub use self::any::*;
pub use self::executor_resume::*;
pub use self::value::*;

pub fn build(exception: impl Into<Value>) -> ExecutionException {
    let value = exception.into();
    ExecutionException(serde_json::to_value(value).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_value_happy_path() {
        let ExecutionException(value) = build(Value {
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

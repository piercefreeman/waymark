//! The in-process Rust actions served by the inline worker pool.

use std::collections::HashMap;

use serde_json::Value;
use waymark_worker_core::WorkerPoolError;
use waymark_worker_inline::ActionCallable;

async fn action_double(kwargs: HashMap<String, Value>) -> Result<Value, WorkerPoolError> {
    let value = kwargs
        .get("value")
        .and_then(|value| value.as_i64())
        .ok_or_else(|| WorkerPoolError::new("ActionError", "double expects integer value"))?;
    Ok(Value::Number((value * 2).into()))
}

async fn action_sum(kwargs: HashMap<String, Value>) -> Result<Value, WorkerPoolError> {
    let values = kwargs
        .get("values")
        .and_then(|value| value.as_array())
        .ok_or_else(|| WorkerPoolError::new("ActionError", "sum expects list of integers"))?;
    let mut total = 0i64;
    for item in values {
        total += item.as_i64().unwrap_or(0);
    }
    Ok(Value::Number(total.into()))
}

pub fn action_registry() -> HashMap<String, ActionCallable> {
    let mut actions: HashMap<String, ActionCallable> = HashMap::new();
    actions.insert(
        "double".to_string(),
        std::sync::Arc::new(|kwargs| Box::pin(action_double(kwargs))),
    );
    actions.insert(
        "sum".to_string(),
        std::sync::Arc::new(|kwargs| Box::pin(action_sum(kwargs))),
    );
    actions
}

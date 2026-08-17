//! The in-process Rust actions served by the inline worker pool.

use std::collections::HashMap;

use waymark_worker_core::WorkerPoolError;
use waymark_worker_inline::InlineActionCallable;
use waymark_worker_inline_compat::inline_action;

async fn action_double(
    kwargs: HashMap<String, serde_json::Value>,
) -> Result<serde_json::Value, WorkerPoolError> {
    let value = kwargs
        .get("value")
        .and_then(|value| value.as_i64())
        .ok_or_else(|| WorkerPoolError::new("ActionError", "double expects integer value"))?;
    Ok(serde_json::Value::Number((value * 2).into()))
}

async fn action_sum(
    kwargs: HashMap<String, serde_json::Value>,
) -> Result<serde_json::Value, WorkerPoolError> {
    let values = kwargs
        .get("values")
        .and_then(|value| value.as_array())
        .ok_or_else(|| WorkerPoolError::new("ActionError", "sum expects list of integers"))?;
    let mut total = 0i64;
    for item in values {
        total += item.as_i64().unwrap_or(0);
    }
    Ok(serde_json::Value::Number(total.into()))
}

pub fn action_registry() -> HashMap<String, InlineActionCallable> {
    let mut actions: HashMap<String, InlineActionCallable> = HashMap::new();
    actions.insert("double".to_string(), inline_action(action_double));
    actions.insert("sum".to_string(), inline_action(action_sum));
    actions
}

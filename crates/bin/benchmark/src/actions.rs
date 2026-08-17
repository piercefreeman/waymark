//! The in-process Rust actions served by the inline worker pool.

use std::collections::HashMap;

use waymark_vm_value_python::{ReadyValue, Value};
use waymark_worker_core::WorkerPoolError;
use waymark_worker_inline::InlineActionCallable;
use waymark_worker_inline_compat::inline_action;

async fn action_double(kwargs: HashMap<String, ReadyValue>) -> Result<ReadyValue, WorkerPoolError> {
    let Some(ReadyValue::Int(value)) = kwargs.get("value") else {
        return Err(WorkerPoolError::new(
            "ActionError",
            "double expects integer value",
        ));
    };
    Ok(ReadyValue::Int(value * 2))
}

async fn action_sum(kwargs: HashMap<String, ReadyValue>) -> Result<ReadyValue, WorkerPoolError> {
    let Some(ReadyValue::List(values)) = kwargs.get("values") else {
        return Err(WorkerPoolError::new(
            "ActionError",
            "sum expects list of integers",
        ));
    };
    let mut total = 0i64;
    for item in values {
        let Value::Ready(ReadyValue::Int(value)) = item else {
            return Err(WorkerPoolError::new(
                "ActionError",
                "sum expects integer elements",
            ));
        };
        total += value;
    }
    Ok(ReadyValue::Int(total))
}

pub fn action_registry() -> HashMap<String, InlineActionCallable> {
    let mut actions: HashMap<String, InlineActionCallable> = HashMap::new();
    actions.insert("double".to_string(), inline_action(action_double));
    actions.insert("sum".to_string(), inline_action(action_sum));
    actions
}

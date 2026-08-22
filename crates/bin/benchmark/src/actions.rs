//! The in-process Rust actions served by the inline worker pool.

use std::collections::HashMap;

use waymark_vm_value_python::{ReadyValue, Value};

use waymark_worker_inline::InlineActionCallable;
use waymark_worker_inline_compat::inline_action;

/// The inline adapter pinned to this binary's flavor converter.
fn py_inline_action<F, Fut>(body: F) -> waymark_worker_inline::InlineActionCallable
where
    F: Fn(std::collections::HashMap<String, waymark_vm_value_python::ReadyValue>) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: Future<
            Output = Result<
                waymark_vm_value_python::ReadyValue,
                waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>,
            >,
        > + Send
        + 'static,
{
    inline_action::<waymark_vm_value_python_convert_proto::Converter, _, _, _>(body)
}

/// The exception an action raises for a malformed call.
fn action_error(message: &str) -> waymark_vm_runtime_exception::Exception<ReadyValue> {
    waymark_vm_runtime_exception::Exception {
        type_id: "ActionError".to_owned(),
        details: ReadyValue::Dict(indexmap::IndexMap::from([(
            "message".to_owned(),
            Value::Ready(ReadyValue::String(message.to_owned())),
        )])),
    }
}

async fn action_double(
    kwargs: HashMap<String, ReadyValue>,
) -> Result<ReadyValue, waymark_vm_runtime_exception::Exception<ReadyValue>> {
    let Some(ReadyValue::Int(value)) = kwargs.get("value") else {
        return Err(action_error("double expects integer value"));
    };
    Ok(ReadyValue::Int(value * 2))
}

async fn action_sum(
    kwargs: HashMap<String, ReadyValue>,
) -> Result<ReadyValue, waymark_vm_runtime_exception::Exception<ReadyValue>> {
    let Some(ReadyValue::List(values)) = kwargs.get("values") else {
        return Err(action_error("sum expects list of integers"));
    };
    let mut total = 0i64;
    for item in values {
        let Value::Ready(ReadyValue::Int(value)) = item else {
            return Err(action_error("sum expects integer elements"));
        };
        total += value;
    }
    Ok(ReadyValue::Int(total))
}

pub fn action_registry() -> HashMap<String, InlineActionCallable> {
    let mut actions: HashMap<String, InlineActionCallable> = HashMap::new();
    actions.insert("double".to_string(), py_inline_action(action_double));
    actions.insert("sum".to_string(), py_inline_action(action_sum));
    actions
}

use std::collections::HashMap;

use waymark_proto::messages as proto;

pub fn kwargs_to_workflow_arguments(
    kwargs: &HashMap<String, serde_json::Value>,
) -> proto::WorkflowArguments {
    let mut arguments = Vec::with_capacity(kwargs.len());
    for (key, value) in kwargs {
        let arg_value = waymark_message_conversions::json_to_workflow_argument_value(value);
        arguments.push(proto::WorkflowArgument {
            key: key.clone(),
            value: Some(arg_value),
        });
    }
    proto::WorkflowArguments { arguments }
}

fn normalize_error_value(error: serde_json::Value) -> serde_json::Value {
    let serde_json::Value::Object(mut map) = error else {
        return error;
    };

    if let Some(serde_json::Value::Object(exception)) = map.remove("__exception__") {
        return ensure_error_fields(exception);
    }

    ensure_error_fields(map)
}

fn ensure_error_fields(mut map: serde_json::Map<String, serde_json::Value>) -> serde_json::Value {
    let error_type = map
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("RemoteWorkerError")
        .to_string();
    let error_message = map
        .get("message")
        .and_then(|value| value.as_str())
        .unwrap_or("remote worker error")
        .to_string();
    if !map.contains_key("type") {
        map.insert("type".to_string(), serde_json::Value::String(error_type));
    }
    if !map.contains_key("message") {
        map.insert(
            "message".to_string(),
            serde_json::Value::String(error_message),
        );
    }
    serde_json::Value::Object(map)
}

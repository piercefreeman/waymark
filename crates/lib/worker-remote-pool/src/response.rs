use prost::Message as _;
use waymark_proto::messages as proto;
use waymark_worker_core::{WorkerPoolError, error_to_value};

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

fn normalize_error_value(error: serde_json::Value) -> serde_json::Value {
    let serde_json::Value::Object(mut map) = error else {
        return error;
    };

    if let Some(serde_json::Value::Object(exception)) = map.remove("__exception__") {
        return ensure_error_fields(exception);
    }

    ensure_error_fields(map)
}

pub fn decode_action_result(
    metrics: &waymark_worker_metrics::RoundTripMetrics,
) -> serde_json::Value {
    let payload = proto::WorkflowArguments::decode(metrics.response_payload.as_slice())
        .map(waymark_message_conversions::workflow_arguments_to_json)
        .unwrap_or(serde_json::Value::Null);

    if metrics.success {
        if let serde_json::Value::Object(mut map) = payload {
            if let Some(result) = map.remove("result") {
                return result;
            }
            return serde_json::Value::Object(map);
        }
        return payload;
    }

    if let serde_json::Value::Object(mut map) = payload {
        if let Some(error) = map.remove("error") {
            return normalize_error_value(error);
        }
        return serde_json::Value::Object(map);
    }

    let error_type = metrics.error_type.as_deref().unwrap_or("RemoteWorkerError");
    let error_message = metrics
        .error_message
        .as_deref()
        .unwrap_or("remote worker error");
    error_to_value(&WorkerPoolError::new(error_type, error_message))
}

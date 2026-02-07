//! Protocol buffer message types and encoding/decoding utilities.
//!
//! This module wraps the generated protobuf types and provides helper functions
//! for encoding and decoding messages in the worker bridge protocol.

use std::time::Instant;

use once_cell::sync::Lazy;
use prost::Message;
use thiserror::Error;

/// Re-export generated protobuf types
pub mod proto {
    // Messages for worker bridge communication
    tonic::include_proto!("waymark.messages");
}

/// AST types from ast.proto for IR representation
pub mod ast {
    // IR AST types
    tonic::include_proto!("waymark.ast");
}

/// Execution graph types from execution.proto
pub mod execution {
    // Execution state types
    tonic::include_proto!("waymark.execution");
}

/// Errors that can occur during message encoding/decoding
#[derive(Debug, Error)]
pub enum MessageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to decode message: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("Failed to encode message: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("Channel closed")]
    ChannelClosed,
}

/// Encode a protobuf message to bytes
pub fn encode_message<M: Message>(msg: &M) -> Vec<u8> {
    msg.encode_to_vec()
}

/// Decode a protobuf message from bytes
pub fn decode_message<M>(bytes: &[u8]) -> Result<M, MessageError>
where
    M: Message + Default,
{
    M::decode(bytes).map_err(MessageError::from)
}

/// Create an ACK envelope for the given delivery_id
pub fn ack_envelope(partition_id: u32, delivery_id: u64) -> proto::Envelope {
    let ack = proto::Ack {
        acked_delivery_id: delivery_id,
    };
    proto::Envelope {
        delivery_id,
        partition_id,
        kind: proto::MessageKind::Ack as i32,
        payload: encode_message(&ack),
    }
}

/// Get monotonic time in nanoseconds since process start.
/// Used for performance measurement.
pub fn now_monotonic_ns() -> u64 {
    static START: Lazy<Instant> = Lazy::new(Instant::now);
    START.elapsed().as_nanos() as u64
}

/// Convert a WorkflowArgumentValue to a serde_json::Value
pub fn workflow_argument_value_to_json(value: &proto::WorkflowArgumentValue) -> serde_json::Value {
    use proto::workflow_argument_value::Kind;
    use serde_json::json;

    match &value.kind {
        Some(Kind::Primitive(p)) => primitive_to_json(p),
        Some(Kind::Basemodel(bm)) => optional_workflow_dict_to_json(&bm.data),
        Some(Kind::Exception(e)) => {
            json!({
                "__exception__": {
                    "type": e.r#type,
                    "module": e.module,
                    "message": e.message,
                    "traceback": e.traceback,
                    "values": optional_workflow_dict_to_json(&e.values)
                }
            })
        }
        Some(Kind::ListValue(list)) => {
            let items: Vec<serde_json::Value> = list
                .items
                .iter()
                .map(workflow_argument_value_to_json)
                .collect();
            serde_json::Value::Array(items)
        }
        Some(Kind::TupleValue(tuple)) => {
            let items: Vec<serde_json::Value> = tuple
                .items
                .iter()
                .map(workflow_argument_value_to_json)
                .collect();
            serde_json::Value::Array(items)
        }
        Some(Kind::DictValue(dict)) => workflow_dict_to_json(dict),
        None => serde_json::Value::Null,
    }
}

fn primitive_to_json(p: &proto::PrimitiveWorkflowArgument) -> serde_json::Value {
    use proto::primitive_workflow_argument::Kind;
    use serde_json::json;

    match &p.kind {
        Some(Kind::StringValue(s)) => json!(s),
        Some(Kind::DoubleValue(d)) => json!(d),
        Some(Kind::IntValue(i)) => json!(i),
        Some(Kind::BoolValue(b)) => json!(b),
        Some(Kind::NullValue(_)) => serde_json::Value::Null,
        None => serde_json::Value::Null,
    }
}

fn workflow_dict_to_json(dict: &proto::WorkflowDictArgument) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for entry in &dict.entries {
        if let Some(value) = &entry.value {
            map.insert(entry.key.clone(), workflow_argument_value_to_json(value));
        }
    }
    serde_json::Value::Object(map)
}

fn optional_workflow_dict_to_json(dict: &Option<proto::WorkflowDictArgument>) -> serde_json::Value {
    match dict {
        Some(d) => workflow_dict_to_json(d),
        None => serde_json::Value::Object(serde_json::Map::new()),
    }
}

/// Convert WorkflowArguments protobuf to a JSON object.
/// Returns None if decoding fails.
pub fn workflow_arguments_to_json(bytes: &[u8]) -> Option<serde_json::Value> {
    let args: proto::WorkflowArguments = decode_message(bytes).ok()?;

    let mut map = serde_json::Map::new();
    for arg in &args.arguments {
        if let Some(value) = &arg.value {
            map.insert(arg.key.clone(), workflow_argument_value_to_json(value));
        }
    }

    Some(serde_json::Value::Object(map))
}

/// Convert a serde_json::Value to a WorkflowArgumentValue.
pub fn json_to_workflow_argument_value(value: &serde_json::Value) -> proto::WorkflowArgumentValue {
    use proto::primitive_workflow_argument::Kind as PrimitiveKind;
    use proto::workflow_argument_value::Kind;

    let kind = match value {
        serde_json::Value::Null => Kind::Primitive(proto::PrimitiveWorkflowArgument {
            kind: Some(PrimitiveKind::NullValue(0)),
        }),
        serde_json::Value::Bool(b) => Kind::Primitive(proto::PrimitiveWorkflowArgument {
            kind: Some(PrimitiveKind::BoolValue(*b)),
        }),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Kind::Primitive(proto::PrimitiveWorkflowArgument {
                    kind: Some(PrimitiveKind::IntValue(i)),
                })
            } else if let Some(u) = n.as_u64() {
                Kind::Primitive(proto::PrimitiveWorkflowArgument {
                    kind: Some(PrimitiveKind::IntValue(u as i64)),
                })
            } else {
                Kind::Primitive(proto::PrimitiveWorkflowArgument {
                    kind: Some(PrimitiveKind::DoubleValue(n.as_f64().unwrap_or(0.0))),
                })
            }
        }
        serde_json::Value::String(s) => Kind::Primitive(proto::PrimitiveWorkflowArgument {
            kind: Some(PrimitiveKind::StringValue(s.clone())),
        }),
        serde_json::Value::Array(items) => {
            let mut list = proto::WorkflowListArgument { items: Vec::new() };
            for item in items {
                list.items.push(json_to_workflow_argument_value(item));
            }
            Kind::ListValue(list)
        }
        serde_json::Value::Object(map) => {
            let mut dict = proto::WorkflowDictArgument {
                entries: Vec::new(),
            };
            for (key, item) in map {
                dict.entries.push(proto::WorkflowArgument {
                    key: key.clone(),
                    value: Some(json_to_workflow_argument_value(item)),
                });
            }
            Kind::DictValue(dict)
        }
    };

    proto::WorkflowArgumentValue { kind: Some(kind) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = proto::Ack {
            acked_delivery_id: 42,
        };
        let bytes = encode_message(&original);
        let decoded: proto::Ack = decode_message(&bytes).expect("decode");
        assert_eq!(decoded.acked_delivery_id, 42);
    }

    #[test]
    fn test_ack_envelope() {
        let envelope = ack_envelope(1, 100);
        assert_eq!(envelope.delivery_id, 100);
        assert_eq!(envelope.partition_id, 1);
        assert_eq!(envelope.kind, proto::MessageKind::Ack as i32);

        let ack: proto::Ack = decode_message(&envelope.payload).expect("decode ack");
        assert_eq!(ack.acked_delivery_id, 100);
    }

    #[test]
    fn test_monotonic_ns_increases() {
        let t1 = now_monotonic_ns();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let t2 = now_monotonic_ns();
        assert!(t2 > t1);
    }

    #[test]
    fn test_json_argument_roundtrip() {
        let value = serde_json::json!({
            "int": 7,
            "float": 1.25,
            "bool": true,
            "text": "hello",
            "list": [1, 2, 3],
            "nested": {"a": 1, "b": [false, null]},
        });

        let arg = json_to_workflow_argument_value(&value);
        let back = workflow_argument_value_to_json(&arg);
        assert_eq!(value, back);
    }
}

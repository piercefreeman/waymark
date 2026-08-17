//! Conversions for the Python flavor's proto value document.
//!
//! Everything here operates on [`waymark_proto::python_value`] — the
//! encoding of a single Python workflow value — and knows nothing about
//! the framing that carries the encoded documents.

use prost::Message as _;
use waymark_proto::python_value as proto_value;

/// Convert a WorkflowArgumentValue to a serde_json::Value
pub fn workflow_argument_value_to_json(
    value: &proto_value::WorkflowArgumentValue,
) -> serde_json::Value {
    use proto_value::workflow_argument_value::Kind;
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

fn primitive_to_json(p: &proto_value::PrimitiveWorkflowArgument) -> serde_json::Value {
    use proto_value::primitive_workflow_argument::Kind;
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

fn workflow_dict_to_json(dict: &proto_value::WorkflowDictArgument) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for entry in &dict.entries {
        if let Some(value) = &entry.value {
            map.insert(entry.key.clone(), workflow_argument_value_to_json(value));
        }
    }
    serde_json::Value::Object(map)
}

fn optional_workflow_dict_to_json(
    dict: &Option<proto_value::WorkflowDictArgument>,
) -> serde_json::Value {
    match dict {
        Some(d) => workflow_dict_to_json(d),
        None => serde_json::Value::Object(serde_json::Map::new()),
    }
}

/// Convert a serde_json::Value to a WorkflowArgumentValue.
pub fn json_to_workflow_argument_value(
    value: &serde_json::Value,
) -> proto_value::WorkflowArgumentValue {
    use proto_value::primitive_workflow_argument::Kind as PrimitiveKind;
    use proto_value::workflow_argument_value::Kind;

    let kind = match value {
        serde_json::Value::Null => Kind::Primitive(proto_value::PrimitiveWorkflowArgument {
            kind: Some(PrimitiveKind::NullValue(0)),
        }),
        serde_json::Value::Bool(b) => Kind::Primitive(proto_value::PrimitiveWorkflowArgument {
            kind: Some(PrimitiveKind::BoolValue(*b)),
        }),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Kind::Primitive(proto_value::PrimitiveWorkflowArgument {
                    kind: Some(PrimitiveKind::IntValue(i)),
                })
            } else if let Some(u) = n.as_u64() {
                Kind::Primitive(proto_value::PrimitiveWorkflowArgument {
                    kind: Some(PrimitiveKind::IntValue(u as i64)),
                })
            } else {
                Kind::Primitive(proto_value::PrimitiveWorkflowArgument {
                    kind: Some(PrimitiveKind::DoubleValue(n.as_f64().unwrap_or(0.0))),
                })
            }
        }
        serde_json::Value::String(s) => Kind::Primitive(proto_value::PrimitiveWorkflowArgument {
            kind: Some(PrimitiveKind::StringValue(s.clone())),
        }),
        serde_json::Value::Array(items) => {
            let mut list = proto_value::WorkflowListArgument { items: Vec::new() };
            for item in items {
                list.items.push(json_to_workflow_argument_value(item));
            }
            Kind::ListValue(list)
        }
        serde_json::Value::Object(map) => {
            let mut dict = proto_value::WorkflowDictArgument {
                entries: Vec::new(),
            };
            for (key, item) in map {
                dict.entries.push(proto_value::WorkflowDictEntry {
                    key: key.clone(),
                    value: Some(json_to_workflow_argument_value(item)),
                });
            }
            Kind::DictValue(dict)
        }
    };

    proto_value::WorkflowArgumentValue { kind: Some(kind) }
}

/// Encode a [`proto_value::WorkflowArgumentValue`] into the opaque value
/// bytes carried at the framing level.
pub fn encode_workflow_argument_value(value: &proto_value::WorkflowArgumentValue) -> Vec<u8> {
    value.encode_to_vec()
}

/// Decode an encoded [`proto_value::WorkflowArgumentValue`] document.
pub fn decode_workflow_argument_value(
    bytes: &[u8],
) -> Result<proto_value::WorkflowArgumentValue, prost::DecodeError> {
    proto_value::WorkflowArgumentValue::decode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

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

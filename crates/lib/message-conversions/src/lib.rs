//! Protocol buffer message conversion utilities.

use waymark_proto::messages as proto;

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

pub fn workflow_arguments_to_json(args: proto::WorkflowArguments) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for arg in &args.arguments {
        if let Some(value) = &arg.value {
            map.insert(arg.key.clone(), workflow_argument_value_to_json(value));
        }
    }

    serde_json::Value::Object(map)
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

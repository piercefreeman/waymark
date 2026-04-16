//! Protocol buffer message conversion utilities.

use std::collections::HashMap;

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
        Some(Kind::FlatValue(flat)) => flat_workflow_argument_to_json(flat),
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
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Kind::FlatValue(json_to_flat_workflow_argument(value))
        }
    };

    proto::WorkflowArgumentValue { kind: Some(kind) }
}

fn flat_workflow_argument_to_json(flat: &proto::FlatWorkflowArgument) -> serde_json::Value {
    if flat.nodes.is_empty() {
        return serde_json::Value::Null;
    }

    let mut nodes_by_id = HashMap::with_capacity(flat.nodes.len());
    for node in &flat.nodes {
        nodes_by_id.insert(node.node_id, node);
    }

    let mut cache = HashMap::with_capacity(flat.nodes.len());
    flat_node_to_json(flat.root_node_id, &nodes_by_id, &mut cache)
        .unwrap_or(serde_json::Value::Null)
}

fn flat_node_to_json(
    node_id: u32,
    nodes_by_id: &HashMap<u32, &proto::FlatWorkflowNode>,
    cache: &mut HashMap<u32, serde_json::Value>,
) -> Option<serde_json::Value> {
    if let Some(value) = cache.get(&node_id) {
        return Some(value.clone());
    }

    use proto::flat_workflow_node::Kind;
    use serde_json::json;

    let node = nodes_by_id.get(&node_id)?;
    let value = match &node.kind {
        Some(Kind::Primitive(primitive)) => primitive_to_json(primitive),
        Some(Kind::ListValue(list)) => serde_json::Value::Array(
            list.item_node_ids
                .iter()
                .map(|item_id| flat_node_to_json(*item_id, nodes_by_id, cache))
                .collect::<Option<Vec<_>>>()?,
        ),
        Some(Kind::TupleValue(tuple)) => serde_json::Value::Array(
            tuple
                .item_node_ids
                .iter()
                .map(|item_id| flat_node_to_json(*item_id, nodes_by_id, cache))
                .collect::<Option<Vec<_>>>()?,
        ),
        Some(Kind::DictValue(dict)) => flat_entries_to_json(&dict.entries, nodes_by_id, cache),
        Some(Kind::Basemodel(model)) => {
            let entries = model
                .data_entries
                .iter()
                .map(|entry| (entry.key.clone(), entry.value_node_id))
                .collect::<Vec<_>>();
            flat_pairs_to_json(&entries, nodes_by_id, cache)
        }
        Some(Kind::Exception(exception)) => {
            let entries = exception
                .value_entries
                .iter()
                .map(|entry| (entry.key.clone(), entry.value_node_id))
                .collect::<Vec<_>>();
            let values = flat_pairs_to_json(&entries, nodes_by_id, cache);
            json!({
                "__exception__": {
                    "type": exception.r#type,
                    "module": exception.module,
                    "message": exception.message,
                    "traceback": exception.traceback,
                    "values": values,
                }
            })
        }
        None => return None,
    };

    cache.insert(node_id, value.clone());
    Some(value)
}

fn flat_entries_to_json(
    entries: &[proto::FlatWorkflowEntry],
    nodes_by_id: &HashMap<u32, &proto::FlatWorkflowNode>,
    cache: &mut HashMap<u32, serde_json::Value>,
) -> serde_json::Value {
    let pairs = entries
        .iter()
        .map(|entry| (entry.key.clone(), entry.value_node_id))
        .collect::<Vec<_>>();
    flat_pairs_to_json(&pairs, nodes_by_id, cache)
}

fn flat_pairs_to_json(
    entries: &[(String, u32)],
    nodes_by_id: &HashMap<u32, &proto::FlatWorkflowNode>,
    cache: &mut HashMap<u32, serde_json::Value>,
) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (key, node_id) in entries {
        let Some(value) = flat_node_to_json(*node_id, nodes_by_id, cache) else {
            return serde_json::Value::Null;
        };
        map.insert(key.clone(), value);
    }
    serde_json::Value::Object(map)
}

fn json_to_flat_workflow_argument(value: &serde_json::Value) -> proto::FlatWorkflowArgument {
    let mut flat = proto::FlatWorkflowArgument {
        root_node_id: 0,
        nodes: Vec::new(),
    };
    flat.root_node_id = append_flat_node(&mut flat, value);
    flat
}

fn append_flat_node(flat: &mut proto::FlatWorkflowArgument, value: &serde_json::Value) -> u32 {
    use proto::flat_workflow_node::Kind;
    use proto::primitive_workflow_argument::Kind as PrimitiveKind;

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
            let item_node_ids = items
                .iter()
                .map(|item| append_flat_node(flat, item))
                .collect();
            Kind::ListValue(proto::FlatWorkflowListNode { item_node_ids })
        }
        serde_json::Value::Object(map) => {
            let entries = map
                .iter()
                .map(|(key, item)| proto::FlatWorkflowEntry {
                    key: key.clone(),
                    value_node_id: append_flat_node(flat, item),
                })
                .collect();
            Kind::DictValue(proto::FlatWorkflowDictNode { entries })
        }
    };

    let node_id = flat.nodes.len() as u32;
    flat.nodes.push(proto::FlatWorkflowNode {
        node_id,
        kind: Some(kind),
    });
    node_id
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message as _;

    fn build_nested(depth: usize) -> serde_json::Value {
        let mut value = serde_json::json!({"leaf": "ok"});
        for index in 0..depth {
            value = if index % 2 == 0 {
                serde_json::json!({"node": value})
            } else {
                serde_json::json!({"items": [value]})
            };
        }
        serde_json::json!({"root": value})
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

    #[test]
    fn test_deep_json_roundtrip_survives_protobuf_decode() {
        let deep = build_nested(40);
        let arguments = proto::WorkflowArguments {
            arguments: vec![proto::WorkflowArgument {
                key: "value".to_string(),
                value: Some(json_to_workflow_argument_value(&deep)),
            }],
        };

        let bytes = arguments.encode_to_vec();
        let decoded = proto::WorkflowArguments::decode(bytes.as_slice()).expect("decode arguments");
        let back = workflow_arguments_to_json(decoded);

        assert_eq!(back, serde_json::json!({"value": deep}));
    }
}

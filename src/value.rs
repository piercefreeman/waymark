use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::messages::proto;

#[derive(Debug, Clone)]
pub enum WorkflowValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<WorkflowValue>),
    Tuple(Vec<WorkflowValue>),
    Dict(HashMap<String, WorkflowValue>),
    Exception {
        exc_type: String,
        module: String,
        message: String,
        traceback: String,
        values: HashMap<String, WorkflowValue>,
        /// Exception class hierarchy (MRO) for proper except matching.
        /// e.g., for KeyError: ["KeyError", "LookupError", "Exception", "BaseException"]
        type_hierarchy: Vec<String>,
    },
}

impl WorkflowValue {
    pub fn from_proto(value: &proto::WorkflowArgumentValue) -> Self {
        use proto::primitive_workflow_argument::Kind as PrimitiveKind;
        use proto::workflow_argument_value::Kind;

        match &value.kind {
            Some(Kind::Primitive(p)) => match &p.kind {
                Some(PrimitiveKind::IntValue(i)) => WorkflowValue::Int(*i),
                Some(PrimitiveKind::DoubleValue(f)) => WorkflowValue::Float(*f),
                Some(PrimitiveKind::StringValue(s)) => WorkflowValue::String(s.clone()),
                Some(PrimitiveKind::BoolValue(b)) => WorkflowValue::Bool(*b),
                Some(PrimitiveKind::NullValue(_)) => WorkflowValue::Null,
                None => WorkflowValue::Null,
            },
            Some(Kind::ListValue(list)) => {
                WorkflowValue::List(list.items.iter().map(WorkflowValue::from_proto).collect())
            }
            Some(Kind::TupleValue(tuple)) => {
                WorkflowValue::Tuple(tuple.items.iter().map(WorkflowValue::from_proto).collect())
            }
            Some(Kind::DictValue(dict)) => {
                let mut entries = HashMap::new();
                for entry in &dict.entries {
                    let value = entry
                        .value
                        .as_ref()
                        .map(WorkflowValue::from_proto)
                        .unwrap_or(WorkflowValue::Null);
                    entries.insert(entry.key.clone(), value);
                }
                WorkflowValue::Dict(entries)
            }
            Some(Kind::Basemodel(model)) => {
                let mut data = HashMap::new();
                if let Some(dict) = &model.data {
                    for entry in &dict.entries {
                        let value = entry
                            .value
                            .as_ref()
                            .map(WorkflowValue::from_proto)
                            .unwrap_or(WorkflowValue::Null);
                        data.insert(entry.key.clone(), value);
                    }
                }
                WorkflowValue::Dict(data)
            }
            Some(Kind::Exception(exc)) => {
                let mut values = HashMap::new();
                if let Some(dict) = &exc.values {
                    for entry in &dict.entries {
                        let value = entry
                            .value
                            .as_ref()
                            .map(WorkflowValue::from_proto)
                            .unwrap_or(WorkflowValue::Null);
                        values.insert(entry.key.clone(), value);
                    }
                }
                WorkflowValue::Exception {
                    exc_type: exc.r#type.clone(),
                    module: exc.module.clone(),
                    message: exc.message.clone(),
                    traceback: exc.traceback.clone(),
                    values,
                    type_hierarchy: exc.type_hierarchy.clone(),
                }
            }
            None => WorkflowValue::Null,
        }
    }

    pub fn to_proto(&self) -> proto::WorkflowArgumentValue {
        use proto::workflow_argument_value::Kind;

        let kind = match self {
            WorkflowValue::Null => Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::NullValue(0)),
            }),
            WorkflowValue::Bool(b) => Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::BoolValue(*b)),
            }),
            WorkflowValue::Int(i) => Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::IntValue(*i)),
            }),
            WorkflowValue::Float(f) => Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::DoubleValue(*f)),
            }),
            WorkflowValue::String(s) => Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::StringValue(
                    s.clone(),
                )),
            }),
            WorkflowValue::List(items) => Kind::ListValue(proto::WorkflowListArgument {
                items: items.iter().map(WorkflowValue::to_proto).collect(),
            }),
            WorkflowValue::Tuple(items) => Kind::TupleValue(proto::WorkflowTupleArgument {
                items: items.iter().map(WorkflowValue::to_proto).collect(),
            }),
            WorkflowValue::Dict(entries) => {
                let entries = entries
                    .iter()
                    .map(|(key, value)| proto::WorkflowArgument {
                        key: key.clone(),
                        value: Some(value.to_proto()),
                    })
                    .collect();
                Kind::DictValue(proto::WorkflowDictArgument { entries })
            }
            WorkflowValue::Exception {
                exc_type,
                module,
                message,
                traceback,
                values,
                type_hierarchy,
            } => {
                let entries = values
                    .iter()
                    .map(|(key, value)| proto::WorkflowArgument {
                        key: key.clone(),
                        value: Some(value.to_proto()),
                    })
                    .collect();
                Kind::Exception(proto::WorkflowErrorValue {
                    r#type: exc_type.clone(),
                    module: module.clone(),
                    message: message.clone(),
                    traceback: traceback.clone(),
                    values: Some(proto::WorkflowDictArgument { entries }),
                    type_hierarchy: type_hierarchy.clone(),
                })
            }
        };

        proto::WorkflowArgumentValue { kind: Some(kind) }
    }

    pub fn from_json(value: &JsonValue) -> Self {
        match value {
            JsonValue::Null => WorkflowValue::Null,
            JsonValue::Bool(b) => WorkflowValue::Bool(*b),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    WorkflowValue::Int(i)
                } else if let Some(f) = n.as_f64() {
                    WorkflowValue::Float(f)
                } else {
                    WorkflowValue::Null
                }
            }
            JsonValue::String(s) => WorkflowValue::String(s.clone()),
            JsonValue::Array(arr) => {
                WorkflowValue::List(arr.iter().map(WorkflowValue::from_json).collect())
            }
            JsonValue::Object(obj) => {
                if let Some(exception) = Self::exception_from_json(obj) {
                    return exception;
                }

                let mut entries = HashMap::new();
                for (key, value) in obj {
                    entries.insert(key.clone(), WorkflowValue::from_json(value));
                }
                WorkflowValue::Dict(entries)
            }
        }
    }

    pub fn to_json(&self) -> JsonValue {
        match self {
            WorkflowValue::Null => JsonValue::Null,
            WorkflowValue::Bool(b) => JsonValue::Bool(*b),
            WorkflowValue::Int(i) => JsonValue::Number((*i).into()),
            WorkflowValue::Float(f) => serde_json::Number::from_f64(*f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            WorkflowValue::String(s) => JsonValue::String(s.clone()),
            WorkflowValue::List(items) => {
                JsonValue::Array(items.iter().map(WorkflowValue::to_json).collect())
            }
            WorkflowValue::Tuple(items) => {
                JsonValue::Array(items.iter().map(WorkflowValue::to_json).collect())
            }
            WorkflowValue::Dict(entries) => {
                let map = entries
                    .iter()
                    .map(|(key, value)| (key.clone(), value.to_json()))
                    .collect();
                JsonValue::Object(map)
            }
            WorkflowValue::Exception {
                exc_type,
                module,
                message,
                traceback,
                values,
                type_hierarchy,
            } => {
                let values_json: serde_json::Map<String, JsonValue> = values
                    .iter()
                    .map(|(key, value)| (key.clone(), value.to_json()))
                    .collect();
                let exc_obj = serde_json::json!({
                    "type": exc_type,
                    "module": module,
                    "message": message,
                    "traceback": traceback,
                    "values": JsonValue::Object(values_json),
                    "type_hierarchy": type_hierarchy,
                });
                serde_json::json!({
                    "__exception__": exc_obj
                })
            }
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            WorkflowValue::Null => false,
            WorkflowValue::Bool(b) => *b,
            WorkflowValue::Int(i) => *i != 0,
            WorkflowValue::Float(f) => *f != 0.0,
            WorkflowValue::String(s) => !s.is_empty(),
            WorkflowValue::List(items) => !items.is_empty(),
            WorkflowValue::Tuple(items) => !items.is_empty(),
            WorkflowValue::Dict(entries) => !entries.is_empty(),
            WorkflowValue::Exception { .. } => true,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            WorkflowValue::Int(i) => Some(*i),
            WorkflowValue::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            WorkflowValue::Int(i) => Some(*i as f64),
            WorkflowValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&Vec<WorkflowValue>> {
        match self {
            WorkflowValue::List(items) => Some(items),
            WorkflowValue::Tuple(items) => Some(items),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&HashMap<String, WorkflowValue>> {
        match self {
            WorkflowValue::Dict(entries) => Some(entries),
            _ => None,
        }
    }

    pub fn to_key_string(&self) -> String {
        match self {
            WorkflowValue::Null => "null".to_string(),
            WorkflowValue::Bool(b) => b.to_string(),
            WorkflowValue::Int(i) => i.to_string(),
            WorkflowValue::Float(f) => f.to_string(),
            WorkflowValue::String(s) => s.clone(),
            WorkflowValue::List(_) => "[list]".to_string(),
            WorkflowValue::Tuple(_) => "[tuple]".to_string(),
            WorkflowValue::Dict(_) => "{dict}".to_string(),
            WorkflowValue::Exception { exc_type, .. } => exc_type.clone(),
        }
    }

    fn exception_from_json(obj: &serde_json::Map<String, JsonValue>) -> Option<WorkflowValue> {
        match obj.get("__exception__") {
            Some(JsonValue::Object(exc_obj)) => {
                let exc_type = exc_obj.get("type").and_then(|v| v.as_str())?;
                let module = exc_obj.get("module").and_then(|v| v.as_str()).unwrap_or("");
                let message = exc_obj
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let traceback = exc_obj
                    .get("traceback")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let values = Self::exception_values_from_json(exc_obj);
                let type_hierarchy = Self::exception_hierarchy_from_json(exc_obj);
                Some(WorkflowValue::Exception {
                    exc_type: exc_type.to_string(),
                    module: module.to_string(),
                    message: message.to_string(),
                    traceback: traceback.to_string(),
                    values,
                    type_hierarchy,
                })
            }
            Some(JsonValue::Bool(true)) => {
                let exc_type = obj.get("type").and_then(|v| v.as_str())?;
                let module = obj.get("module").and_then(|v| v.as_str()).unwrap_or("");
                let message = obj.get("message").and_then(|v| v.as_str()).unwrap_or("");
                let traceback = obj.get("traceback").and_then(|v| v.as_str()).unwrap_or("");
                let values = Self::exception_values_from_json(obj);
                let type_hierarchy = Self::exception_hierarchy_from_json(obj);
                Some(WorkflowValue::Exception {
                    exc_type: exc_type.to_string(),
                    module: module.to_string(),
                    message: message.to_string(),
                    traceback: traceback.to_string(),
                    values,
                    type_hierarchy,
                })
            }
            _ => None,
        }
    }

    fn exception_values_from_json(
        obj: &serde_json::Map<String, JsonValue>,
    ) -> HashMap<String, WorkflowValue> {
        let mut values = HashMap::new();
        let Some(JsonValue::Object(values_obj)) = obj.get("values") else {
            return values;
        };
        for (key, value) in values_obj {
            values.insert(key.clone(), WorkflowValue::from_json(value));
        }
        values
    }

    fn exception_hierarchy_from_json(obj: &serde_json::Map<String, JsonValue>) -> Vec<String> {
        match obj.get("type_hierarchy") {
            Some(JsonValue::Array(arr)) => arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            _ => vec![],
        }
    }
}

impl PartialEq for WorkflowValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (WorkflowValue::Null, WorkflowValue::Null) => true,
            (WorkflowValue::Bool(a), WorkflowValue::Bool(b)) => a == b,
            (WorkflowValue::Int(a), WorkflowValue::Int(b)) => a == b,
            (WorkflowValue::Float(a), WorkflowValue::Float(b)) => a == b,
            (WorkflowValue::Int(a), WorkflowValue::Float(b)) => (*a as f64) == *b,
            (WorkflowValue::Float(a), WorkflowValue::Int(b)) => *a == (*b as f64),
            (WorkflowValue::String(a), WorkflowValue::String(b)) => a == b,
            (WorkflowValue::List(a), WorkflowValue::List(b)) => a == b,
            (WorkflowValue::Tuple(a), WorkflowValue::Tuple(b)) => a == b,
            (WorkflowValue::Dict(a), WorkflowValue::Dict(b)) => a == b,
            (
                WorkflowValue::Exception {
                    exc_type: at,
                    module: am,
                    message: amsg,
                    traceback: atb,
                    values: avals,
                    type_hierarchy: ahier,
                },
                WorkflowValue::Exception {
                    exc_type: bt,
                    module: bm,
                    message: bmsg,
                    traceback: btb,
                    values: bvals,
                    type_hierarchy: bhier,
                },
            ) => {
                at == bt
                    && am == bm
                    && amsg == bmsg
                    && atb == btb
                    && avals == bvals
                    && ahier == bhier
            }
            _ => false,
        }
    }
}

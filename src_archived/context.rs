use std::collections::HashMap;

use anyhow::{Context as AnyhowContext, Result};
use serde_json::Value;

use crate::messages::proto;

#[derive(Debug, Clone, Default)]
pub struct DecodedPayload {
    pub result: Option<Value>,
    pub error: Option<Value>,
}

pub fn arguments_to_json(args: &proto::WorkflowArguments) -> Result<HashMap<String, Value>> {
    let mut map = HashMap::new();
    for arg in &args.arguments {
        let key = arg.key.clone();
        let value = arg
            .value
            .as_ref()
            .context("workflow argument missing value")?;
        map.insert(key, argument_value_to_json(value)?);
    }
    Ok(map)
}

pub fn argument_value_to_json(value: &proto::WorkflowArgumentValue) -> Result<Value> {
    match value.kind.as_ref().context("argument value missing kind")? {
        proto::workflow_argument_value::Kind::Primitive(p) => primitive_to_json(p),
        proto::workflow_argument_value::Kind::ListValue(list) => {
            let mut arr = Vec::with_capacity(list.items.len());
            for item in &list.items {
                arr.push(argument_value_to_json(item)?);
            }
            Ok(Value::Array(arr))
        }
        proto::workflow_argument_value::Kind::TupleValue(tuple) => {
            let mut arr = Vec::with_capacity(tuple.items.len());
            for item in &tuple.items {
                arr.push(argument_value_to_json(item)?);
            }
            Ok(Value::Array(arr))
        }
        proto::workflow_argument_value::Kind::DictValue(dict) => {
            let mut obj = serde_json::Map::new();
            for entry in &dict.entries {
                let key = entry.key.clone();
                let Some(val) = entry.value.as_ref() else {
                    continue;
                };
                obj.insert(key, argument_value_to_json(val)?);
            }
            Ok(Value::Object(obj))
        }
        proto::workflow_argument_value::Kind::Basemodel(model) => {
            let mut obj = serde_json::Map::new();
            obj.insert("module".to_string(), Value::String(model.module.clone()));
            obj.insert("name".to_string(), Value::String(model.name.clone()));
            let mut data = serde_json::Map::new();
            if let Some(entries) = model.data.as_ref() {
                for entry in &entries.entries {
                    let key = entry.key.clone();
                    let Some(val) = entry.value.as_ref() else {
                        continue;
                    };
                    data.insert(key, argument_value_to_json(val)?);
                }
            }
            obj.insert("data".to_string(), Value::Object(data));
            Ok(Value::Object(obj))
        }
        proto::workflow_argument_value::Kind::Exception(exc) => {
            let mut obj = serde_json::Map::new();
            obj.insert("type".to_string(), Value::String(exc.r#type.clone()));
            obj.insert("module".to_string(), Value::String(exc.module.clone()));
            obj.insert("message".to_string(), Value::String(exc.message.clone()));
            obj.insert(
                "traceback".to_string(),
                Value::String(exc.traceback.clone()),
            );
            Ok(Value::Object(obj))
        }
    }
}

fn primitive_to_json(p: &proto::PrimitiveWorkflowArgument) -> Result<Value> {
    match p.kind.as_ref().context("primitive missing kind")? {
        proto::primitive_workflow_argument::Kind::StringValue(s) => Ok(Value::String(s.clone())),
        proto::primitive_workflow_argument::Kind::DoubleValue(f) => {
            Ok(serde_json::Number::from_f64(*f)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        proto::primitive_workflow_argument::Kind::IntValue(i) => Ok(Value::Number((*i).into())),
        proto::primitive_workflow_argument::Kind::BoolValue(b) => Ok(Value::Bool(*b)),
        proto::primitive_workflow_argument::Kind::NullValue(_) => Ok(Value::Null),
    }
}

pub fn json_to_argument_value(value: &Value) -> Result<proto::WorkflowArgumentValue> {
    let kind = match value {
        Value::Null => {
            proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::NullValue(
                    prost_types::NullValue::NullValue as i32,
                )),
            })
        }
        Value::Bool(flag) => {
            proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::BoolValue(*flag)),
            })
        }
        Value::Number(num) => {
            let primitive = if let Some(int_val) = num.as_i64() {
                proto::primitive_workflow_argument::Kind::IntValue(int_val)
            } else if let Some(uint_val) = num.as_u64() {
                let int_val = i64::try_from(uint_val).unwrap_or(i64::MAX);
                proto::primitive_workflow_argument::Kind::IntValue(int_val)
            } else if let Some(float_val) = num.as_f64() {
                proto::primitive_workflow_argument::Kind::DoubleValue(float_val)
            } else {
                proto::primitive_workflow_argument::Kind::NullValue(
                    prost_types::NullValue::NullValue as i32,
                )
            };
            proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(primitive),
            })
        }
        Value::String(text) => {
            proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::StringValue(
                    text.clone(),
                )),
            })
        }
        Value::Array(items) => {
            let mut list = proto::WorkflowListArgument::default();
            for item in items {
                let converted = json_to_argument_value(item)?;
                list.items.push(converted);
            }
            proto::workflow_argument_value::Kind::ListValue(list)
        }
        Value::Object(map) => {
            if let Some(model) = maybe_basemodel_argument(map)? {
                proto::workflow_argument_value::Kind::Basemodel(model)
            } else if let Some(exc) = maybe_exception_argument(map)? {
                proto::workflow_argument_value::Kind::Exception(exc)
            } else {
                let mut dict = proto::WorkflowDictArgument::default();
                for (key, val) in map {
                    let converted = json_to_argument_value(val)?;
                    dict.entries.push(proto::WorkflowArgument {
                        key: key.clone(),
                        value: Some(converted),
                    });
                }
                proto::workflow_argument_value::Kind::DictValue(dict)
            }
        }
    };
    Ok(proto::WorkflowArgumentValue { kind: Some(kind) })
}

fn maybe_basemodel_argument(
    map: &serde_json::Map<String, Value>,
) -> Result<Option<proto::BaseModelWorkflowArgument>> {
    let module = map.get("module").and_then(|v| v.as_str());
    let name = map.get("name").and_then(|v| v.as_str());
    let data = map.get("data");
    if module.is_none() || name.is_none() || data.is_none() {
        return Ok(None);
    }
    let Value::Object(data_obj) = data.unwrap() else {
        return Ok(None);
    };
    let mut dict = proto::WorkflowDictArgument::default();
    for (key, val) in data_obj {
        let converted = json_to_argument_value(val)?;
        dict.entries.push(proto::WorkflowArgument {
            key: key.clone(),
            value: Some(converted),
        });
    }
    Ok(Some(proto::BaseModelWorkflowArgument {
        module: module.unwrap().to_string(),
        name: name.unwrap().to_string(),
        data: Some(dict),
    }))
}

fn maybe_exception_argument(
    map: &serde_json::Map<String, Value>,
) -> Result<Option<proto::WorkflowErrorValue>> {
    let r#type = map.get("type").and_then(|v| v.as_str());
    let module = map.get("module").and_then(|v| v.as_str());
    let message = map.get("message").and_then(|v| v.as_str());
    let traceback = map.get("traceback").and_then(|v| v.as_str());
    if r#type.is_none() && module.is_none() && message.is_none() {
        return Ok(None);
    }
    Ok(Some(proto::WorkflowErrorValue {
        r#type: r#type.unwrap_or_default().to_string(),
        module: module.unwrap_or_default().to_string(),
        message: message.unwrap_or_default().to_string(),
        traceback: traceback.unwrap_or_default().to_string(),
    }))
}

pub fn values_to_arguments(values: &HashMap<String, Value>) -> Result<proto::WorkflowArguments> {
    let mut arguments = proto::WorkflowArguments::default();
    for (key, value) in values {
        let converted = json_to_argument_value(value)?;
        arguments.arguments.push(proto::WorkflowArgument {
            key: key.clone(),
            value: Some(converted),
        });
    }
    Ok(arguments)
}

pub fn decode_payload(arguments: &proto::WorkflowArguments) -> Result<DecodedPayload> {
    let mut result: Option<Value> = None;
    let mut error: Option<Value> = None;
    for arg in &arguments.arguments {
        if arg.key == "result"
            && let Some(value) = arg.value.as_ref()
        {
            result = Some(argument_value_to_json(value)?);
        } else if arg.key == "error"
            && let Some(value) = arg.value.as_ref()
        {
            error = Some(argument_value_to_json(value)?);
        }
    }
    Ok(DecodedPayload { result, error })
}

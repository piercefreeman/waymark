//! [`Convert`](waymark_convert_core::Convert) implementations from JSON
//! to VM value types.

#![warn(missing_docs)]

use std::convert::Infallible;

use indexmap::IndexMap;
use typed_floats::NonNaNFinite;
use waymark_convert_core::{Convert, TryConvert};
use waymark_vm_runtime_exception::Exception;
use waymark_vm_value::{ReadyValue, Value};

/// Stateless converter with all JSON-to-VM [`Convert`] impls.
pub struct Converter;

impl TryConvert<serde_json::Value, ReadyValue> for Converter {
    type Error = Infallible;

    fn try_convert(value: serde_json::Value) -> Result<ReadyValue, Self::Error> {
        Ok(match value {
            serde_json::Value::Null => ReadyValue::None,
            serde_json::Value::Bool(b) => ReadyValue::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    ReadyValue::Int(i)
                } else if let Some(f) = n.as_f64() {
                    match NonNaNFinite::try_from(f) {
                        Ok(finite) => ReadyValue::Float(finite),
                        Err(_) => ReadyValue::None,
                    }
                } else {
                    ReadyValue::None
                }
            }
            serde_json::Value::String(s) => ReadyValue::String(s),
            serde_json::Value::Array(items) => ReadyValue::List(
                items
                    .into_iter()
                    .map(<Converter as Convert<_, ReadyValue>>::convert)
                    .map(Value::Ready)
                    .collect(),
            ),
            serde_json::Value::Object(entries) => {
                let mut map = IndexMap::new();
                for (k, v) in entries {
                    map.insert(
                        k,
                        Value::Ready(<Converter as Convert<_, ReadyValue>>::convert(v)),
                    );
                }
                ReadyValue::Dict(map)
            }
        })
    }
}

impl TryConvert<serde_json::Value, Value> for Converter {
    type Error = Infallible;

    fn try_convert(value: serde_json::Value) -> Result<Value, Self::Error> {
        Ok(Value::Ready(
            <Converter as TryConvert<_, ReadyValue>>::try_convert(value)?,
        ))
    }
}

impl TryConvert<serde_json::Value, Exception<Value>> for Converter {
    type Error = Infallible;

    fn try_convert(value: serde_json::Value) -> Result<Exception<Value>, Self::Error> {
        let type_id = value
            .get("type")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| "ActionException".into());
        let details = value
            .get("message")
            .cloned()
            .map(<Converter as Convert<_, Value>>::convert)
            .unwrap_or_else(|| <Converter as Convert<_, Value>>::convert(value));
        Ok(Exception { type_id, details })
    }
}

/// Identity pass-through for JSON values (useful when the caller
/// already has the JSON representation).
impl TryConvert<serde_json::Value, serde_json::Value> for Converter {
    type Error = Infallible;

    fn try_convert(value: serde_json::Value) -> Result<serde_json::Value, Self::Error> {
        Ok(value)
    }
}

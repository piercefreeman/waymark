//! [`Convert`](waymark_convert_core::Convert) implementations between JSON
//! and VM value types.

#![warn(missing_docs)]

use std::convert::Infallible;

use indexmap::IndexMap;
use typed_floats::NonNaNFinite;
use waymark_convert_core::{Convert, TryConvert};
use waymark_vm_runtime_exception::Exception;
use waymark_vm_value::{ReadyValue, Value};

pub use waymark_vm_value_convert_core::PendingPromiseError;

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

impl<Details> TryConvert<serde_json::Value, Exception<Details>> for Converter
where
    Converter: TryConvert<serde_json::Value, Details>,
{
    type Error = <Converter as TryConvert<serde_json::Value, Details>>::Error;

    fn try_convert(value: serde_json::Value) -> Result<Exception<Details>, Self::Error> {
        let type_id = value
            .get("type")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| "ActionException".into());

        let details = value
            .get("message")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let details = <Converter as TryConvert<_, Details>>::try_convert(details)?;

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

/// Serialize a ready value back to JSON (for action-call arguments).
///
/// Implemented manually to avoid depending on serde's `Serialize` impl
/// being active — the conversion is direct via pattern matching.
impl TryConvert<ReadyValue, serde_json::Value> for Converter {
    type Error = PendingPromiseError;

    fn try_convert(value: ReadyValue) -> Result<serde_json::Value, Self::Error> {
        Ok(match value {
            ReadyValue::Int(i) => serde_json::Value::Number(i.into()),
            ReadyValue::Float(f) => {
                let n = serde_json::Number::from_f64(f.get())
                    .unwrap_or_else(|| serde_json::Number::from(0));
                serde_json::Value::Number(n)
            }
            ReadyValue::Bool(b) => serde_json::Value::Bool(b),
            ReadyValue::String(s) => serde_json::Value::String(s),
            ReadyValue::None => serde_json::Value::Null,
            ReadyValue::List(items) => serde_json::Value::Array(
                items
                    .into_iter()
                    .map(<Converter as TryConvert<_, serde_json::Value>>::try_convert)
                    .collect::<Result<_, _>>()?,
            ),
            ReadyValue::Dict(entries) => serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(k, v)| {
                        Ok((
                            k,
                            <Converter as TryConvert<_, serde_json::Value>>::try_convert(v)?,
                        ))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            ReadyValue::Exception(exc) => {
                <Converter as TryConvert<_, serde_json::Value>>::try_convert(*exc)?
            }
        })
    }
}

/// Convert a [`Value`] to JSON, erroring on [`Value::Pending`].
impl TryConvert<Value, serde_json::Value> for Converter {
    type Error = PendingPromiseError;

    fn try_convert(value: Value) -> Result<serde_json::Value, Self::Error> {
        match value {
            Value::Ready(v) => <Converter as TryConvert<_, serde_json::Value>>::try_convert(v),
            Value::Pending(id) => Err(PendingPromiseError(id)),
        }
    }
}

/// Convert an [`Exception`] to JSON.
///
/// Produces `{"type": "<type_id>", "message": <details>}`.
impl<Details> TryConvert<Exception<Details>, serde_json::Value> for Converter
where
    Converter: TryConvert<Details, serde_json::Value>,
{
    type Error = <Converter as TryConvert<Details, serde_json::Value>>::Error;

    fn try_convert(exc: Exception<Details>) -> Result<serde_json::Value, Self::Error> {
        let mut map = serde_json::Map::new();
        map.insert("type".into(), serde_json::Value::String(exc.type_id));
        map.insert(
            "message".into(),
            <Converter as TryConvert<_, serde_json::Value>>::try_convert(exc.details)?,
        );
        Ok(serde_json::Value::Object(map))
    }
}

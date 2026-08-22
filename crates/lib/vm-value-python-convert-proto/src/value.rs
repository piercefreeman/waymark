//! Conversions of the value tree itself: single values, exceptions, and
//! their encoded-payload forms.
//!
//! # Fidelity
//!
//! The proto value roster is wider than the VM's, so reading collapses
//! what the VM cannot hold: a tuple becomes a list, and a basemodel
//! becomes its data dict, dropping the defining module and name.  Both
//! await a VM value that can express them.
//!
//! A double that is not finite has no [`ReadyValue::Float`] to land in —
//! that variant is a `NonNaNFinite` — so it reads as
//! [`ReadyValue::None`].

use std::convert::Infallible;

use indexmap::IndexMap;
use typed_floats::NonNaNFinite;
use waymark_convert_core::{Convert as _, TryConvert};
use waymark_proto::python_value as proto_value;
use waymark_vm_runtime_exception::Exception;
use waymark_vm_value_python::{ReadyValue, Value};

use waymark_vm_value_convert_core::PendingPromiseError;

use crate::Converter;

fn primitive(kind: proto_value::primitive_value::Kind) -> proto_value::value::Kind {
    proto_value::value::Kind::Primitive(proto_value::PrimitiveValue { kind: Some(kind) })
}

impl TryConvert<&ReadyValue, proto_value::Value> for Converter {
    type Error = PendingPromiseError;

    fn try_convert(value: &ReadyValue) -> Result<proto_value::Value, Self::Error> {
        use proto_value::primitive_value::Kind as PrimitiveKind;
        use proto_value::value::Kind;

        let kind = match value {
            ReadyValue::Int(value) => primitive(PrimitiveKind::IntValue(*value)),
            ReadyValue::Float(value) => primitive(PrimitiveKind::DoubleValue(value.get())),
            ReadyValue::Bool(value) => primitive(PrimitiveKind::BoolValue(*value)),
            ReadyValue::String(value) => primitive(PrimitiveKind::StringValue(value.clone())),
            ReadyValue::None => primitive(PrimitiveKind::NullValue(0)),
            ReadyValue::List(items) => Kind::ListValue(proto_value::ListValue {
                items: items
                    .iter()
                    .map(Self::try_convert)
                    .collect::<Result<_, _>>()?,
            }),
            ReadyValue::Dict(entries) => Kind::DictValue(proto_value::DictValue {
                entries: entries
                    .iter()
                    .map(|(key, value)| {
                        Ok(proto_value::DictEntry {
                            key: key.clone(),
                            value: Some(Self::try_convert(value)?),
                        })
                    })
                    .collect::<Result<_, _>>()?,
            }),
            ReadyValue::Exception(exception) => {
                Kind::Exception(Box::new(Self::try_convert(&**exception)?))
            }
            // This flavor has no extension value: the type is
            // uninhabited, so there is nothing to write.
            ReadyValue::Extension(extension) => match *extension {},
        };

        Ok(proto_value::Value { kind: Some(kind) })
    }
}

impl TryConvert<&Value, proto_value::Value> for Converter {
    type Error = PendingPromiseError;

    fn try_convert(value: &Value) -> Result<proto_value::Value, Self::Error> {
        match value {
            Value::Ready(value) => Self::try_convert(value),
            Value::Pending(promise_state_id) => Err(PendingPromiseError(*promise_state_id)),
        }
    }
}

/// Write an exception, whatever its details are a value of.
///
/// The details ride as an ordinary value, so this holds for both the
/// promise-aware [`Value`] an exception carries inside a value tree and
/// the [`ReadyValue`] a settled outcome carries.
impl<'d, Details> TryConvert<&'d Exception<Details>, proto_value::ExceptionValue> for Converter
where
    Converter: TryConvert<&'d Details, proto_value::Value>,
{
    type Error = <Converter as TryConvert<&'d Details, proto_value::Value>>::Error;

    fn try_convert(
        exception: &'d Exception<Details>,
    ) -> Result<proto_value::ExceptionValue, Self::Error> {
        Ok(proto_value::ExceptionValue {
            type_id: exception.type_id.clone(),
            details: Some(Box::new(Self::try_convert(&exception.details)?)),
        })
    }
}

impl TryConvert<&proto_value::Value, ReadyValue> for Converter {
    type Error = Infallible;

    fn try_convert(value: &proto_value::Value) -> Result<ReadyValue, Self::Error> {
        use proto_value::value::Kind;

        let Some(kind) = &value.kind else {
            // A value naming no kind is as empty as the encoding can be.
            return Ok(ReadyValue::None);
        };

        Ok(match kind {
            Kind::Primitive(value) => Self::convert(value),
            // The VM has no value for a basemodel, so it arrives as the
            // data it carries; the defining module and name are dropped.
            Kind::Basemodel(basemodel) => match &basemodel.data {
                Some(data) => Self::convert(data),
                None => ReadyValue::Dict(Default::default()),
            },
            Kind::Exception(exception) => {
                ReadyValue::Exception(Box::new(Self::convert(&**exception)))
            }
            Kind::ListValue(list) => ReadyValue::List(items(&list.items)),
            // The VM has no tuple value, so a tuple arrives as a list.
            Kind::TupleValue(tuple) => ReadyValue::List(items(&tuple.items)),
            Kind::DictValue(dict) => Self::convert(dict),
        })
    }
}

fn items(items: &[proto_value::Value]) -> Vec<Value> {
    items.iter().map(Converter::convert).collect()
}

impl TryConvert<&proto_value::Value, Value> for Converter {
    type Error = Infallible;

    fn try_convert(value: &proto_value::Value) -> Result<Value, Self::Error> {
        Ok(Value::Ready(Self::convert(value)))
    }
}

impl TryConvert<&proto_value::PrimitiveValue, ReadyValue> for Converter {
    type Error = Infallible;

    fn try_convert(value: &proto_value::PrimitiveValue) -> Result<ReadyValue, Self::Error> {
        use proto_value::primitive_value::Kind;

        let Some(kind) = &value.kind else {
            return Ok(ReadyValue::None);
        };

        Ok(match kind {
            Kind::StringValue(value) => ReadyValue::String(value.clone()),
            Kind::DoubleValue(value) => match NonNaNFinite::try_from(*value) {
                Ok(value) => ReadyValue::Float(value),
                // The VM's float is finite and not NaN; anything else has
                // nowhere to land.
                Err(_) => ReadyValue::None,
            },
            Kind::IntValue(value) => ReadyValue::Int(*value),
            Kind::BoolValue(value) => ReadyValue::Bool(*value),
            Kind::NullValue(_) => ReadyValue::None,
        })
    }
}

impl TryConvert<&proto_value::DictValue, ReadyValue> for Converter {
    type Error = Infallible;

    fn try_convert(dict: &proto_value::DictValue) -> Result<ReadyValue, Self::Error> {
        let mut map = IndexMap::with_capacity(dict.entries.len());
        for entry in &dict.entries {
            let value = match &entry.value {
                Some(value) => Self::convert(value),
                None => Value::Ready(ReadyValue::None),
            };
            map.insert(entry.key.clone(), value);
        }
        Ok(ReadyValue::Dict(map))
    }
}

/// Read an exception into whatever its details are a value of.
///
/// An exception naming no details carries the value the flavor has for
/// nothing at all.
impl<'d, Details> TryConvert<&'d proto_value::ExceptionValue, Exception<Details>> for Converter
where
    Converter: TryConvert<&'d proto_value::Value, Details, Error = Infallible>,
    Details: From<ReadyValue>,
{
    type Error = Infallible;

    fn try_convert(
        exception: &'d proto_value::ExceptionValue,
    ) -> Result<Exception<Details>, Self::Error> {
        Ok(Exception {
            type_id: exception.type_id.clone(),
            details: exception
                .details
                .as_deref()
                .map(Self::convert)
                .unwrap_or_else(|| ReadyValue::None.into()),
        })
    }
}

/// Read a ready value back from an owned encoded-value payload.
impl TryConvert<Vec<u8>, ReadyValue> for Converter {
    type Error = prost::DecodeError;

    fn try_convert(bytes: Vec<u8>) -> Result<ReadyValue, Self::Error> {
        let message: proto_value::Value = prost::Message::decode(bytes.as_slice())?;
        Ok(Self::convert(&message))
    }
}

/// Read an exception back from an owned payload of its own wire
/// message.
///
/// The exception payload is carried as an encoded
/// [`proto_value::ExceptionValue`], a message of its own rather than a
/// value.
impl TryConvert<Vec<u8>, Exception<ReadyValue>> for Converter {
    type Error = prost::DecodeError;

    fn try_convert(bytes: Vec<u8>) -> Result<Exception<ReadyValue>, Self::Error> {
        let message: proto_value::ExceptionValue = prost::Message::decode(bytes.as_slice())?;
        Ok(Self::convert(&message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready(value: ReadyValue) -> Value {
        Value::Ready(value)
    }

    fn read(value: &proto_value::Value) -> ReadyValue {
        Converter::convert(value)
    }

    fn round_trip(value: ReadyValue) -> ReadyValue {
        let encoded: proto_value::Value =
            Converter::try_convert(&value).expect("no pending promise in the value");
        Converter::convert(&encoded)
    }

    #[test]
    fn primitives_round_trip() {
        for value in [
            ReadyValue::Int(42),
            ReadyValue::Int(-7),
            ReadyValue::Float(NonNaNFinite::try_from(1.25).unwrap()),
            ReadyValue::Bool(true),
            ReadyValue::String("hello".to_owned()),
            ReadyValue::None,
        ] {
            assert_eq!(round_trip(value.clone()), value);
        }
    }

    #[test]
    fn nested_containers_round_trip() {
        let value = ReadyValue::Dict(IndexMap::from([
            (
                "list".to_owned(),
                ready(ReadyValue::List(vec![
                    ready(ReadyValue::Int(1)),
                    ready(ReadyValue::String("two".to_owned())),
                ])),
            ),
            (
                "nested".to_owned(),
                ready(ReadyValue::Dict(IndexMap::from([(
                    "deep".to_owned(),
                    ready(ReadyValue::Bool(false)),
                )]))),
            ),
        ]));

        assert_eq!(round_trip(value.clone()), value);
    }

    #[test]
    fn dict_keeps_its_insertion_order() {
        // The order a dict was built in is part of the value: Python
        // dicts are insertion-ordered.  Sorting the keys en route — as a
        // JSON intermediary would — is a corruption, so the order has to
        // survive a round trip in the order given, not in the order the
        // keys happen to sort in.
        let keys = ["zebra", "apple", "mango", "banana"];
        let value = ReadyValue::Dict(
            keys.iter()
                .enumerate()
                .map(|(index, key)| ((*key).to_owned(), ready(ReadyValue::Int(index as i64))))
                .collect(),
        );

        let ReadyValue::Dict(entries) = round_trip(value) else {
            panic!("a dict round trips as a dict");
        };

        let round_tripped: Vec<_> = entries.keys().map(String::as_str).collect();
        assert_eq!(round_tripped, keys);
    }

    #[test]
    fn an_exception_stays_an_exception() {
        // The exception-ness is the point: it is what lets the VM settle
        // a promise raised rather than with a value.
        let exception = ReadyValue::Exception(Box::new(Exception {
            type_id: "ValueError".to_owned(),
            details: ready(ReadyValue::Dict(IndexMap::from([(
                "message".to_owned(),
                ready(ReadyValue::String("boom".to_owned())),
            )]))),
        }));

        assert_eq!(round_trip(exception.clone()), exception);
    }

    #[test]
    fn a_tuple_reads_as_a_list() {
        let tuple = proto_value::Value {
            kind: Some(proto_value::value::Kind::TupleValue(
                proto_value::TupleValue {
                    items: vec![
                        Converter::try_convert(&ReadyValue::Int(1)).unwrap(),
                        Converter::try_convert(&ReadyValue::Int(2)).unwrap(),
                    ],
                },
            )),
        };

        assert_eq!(
            read(&tuple),
            ReadyValue::List(vec![ready(ReadyValue::Int(1)), ready(ReadyValue::Int(2))]),
        );
    }

    #[test]
    fn a_basemodel_reads_as_its_data() {
        let basemodel = proto_value::Value {
            kind: Some(proto_value::value::Kind::Basemodel(
                proto_value::BaseModelValue {
                    module: "example".to_owned(),
                    name: "Model".to_owned(),
                    data: Some(proto_value::DictValue {
                        entries: vec![proto_value::DictEntry {
                            key: "field".to_owned(),
                            value: Some(Converter::try_convert(&ReadyValue::Int(3)).unwrap()),
                        }],
                    }),
                },
            )),
        };

        assert_eq!(
            read(&basemodel),
            ReadyValue::Dict(IndexMap::from([(
                "field".to_owned(),
                ready(ReadyValue::Int(3)),
            )])),
        );
    }

    #[test]
    fn a_non_finite_double_reads_as_none() {
        for double in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let value = proto_value::Value {
                kind: Some(primitive(proto_value::primitive_value::Kind::DoubleValue(
                    double,
                ))),
            };

            assert_eq!(read(&value), ReadyValue::None);
        }
    }

    #[test]
    fn an_encoded_value_payload_reads_back() {
        // A value payload IS an encoded proto value message, byte for
        // byte — what the framing-level `WorkflowArgument.value`
        // carries.
        let value = ReadyValue::Dict(IndexMap::from([
            ("zebra".to_owned(), ready(ReadyValue::Int(1))),
            ("apple".to_owned(), ready(ReadyValue::Int(2))),
        ]));

        let message: proto_value::Value =
            Converter::try_convert(&value).expect("no pending promise");
        let bytes = prost::Message::encode_to_vec(&message);

        let read: ReadyValue = Converter::try_convert(bytes).expect("the bytes decode");
        assert_eq!(read, value);
    }

    #[test]
    fn a_pending_promise_cannot_be_written() {
        let value = ReadyValue::List(vec![Value::Pending(
            waymark_vm_runtime_promise_core::PromiseStateId(7),
        )]);

        let written: Result<proto_value::Value, _> = Converter::try_convert(&value);

        assert_eq!(
            written.expect_err("a pending promise has no encoding"),
            PendingPromiseError(waymark_vm_runtime_promise_core::PromiseStateId(7)),
        );
    }
}

//! Argument-reading machinery shared by the action and workflow seams.

use waymark_convert_core::Convert as _;
use waymark_proto::python_value as proto_value;
use waymark_vm_value_python::ReadyValue;

use crate::Converter;

/// An argument entry named a key but carried no value, which is a
/// malformed arguments message rather than an argument that holds
/// nothing (this flavor's "nothing" is an encoded `None`).
#[derive(Debug, thiserror::Error)]
#[error("argument {key:?} carries no value")]
pub struct MissingArgumentValueError {
    /// The framing key of the value-less entry.
    pub key: String,
}

/// Read named-argument entries — the `(key, value)` shape both argument
/// messages share — into the map of ready values they carry.
///
/// An entry carrying no value is a [`MissingArgumentValueError`].
pub(crate) fn named_arguments<'a>(
    entries: impl Iterator<Item = (&'a String, Option<&'a proto_value::Value>)>,
) -> Result<std::collections::HashMap<String, ReadyValue>, MissingArgumentValueError> {
    entries
        .map(|(key, value)| {
            let value = value.ok_or_else(|| MissingArgumentValueError { key: key.clone() })?;
            Ok((key.clone(), Converter::convert(value)))
        })
        .collect()
}

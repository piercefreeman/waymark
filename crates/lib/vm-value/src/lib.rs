//! Canonical VM runtime value type.

#![warn(missing_docs)]

use indexmap::IndexMap;
use typed_floats::NonNaNFinite;

pub mod coreset;
pub mod extcallset;
pub mod pureset;
mod pythonic;

/// The VM value that is ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadyValue {
    /// Integer value.
    Int(i64),

    /// Non-NaN finite floating-point value.
    Float(NonNaNFinite),

    /// Boolean value.
    Bool(bool),

    /// String value.
    String(String),

    /// `None` value.
    ///
    /// Equivalent to `()` (unit) type in rust or `void` in C-like languages
    /// in semantics.
    None,

    /// Ordered list value.
    List(Vec<Value>),

    /// Dictionary value stored as an insertion-ordered string-keyed map.
    Dict(IndexMap<String, Value>),
}

impl ReadyValue {
    /// Returns whether the value is truthy using Python-like semantics.
    pub fn is_truthy(&self) -> bool {
        match self {
            Self::Int(value) => *value != 0,
            Self::Float(value) => value.get() != 0.0,
            Self::Bool(value) => *value,
            Self::String(value) => !value.is_empty(),
            Self::None => false,
            Self::List(items) => !items.is_empty(),
            Self::Dict(entries) => !entries.is_empty(),
        }
    }
}

/// The bound VM promise value type alias.
pub type PromiseValue = waymark_vm_runtime_promise_value::PromiseValue<ReadyValue>;

/// The final VM value type.
///
/// Use this type alias where you need to refer to the surface value type
/// without knowing the specifics of how the values are internally structured.
pub type Value = PromiseValue;

impl waymark_vm_runtime_value::RootValueAccess for ReadyValue {
    type RootValue = Value;
}

#[cfg(test)]
static_assertions::assert_impl_all!(Value: waymark_vm_interpreter_fullset::Value);

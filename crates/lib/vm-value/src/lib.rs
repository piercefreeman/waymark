//! Canonical VM runtime value type.

#![warn(missing_docs)]

use indexmap::IndexMap;
use typed_floats::NonNaNFinite;

pub mod coreset;
pub mod extcallset;
pub mod pureset;
mod pythonic;

/// Runtime VM values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
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
    List(Vec<Self>),

    /// Dictionary value stored as an insertion-ordered string-keyed map.
    Dict(IndexMap<String, Self>),
}

impl Value {
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

//! The shared VM runtime value shape.
//!
//! Pure data: the shape every variation of the VM operates on, with no
//! semantics attached. What a value *means* — how it adds, compares,
//! indexes, or coerces — belongs to the operations
//! (`waymark-vm-interpreter-operations` and the per-variation crates
//! implementing their vocabularies), not to the types here.

#![warn(missing_docs)]

use indexmap::IndexMap;
use typed_floats::NonNaNFinite;

mod exception;

/// The VM value that is ready.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

    /// Runtime exception value.
    Exception(Box<waymark_vm_runtime_exception::Exception<Value>>),
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
            Self::Exception(_) => true,
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

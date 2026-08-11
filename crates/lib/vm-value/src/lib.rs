//! Canonical VM runtime value type.

#![warn(missing_docs)]

use derive_where::derive_where;
use indexmap::IndexMap;
use typed_floats::NonNaNFinite;

pub mod coreset;
mod exception;
pub mod extcallset;
pub mod pureset;
mod pythonic;

/// The value flavor: the per-language configuration of [`ReadyValue`].
///
/// The flavor configures the ready value only — the root value shape is
/// structural, not flavor-chosen: containers always hold
/// [`Value<Flavor>`].
pub trait Flavor {
    /// The language-specific extension values hosted by
    /// [`ReadyValue::Extension`].
    ///
    /// Deliberately unbounded: every [`ReadyValue`] capability
    /// (`Debug`, `Clone`, `PartialEq`, serde) is conditional on this
    /// type providing the matching trait — an explicit requirement at
    /// the sites that need it, never a demand on the flavor.
    type Extension;
}

/// The VM value that is ready.
#[derive_where(Debug, Clone, PartialEq; Flavor::Extension)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(
        serialize = "Flavor::Extension: serde::Serialize",
        deserialize = "Flavor::Extension: serde::Deserialize<'de>",
    ))
)]
pub enum ReadyValue<Flavor: self::Flavor> {
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
    List(Vec<Value<Flavor>>),

    /// Dictionary value stored as an insertion-ordered string-keyed map.
    Dict(IndexMap<String, Value<Flavor>>),

    /// Runtime exception value.
    Exception(Box<waymark_vm_runtime_exception::Exception<Value<Flavor>>>),

    /// Language-specific extension value.
    Extension(Flavor::Extension),
}

impl<Flavor: self::Flavor> ReadyValue<Flavor> {
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
            Self::Extension(_) => true,
        }
    }
}

/// The [`Flavor::Extension`] type for flavors with no extension values.
///
/// Uninhabited: a `ReadyValue::Extension` of such a flavor can never be
/// constructed.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum NoExtension {}

/// The bound VM promise value type alias.
pub type PromiseValue<Flavor> = waymark_vm_runtime_promise_value::PromiseValue<ReadyValue<Flavor>>;

/// The final VM value of a flavor: the promise-aware surface value
/// wrapping [`ReadyValue`].
///
/// Use this type alias where you need to refer to the surface value type
/// without knowing the specifics of how the values are internally structured.
pub type Value<Flavor> = PromiseValue<Flavor>;

impl<Flavor: self::Flavor> waymark_vm_runtime_value::RootValueAccess for ReadyValue<Flavor> {
    type RootValue = Value<Flavor>;
}

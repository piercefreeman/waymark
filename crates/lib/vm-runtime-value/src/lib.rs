//! Core traits for representing values in the VM runtime.
//!
//! # Root Value
//!
//! Interpreters often operate on a "ready" value representation internally
//! while still needing operations like indexing or attribute access to return
//! the runtime's top-level value type. [`RootValueAccess`] captures that
//! relationship without hard-coding a concrete value implementation.
//!
//! In the canonical runtime, a ready value points at the promise-aware surface
//! value type, and wrapper types forward that association.
//!
//! ## Example
//!
//! ```rust
//! use waymark_vm_runtime_value::RootValueAccess;
//!
//! #[derive(Debug, Clone, PartialEq, Eq)]
//! struct ReadyValue(i64);
//!
//! enum Value {
//!     Ready(ReadyValue),
//!     Pending,
//! }
//!
//! impl RootValueAccess for ReadyValue {
//!     type RootValue = Value;
//! }
//!
//! struct Wrapper<T>(T);
//!
//! impl<T> RootValueAccess for Wrapper<T>
//! where
//!     T: RootValueAccess,
//! {
//!     type RootValue = T::RootValue;
//! }
//!
//! fn accept_runtime_value<T>(_: &T)
//! where
//!     T: RootValueAccess<RootValue = Value>,
//! {
//! }
//!
//! accept_runtime_value(&Wrapper(ReadyValue(42)));
//! ```

#![warn(missing_docs)]

/// Provides access to the runtime's root value type for a value representation.
///
/// This trait is intentionally minimal. It lets interpreter traits describe
/// operations that produce the runtime's top-level value type while remaining
/// generic over intermediate or wrapped value representations.
pub trait RootValueAccess {
    /// The root runtime value type produced by operations on `Self`.
    ///
    /// For a ready value, this is usually the surface value type exposed by the
    /// runtime. For wrapper types, this should usually forward to the wrapped
    /// value's `RootValue`.
    type RootValue;
}

//! The Python VM value.
//!
//! Currently a re-export of the canonical [`waymark_vm_value`] types;
//! becomes the real per-language value definition when the value
//! flavor lands.

#![warn(missing_docs)]

pub use waymark_vm_value::{PromiseValue, ReadyValue, Value};

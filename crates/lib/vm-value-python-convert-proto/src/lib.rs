//! [`Convert`](waymark_convert_core::Convert) implementations between the
//! Python flavor's proto values and VM values.
//!
//! The proto value tree is this flavor's current encoding of a single
//! value, so the conversion is direct: nothing stands between a
//! [`ReadyValue`](waymark_vm_value_python::ReadyValue) and the
//! [`Value`](waymark_proto::python_value::Value) carrying it.
//!
//! The conversions are grouped by seam: the value tree itself in
//! `value`, the action-call seam in `action`, and the workflow
//! initiation and completion seams in `workflow`; the argument-reading
//! machinery both seams share sits in `common`.

#![warn(missing_docs)]

mod action;
mod common;
mod value;
mod workflow;

pub use action::*;
pub use common::*;
pub use workflow::*;

/// Stateless converter for the value tree itself: single values,
/// exceptions, and their encoded payloads.
///
/// The seam converters delegate their value-level work here.
pub struct Converter;

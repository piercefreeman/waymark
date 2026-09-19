//! The Python VM value.
//!
//! Defines the Python value flavor marker and exports the concrete
//! Python value type aliases.

#![warn(missing_docs)]

/// The Python value flavor marker.
///
/// Public only so the value aliases can appear in downstream public
/// interfaces; refer to the values via the aliases, not the marker.
pub enum PythonFlavor {}

impl waymark_vm_value::Flavor for PythonFlavor {
    type Extension = waymark_vm_value::NoExtension;
}

/// The Python VM value that is ready.
pub type ReadyValue = waymark_vm_value::ReadyValue<PythonFlavor>;

/// The bound Python VM promise value type alias.
pub type PromiseValue = waymark_vm_value::PromiseValue<PythonFlavor>;

/// The final Python VM value type.
///
/// Use this type alias where you need to refer to the surface value type
/// without knowing the specifics of how the values are internally structured.
pub type Value = waymark_vm_value::Value<PythonFlavor>;

#[cfg(test)]
static_assertions::assert_impl_all!(Value: waymark_vm_interpreter_fullset::Value);

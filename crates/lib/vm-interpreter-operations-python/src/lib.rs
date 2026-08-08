//! The Python variation of the VM interpreter operations.

#![warn(missing_docs)]

pub mod coreset;
pub mod pureset;
mod pythonic;

/// The Python variation marker for the VM interpreter operations.
pub enum PythonVariation {}

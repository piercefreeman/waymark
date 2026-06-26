//! Core traits for the actions execution at runtime.

#![warn(missing_docs)]

mod outcomes_provider;
mod requester;

pub use crate::outcomes_provider::*;
pub use crate::requester::*;

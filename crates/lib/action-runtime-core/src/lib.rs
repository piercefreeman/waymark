//! Core traits for the actions execution at runtime.

#![warn(missing_docs)]

mod completions_provider;
mod requester;

pub use self::completions_provider::*;
pub use self::requester::*;

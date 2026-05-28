//! Integration tests for `ExcSetInterpreter` driven through `Runtime`.
//!
//! Tests are split by instruction behavior; each module builds a straight-line
//! program via the helpers in `support` and runs it to its terminal effect.

mod exception_details;
mod is_exception;
mod support;

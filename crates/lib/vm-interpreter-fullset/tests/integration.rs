//! Integration tests for [`FullSetInterpreter`] driven through [`Runtime`].
//!
//! Tests are split by the effect under exercise; each module owns one
//! straight-line program built via the helpers in [`support`].

mod support;

mod extcall;
mod select;
mod sleep;
mod state_entry;
mod synchronous;

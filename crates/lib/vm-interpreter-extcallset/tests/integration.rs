//! Integration tests for [`ExtCallSetInterpreter`] driven through [`Runtime`].
//!
//! Tests are split by the effect under exercise; each module owns one
//! straight-line program built via the helpers in [`support`].

mod support;

mod action_call;
mod sleep;
mod sleep_error;

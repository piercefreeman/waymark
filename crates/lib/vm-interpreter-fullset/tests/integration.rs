//! Integration tests for [`FullSetInterpreter`] driven through [`Runtime`].
//!
//! Tests are split by the effect under exercise; each module owns one
//! straight-line program built via the helpers in [`support`].

mod support;

mod exc;
mod extcall;
mod sleep;
mod synchronous;

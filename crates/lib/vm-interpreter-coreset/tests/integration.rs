//! Integration tests for [`CoreSetInterpreter`] driven through [`Runtime`].
//!
//! Tests are split into thematic submodules by the coreset feature under
//! test; each module builds a small executable via the helpers in
//! [`support`] and runs it to its terminal effect.

mod support;

mod call_await;
mod finally;
mod jump;
mod raise;

//! Integration tests for [`PureSetInterpreter`] driven through [`Runtime`].
//!
//! Tests are split into thematic submodules by the pureset feature under
//! test; each test builds a single straight-line function via the helpers in
//! [`support`] and runs it to its terminal effect.

mod support;

mod dict_ops;
mod length_ops;
mod list_ops;
mod scalar_ops;

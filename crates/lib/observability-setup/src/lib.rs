//! Observability setup for managing process-global observability state.

mod common;

pub use self::common::*;

#[cfg_attr(waymark_observability_trace, path = "impl.rs")]
#[cfg_attr(not(waymark_observability_trace), path = "mock.rs")]
mod r#impl;

pub use self::r#impl::*;

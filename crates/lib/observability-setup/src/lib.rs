//! Observability setup for managing process-global observability state.

mod common;

pub use self::common::*;

#[cfg_attr(feature = "trace", path = "impl.rs")]
#[cfg_attr(not(feature = "trace"), path = "mock.rs")]
mod r#impl;

pub use self::r#impl::*;

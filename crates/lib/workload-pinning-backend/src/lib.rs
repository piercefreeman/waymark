//! Backend traits for workload pinning.
//!
//! Defines the contract that a database backend must fulfill to support
//! workload pinning.
//!
//! For convenience, [`Backend`] is a blanket supertrait requiring all three.

#![warn(missing_docs)]

mod common;

pub mod keepalive;
pub mod poll;
pub mod release;

pub use self::common::*;

pub use self::keepalive::KeepalivePinnings;
pub use self::poll::PollUnpinnedWorkloads;
pub use self::release::ReleasePinnings;

/// All workload pinning backend traits.
pub trait Backend: PollUnpinnedWorkloads + KeepalivePinnings + ReleasePinnings {}

impl<T> Backend for T where T: PollUnpinnedWorkloads + KeepalivePinnings + ReleasePinnings {}

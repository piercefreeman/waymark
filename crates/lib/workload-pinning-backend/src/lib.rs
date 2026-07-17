//! Backend traits for workload pinning.
//!
//! Defines the contract that a database backend must fulfill to support
//! workload pinning.
//!
//! The store keeps the runnable set: the workloads that should run.
//! Nodes poll the set to pin workloads, keep their pinnings alive while
//! driving them, and unpin them when done — either releasing a workload
//! back to the set, or parking it, which removes it from the set until
//! it is made runnable again.
//!
//! For convenience, [`Backend`] is a blanket supertrait requiring all of
//! the operation traits.

#![warn(missing_docs)]

mod common;

pub mod keepalive;
pub mod poll;
pub mod release;
pub mod unpin;

pub use self::common::*;

pub use self::keepalive::KeepalivePinnings;
pub use self::poll::PollUnpinnedWorkloads;
pub use self::release::ReleasePinnings;
pub use self::unpin::UnpinWorkloads;

/// All workload pinning backend traits.
pub trait Backend:
    PollUnpinnedWorkloads + KeepalivePinnings + ReleasePinnings + UnpinWorkloads
{
}

impl<T> Backend for T where
    T: PollUnpinnedWorkloads + KeepalivePinnings + ReleasePinnings + UnpinWorkloads
{
}

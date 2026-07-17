//! Core types for workload pinning.

#![warn(missing_docs)]

/// What happens to a workload when it is unpinned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnpinMode {
    /// End the pinning but keep the workload runnable: it may be pinned
    /// again by any node.
    Release,

    /// End the pinning and remove the workload from the runnable set:
    /// it stays unpinnable until it is made runnable again.
    Park,
}

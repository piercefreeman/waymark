//! Core types for workload pinning.

#![warn(missing_docs)]

/// What happens to a workload when it is unpinned.
///
/// Both modes carry an obligation and neither is a safe default: the
/// choice is a claim about the workload's future, made at a site that
/// cannot see the state which makes the claim true.
///
/// - [`Release`] risks a **re-pin spinloop**: the workload returns to
///   the runnable set and is picked up again immediately, so if the next
///   attempt ends the way this one did, the cluster spins at poll rate.
/// - [`Park`] risks **liveness loss**: the workload leaves the runnable
///   set, and nothing brings it back on its own — there is no unpark
///   operation today at all.
///
/// The criterion is therefore not "release unless park applies"; it is a
/// separate positive justification per mode, spelled out on each
/// variant. Neither justification is local: both rest on invariants that
/// live far from the unpin site — what the next attempt would do, and
/// what else may concurrently need this workload to run. That is a known
/// weak spot of this design: the type system does not carry the proof,
/// so the caller has to construct it and write it down.
///
/// [`Release`]: UnpinMode::Release
/// [`Park`]: UnpinMode::Park
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnpinMode {
    /// End the pinning but keep the workload runnable: it may be pinned
    /// again by any node.
    ///
    /// Justified when the next attempt breaks the loop: either it may
    /// make progress, or it deterministically arrives at a point where a
    /// park decision is made. A release whose next attempt reproduces
    /// this one, with no park in reach, is a spinloop — not a no-op.
    Release,

    /// End the pinning and remove the workload from the runnable set:
    /// it stays unpinnable until a deliberate operation makes it
    /// runnable again.
    ///
    /// Justified only by a constructed guarantee that such an operation
    /// will happen when the workload needs to run — or that it never
    /// needs to run again (its terminal outcome is already durably
    /// recorded). "Nothing appears to need it" is not that guarantee.
    ///
    /// The guarantee must also survive the race: if something
    /// concurrently makes the workload need to run, the park must not
    /// land after it and swallow that need. Ordering the park against
    /// whatever produces the need is part of the justification.
    Park,
}

use crate::{LifecycleDecision, VmState};

/// A pluggable policy that evaluates VM state and produces a lifecycle decision.
///
/// Implementations should be cheap to call — the driver evaluates
/// policies after each effect and idle period.
pub trait LifecyclePolicy: Send + Sync + 'static {
    /// Evaluate the policy against the given VM state.
    ///
    /// Returns the recommended lifecycle action.
    fn evaluate(&self, state: &VmState) -> LifecycleDecision;
}

// ---------------------------------------------------------------------------
// IdleEvictionPolicy
// ---------------------------------------------------------------------------

/// Evicts VMs that have been idle for longer than the configured threshold.
///
/// Idle time is measured from the last recorded effect emission.
#[derive(Debug, Clone)]
pub struct IdleEvictionPolicy {
    /// Maximum idle duration before a VM is evicted.
    pub max_idle: chrono::Duration,
}

impl LifecyclePolicy for IdleEvictionPolicy {
    fn evaluate(&self, state: &VmState) -> LifecycleDecision {
        match state.idle_duration() {
            Some(idle) if idle >= self.max_idle => LifecycleDecision::Evict,
            _ => LifecycleDecision::NoAction,
        }
    }
}

// ---------------------------------------------------------------------------
// PeriodicSnapshotPolicy
// ---------------------------------------------------------------------------

/// Snapshots VMs at regular intervals as a safety net.
///
/// The primary persistence trigger is extcall-driven (the driver sends state
/// dumps after processing external calls). This policy serves as a backup:
/// it ensures a VM is persisted at least once per interval even if no extcalls
/// have occurred.
///
/// Only triggers if the VM has never been persisted or if the elapsed time
/// since the last snapshot exceeds the configured interval.
#[derive(Debug, Clone)]
pub struct PeriodicSnapshotPolicy {
    /// Minimum interval between snapshots.
    pub snapshot_interval: chrono::Duration,
}

impl LifecyclePolicy for PeriodicSnapshotPolicy {
    fn evaluate(&self, state: &VmState) -> LifecycleDecision {
        match state.duration_since_persist() {
            None => LifecycleDecision::Persist,
            Some(elapsed) if elapsed >= self.snapshot_interval => LifecycleDecision::Persist,
            _ => LifecycleDecision::NoAction,
        }
    }
}

// ---------------------------------------------------------------------------
// CompositePolicy
// ---------------------------------------------------------------------------

/// Combine multiple policies with a configurable combination strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompositeStrategy {
    /// At least one sub-policy must recommend the action.
    Any,
    /// All sub-policies must agree on the action.
    All,
}

/// A policy that combines multiple sub-policies.
///
/// The most "severe" action wins: `Evict` > `Persist` > `NoAction`.
#[derive(Debug, Clone)]
pub struct CompositePolicy<P> {
    /// The sub-policies to evaluate.
    pub policies: Vec<P>,

    /// How to combine sub-policy decisions.
    pub strategy: CompositeStrategy,
}

impl<P> CompositePolicy<P>
where
    P: LifecyclePolicy,
{
    /// Create a composite policy that requires *any* sub-policy to trigger.
    pub fn any(policies: Vec<P>) -> Self {
        Self {
            policies,
            strategy: CompositeStrategy::Any,
        }
    }

    /// Create a composite policy that requires *all* sub-policies to agree.
    pub fn all(policies: Vec<P>) -> Self {
        Self {
            policies,
            strategy: CompositeStrategy::All,
        }
    }
}

impl<P> LifecyclePolicy for CompositePolicy<P>
where
    P: LifecyclePolicy,
{
    fn evaluate(&self, state: &VmState) -> LifecycleDecision {
        match self.strategy {
            CompositeStrategy::Any => {
                let mut decision = LifecycleDecision::NoAction;
                for policy in &self.policies {
                    match policy.evaluate(state) {
                        LifecycleDecision::Evict => return LifecycleDecision::Evict,
                        LifecycleDecision::Persist => decision = LifecycleDecision::Persist,
                        LifecycleDecision::NoAction => {}
                    }
                }
                decision
            }
            CompositeStrategy::All => {
                let mut all_evict = true;
                let mut all_persist_or_above = true;

                for policy in &self.policies {
                    match policy.evaluate(state) {
                        LifecycleDecision::Evict => {}
                        LifecycleDecision::Persist => {
                            all_evict = false;
                        }
                        LifecycleDecision::NoAction => {
                            all_evict = false;
                            all_persist_or_above = false;
                        }
                    }
                }

                if all_evict {
                    LifecycleDecision::Evict
                } else if all_persist_or_above {
                    LifecycleDecision::Persist
                } else {
                    LifecycleDecision::NoAction
                }
            }
        }
    }
}

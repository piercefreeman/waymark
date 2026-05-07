//! Conditional-join helpers used while lowering `if`/`elif`/`else` chains.

use super::env::FlowState;

use nonempty_collections::NEVec;
use waymark_vm_bytecode_core::StateId;

/// Tracks whether a conditional needs a join state after lowering its branches.
pub enum ConditionalJoin {
    /// No continuation has been recorded yet.
    Pending(PendingConditionalJoin),

    /// At least one continuation reaches the join point.
    Ready(ReadyConditionalJoin),
}

/// The result of finishing conditional-join tracking.
pub enum ConditionalJoinFinish {
    /// No branch reaches a shared join point.
    NoJoin,

    /// Branches rejoin at a state with merged flow information.
    Join {
        /// The bytecode state where branches rejoin.
        join_state: StateId,

        /// The merged initialization state across all continuing branches.
        merged_flow: FlowState,
    },
}

/// Join metadata before any branch continuation has been recorded.
pub struct PendingConditionalJoin {
    /// Flow state that each branch starts from.
    incoming_flow: FlowState,

    /// State to jump to if any branch continues past the conditional.
    join_state: StateId,
}

/// Join metadata once one or more continuations have been recorded.
pub struct ReadyConditionalJoin {
    /// Flow state that each branch starts from.
    incoming_flow: FlowState,

    /// State to jump to when branches continue.
    join_state: StateId,

    /// Flow states contributed by branches that reach the join point.
    continuation_flows: NEVec<FlowState>,
}

impl ConditionalJoin {
    /// Creates conditional-join tracking for a new conditional expression.
    pub fn new(incoming_flow: &FlowState, join_state: StateId) -> Self {
        Self::Pending(PendingConditionalJoin {
            incoming_flow: incoming_flow.clone(),
            join_state,
        })
    }

    /// Returns the flow state that a new branch should start with.
    pub fn branch_flow(&self) -> FlowState {
        self.incoming_flow().clone()
    }

    /// Borrows the incoming flow shared by all branches.
    pub fn incoming_flow(&self) -> &FlowState {
        match self {
            Self::Pending(join) => &join.incoming_flow,
            Self::Ready(join) => &join.incoming_flow,
        }
    }

    /// Returns the join state reserved for the conditional.
    pub fn join_state(&self) -> StateId {
        match self {
            Self::Pending(join) => join.join_state,
            Self::Ready(join) => join.join_state,
        }
    }

    /// Records that execution can fall through without changing flow state.
    pub fn record_fallthrough(&mut self) {
        self.record_continuation(self.incoming_flow().clone());
    }

    /// Records a branch flow that reaches the conditional join point.
    pub fn record_continuation(&mut self, flow_state: FlowState) {
        match self {
            Self::Pending(join) => {
                *self = Self::Ready(ReadyConditionalJoin {
                    incoming_flow: join.incoming_flow.clone(),
                    join_state: join.join_state,
                    continuation_flows: NEVec::new(flow_state),
                });
            }
            Self::Ready(join) => {
                join.record_continuation(flow_state);
            }
        }
    }

    /// Finishes join tracking and returns whether a shared join is required.
    pub fn finish(self) -> ConditionalJoinFinish {
        match self {
            Self::Pending(_) => ConditionalJoinFinish::NoJoin,
            Self::Ready(join) => join.finish(),
        }
    }
}

impl ReadyConditionalJoin {
    /// Appends a continuing branch flow to the pending merge.
    fn record_continuation(&mut self, flow_state: FlowState) {
        self.continuation_flows.push(flow_state);
    }

    /// Produces the final join result with merged branch flow state.
    fn finish(self) -> ConditionalJoinFinish {
        ConditionalJoinFinish::Join {
            join_state: self.join_state,
            merged_flow: FlowState::merge_branches(self.continuation_flows),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use waymark_vm_bytecode_core::StateId;
    use waymark_vm_runtime_core::RegisterId;

    use crate::function::compiler::env::{FlowState, Locals};

    #[test]
    fn conditional_join_keeps_join_state_when_merging_paths() {
        let mut locals = Locals::new();
        let shared = locals
            .declare("shared".to_owned(), RegisterId(0))
            .expect("shared local should declare");

        let mut incoming_flow = FlowState::new();
        incoming_flow.mark_initialized(shared);

        let mut conditional_join = ConditionalJoin::new(&incoming_flow, StateId(9));
        conditional_join.record_fallthrough();
        conditional_join.record_continuation(incoming_flow.clone());

        let ConditionalJoinFinish::Join {
            join_state,
            merged_flow,
        } = conditional_join.finish()
        else {
            panic!("conditional join should merge recorded paths");
        };

        assert_eq!(join_state, StateId(9));
        assert!(merged_flow.is_initialized(shared));
    }

    #[test]
    fn conditional_join_without_continuations_has_no_join() {
        let incoming_flow = FlowState::new();
        let conditional_join = ConditionalJoin::new(&incoming_flow, StateId(9));

        assert!(matches!(
            conditional_join.finish(),
            ConditionalJoinFinish::NoJoin
        ));
    }
}

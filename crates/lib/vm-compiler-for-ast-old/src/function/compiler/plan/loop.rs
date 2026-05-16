//! Loop planning.

use super::super::env::FlowState;

use waymark_vm_bytecode_core::StateId;

/// Reserved state ids and flow data for lowering one `while` loop.
pub struct WhileLoopPlan {
    /// Flow state entering the loop.
    incoming_flow: FlowState,

    /// State where the loop condition is evaluated.
    condition_state: StateId,

    /// State where the loop body begins.
    body_state: StateId,

    /// State reached when the loop exits.
    exit_state: StateId,
}

/// Reserved state ids and flow data for lowering one `for` loop.
pub struct ForLoopPlan {
    /// Flow state entering the loop.
    incoming_flow: FlowState,

    /// State where the loop condition is evaluated.
    condition_state: StateId,

    /// State where the loop body begins.
    body_state: StateId,

    /// State reached by `continue` before re-checking the loop condition.
    continue_state: StateId,

    /// State reached when the loop exits.
    exit_state: StateId,
}

impl WhileLoopPlan {
    /// Creates a loop plan from the reserved condition, body, and exit states.
    pub fn new(
        incoming_flow: &FlowState,
        condition_state: StateId,
        body_state: StateId,
        exit_state: StateId,
    ) -> Self {
        Self {
            incoming_flow: incoming_flow.clone(),
            condition_state,
            body_state,
            exit_state,
        }
    }

    /// Returns the condition-state id.
    pub fn condition_state(&self) -> StateId {
        self.condition_state
    }

    /// Returns the body-state id.
    pub fn body_state(&self) -> StateId {
        self.body_state
    }

    /// Returns the loop-control scope used for `break` and `continue`.
    pub fn loop_scope(&self) -> crate::function::compiler::r#loop::LoopScope {
        crate::function::compiler::r#loop::LoopScope::new(self.exit_state, self.condition_state)
    }

    /// Returns the flow state to use when evaluating the condition.
    pub fn condition_flow(&self) -> FlowState {
        self.incoming_flow.clone()
    }

    /// Returns the flow state to use when entering the body.
    pub fn body_flow(&self) -> FlowState {
        self.incoming_flow.clone()
    }

    /// Returns the exit state and restored incoming flow for loop exit.
    pub fn finish(self) -> (StateId, FlowState) {
        (self.exit_state, self.incoming_flow)
    }
}

impl ForLoopPlan {
    /// Creates a loop plan from the reserved condition, body, continue, and
    /// exit states.
    pub fn new(
        incoming_flow: &FlowState,
        condition_state: StateId,
        body_state: StateId,
        continue_state: StateId,
        exit_state: StateId,
    ) -> Self {
        Self {
            incoming_flow: incoming_flow.clone(),
            condition_state,
            body_state,
            continue_state,
            exit_state,
        }
    }

    /// Returns the condition-state id.
    pub fn condition_state(&self) -> StateId {
        self.condition_state
    }

    /// Returns the body-state id.
    pub fn body_state(&self) -> StateId {
        self.body_state
    }

    /// Returns the continue-state id.
    pub fn continue_state(&self) -> StateId {
        self.continue_state
    }

    /// Returns the loop-control scope used for `break` and `continue`.
    pub fn loop_scope(&self) -> crate::function::compiler::r#loop::LoopScope {
        crate::function::compiler::r#loop::LoopScope::new(self.exit_state, self.continue_state)
    }

    /// Returns the flow state to use when evaluating the condition.
    pub fn condition_flow(&self) -> FlowState {
        self.incoming_flow.clone()
    }

    /// Returns the flow state to use when entering the body.
    pub fn body_flow(&self) -> FlowState {
        self.incoming_flow.clone()
    }

    /// Returns the flow state to use when executing the continue target.
    pub fn continue_flow(&self) -> FlowState {
        self.incoming_flow.clone()
    }

    /// Returns the exit state and restored incoming flow for loop exit.
    pub fn finish(self) -> (StateId, FlowState) {
        (self.exit_state, self.incoming_flow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use waymark_vm_bytecode_core::StateId;
    use waymark_vm_runtime_core::RegisterId;

    use crate::function::compiler::{
        env::{FlowState, Locals},
        r#loop::LoopControlKind,
    };

    #[test]
    fn while_loop_plan_restores_incoming_flow_at_exit() {
        let mut locals = Locals::new();
        let shared = locals
            .declare("shared".to_owned(), RegisterId(0))
            .expect("shared local should declare");

        let mut incoming_flow = FlowState::new();
        incoming_flow.mark_initialized(shared);

        let while_loop = WhileLoopPlan::new(&incoming_flow, StateId(4), StateId(5), StateId(6));
        let loop_scope = while_loop.loop_scope();

        assert_eq!(while_loop.condition_state(), StateId(4));
        assert_eq!(while_loop.body_state(), StateId(5));
        assert_eq!(loop_scope.target(LoopControlKind::Break), StateId(6));
        assert_eq!(loop_scope.target(LoopControlKind::Continue), StateId(4));
        assert!(while_loop.condition_flow().is_initialized(shared));
        assert!(while_loop.body_flow().is_initialized(shared));

        let (exit_state, exit_flow) = while_loop.finish();
        assert_eq!(exit_state, StateId(6));
        assert!(exit_flow.is_initialized(shared));
    }

    #[test]
    fn for_loop_plan_restores_incoming_flow_at_exit() {
        let mut locals = Locals::new();
        let shared = locals
            .declare("shared".to_owned(), RegisterId(0))
            .expect("shared local should declare");

        let mut incoming_flow = FlowState::new();
        incoming_flow.mark_initialized(shared);

        let for_loop = ForLoopPlan::new(
            &incoming_flow,
            StateId(4),
            StateId(5),
            StateId(6),
            StateId(7),
        );
        let loop_scope = for_loop.loop_scope();

        assert_eq!(for_loop.condition_state(), StateId(4));
        assert_eq!(for_loop.body_state(), StateId(5));
        assert_eq!(for_loop.continue_state(), StateId(6));
        assert_eq!(loop_scope.target(LoopControlKind::Break), StateId(7));
        assert_eq!(loop_scope.target(LoopControlKind::Continue), StateId(6));
        assert!(for_loop.condition_flow().is_initialized(shared));
        assert!(for_loop.body_flow().is_initialized(shared));
        assert!(for_loop.continue_flow().is_initialized(shared));

        let (exit_state, exit_flow) = for_loop.finish();
        assert_eq!(exit_state, StateId(7));
        assert!(exit_flow.is_initialized(shared));
    }
}

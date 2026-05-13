//! Loop-scope tracking for `break` and `continue` lowering.

use waymark_vm_bytecode_core::StateId;

/// Jump targets for the innermost active loop.
#[derive(Clone, Copy)]
pub(super) struct LoopScope {
    /// State reached by a `break`.
    break_state: StateId,

    /// State reached by a `continue`.
    continue_state: StateId,
}

/// Stack of loop scopes active during statement lowering.
#[derive(Clone, Default)]
pub(super) struct LoopControlStack {
    /// Active loop scopes from outermost to innermost.
    stack: Vec<LoopScope>,
}

/// The loop-only control statement encountered during compilation.
#[derive(Debug)]
pub enum LoopControlKind {
    /// Exit the innermost enclosing loop.
    Break,

    /// Jump to the next iteration of the innermost enclosing loop.
    Continue,
}

impl LoopScope {
    /// Creates a loop scope with its `break` and `continue` targets.
    pub(super) fn new(break_state: StateId, continue_state: StateId) -> Self {
        Self {
            break_state,
            continue_state,
        }
    }

    /// Returns the jump target for the given loop-control statement.
    pub(super) fn target(self, kind: LoopControlKind) -> StateId {
        match kind {
            LoopControlKind::Break => self.break_state,
            LoopControlKind::Continue => self.continue_state,
        }
    }
}

impl LoopControlStack {
    /// Creates an empty loop-control stack.
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Returns a new stack with `loop_scope` pushed as the innermost scope.
    pub(super) fn with_loop(&self, loop_scope: LoopScope) -> Self {
        let mut stack = self.stack.clone();
        stack.push(loop_scope);

        Self { stack }
    }

    /// Returns the innermost active loop scope, if any.
    pub(super) fn current(&self) -> Option<LoopScope> {
        self.stack.last().copied()
    }
}

impl core::fmt::Display for LoopControlKind {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::Break => "break",
            Self::Continue => "continue",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use waymark_vm_bytecode_core::StateId;

    #[test]
    fn innermost_loop_targets_win() {
        let loop_control = LoopControlStack::new()
            .with_loop(LoopScope::new(StateId(1), StateId(2)))
            .with_loop(LoopScope::new(StateId(3), StateId(4)));

        let Some(loop_scope) = loop_control.current() else {
            panic!("loop control should expose an innermost loop scope");
        };

        assert_eq!(loop_scope.target(LoopControlKind::Break), StateId(3));
        assert_eq!(loop_scope.target(LoopControlKind::Continue), StateId(4));
    }

    #[test]
    fn empty_loop_stack_has_no_targets() {
        let loop_control = LoopControlStack::new();

        assert!(loop_control.current().is_none());
    }

    #[test]
    fn entering_loop_returns_extended_scope_without_mutating_parent() {
        let outer = LoopControlStack::new().with_loop(LoopScope::new(StateId(1), StateId(2)));
        let inner = outer.with_loop(LoopScope::new(StateId(3), StateId(4)));

        let Some(outer_scope) = outer.current() else {
            panic!("outer loop control should expose a loop scope");
        };
        let Some(inner_scope) = inner.current() else {
            panic!("inner loop control should expose a loop scope");
        };

        assert_eq!(outer_scope.target(LoopControlKind::Break), StateId(1));
        assert_eq!(outer_scope.target(LoopControlKind::Continue), StateId(2));
        assert_eq!(inner_scope.target(LoopControlKind::Break), StateId(3));
        assert_eq!(inner_scope.target(LoopControlKind::Continue), StateId(4));
    }
}

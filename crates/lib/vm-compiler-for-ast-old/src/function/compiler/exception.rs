//! Exception-scope metadata shared across try/except lowering helpers.

use std::{cell::RefCell, rc::Rc};

use nonempty_collections::NEVec;
use waymark_vm_bytecode_core::StateId;
use waymark_vm_runtime_core::RegisterId;

use crate::Marked;

use super::env::FlowState;

/// Marker for registers that currently hold an active exception value while
/// dispatching `try`/`except` handlers.
pub(super) struct ExceptionMarker;

/// Stack of active `try`/`except` scopes visible at the current lowering point.
#[derive(Clone, Default)]
pub(super) struct ExceptionScopeStack {
    /// The innermost active exception scope, if any.
    current: Option<Rc<ExceptionScope>>,
}

/// Dispatch metadata for one lowered exception handler.
#[derive(Clone)]
pub(super) struct ExceptionHandlerDispatch {
    /// State that starts executing the handler body.
    entry_state: StateId,

    /// Registers that hold the handler's candidate exception type ids.
    exception_type_registers: Vec<RegisterId>,

    /// Whether this handler is the catch-all fallback for the scope.
    catch_all: bool,
}

/// Shared metadata for one lowered `try`/`except` statement.
pub(super) struct ExceptionScope {
    /// The next outer exception scope used for propagation.
    outer: ExceptionScopeStack,

    /// Register that stores the exception value while dispatching handlers.
    exception_register: Marked<RegisterId, ExceptionMarker>,

    /// Handler dispatch order for this scope.
    handlers: Vec<ExceptionHandlerDispatch>,

    /// Flow states observed at each handler entry across all failure sites.
    handler_flows: RefCell<Vec<Option<NEVec<FlowState>>>>,
}

impl ExceptionScopeStack {
    /// Creates a new scope stack with one more innermost scope.
    pub(super) fn push(&self, scope: ExceptionScope) -> Self {
        Self {
            current: Some(Rc::new(scope)),
        }
    }

    /// Returns the innermost active scope.
    pub(super) fn current_scope(&self) -> Option<Rc<ExceptionScope>> {
        self.current.clone()
    }

    /// Returns whether there is no active exception scope.
    pub(super) fn is_empty(&self) -> bool {
        self.current.is_none()
    }
}

impl ExceptionHandlerDispatch {
    /// Builds one handler-dispatch entry.
    pub(super) fn new(
        entry_state: StateId,
        exception_type_registers: Vec<RegisterId>,
        catch_all: bool,
    ) -> Self {
        Self {
            entry_state,
            exception_type_registers,
            catch_all,
        }
    }

    /// Returns the handler entry state.
    pub(super) fn entry_state(&self) -> StateId {
        self.entry_state
    }

    /// Returns the handler's candidate type-id registers.
    pub(super) fn exception_type_registers(&self) -> &[RegisterId] {
        &self.exception_type_registers
    }

    /// Returns whether this handler matches any exception.
    pub(super) fn is_catch_all(&self) -> bool {
        self.catch_all
    }
}

impl ExceptionScope {
    /// Builds one active exception scope.
    pub(super) fn new(
        outer: ExceptionScopeStack,
        exception_register: Marked<RegisterId, ExceptionMarker>,
        handlers: Vec<ExceptionHandlerDispatch>,
    ) -> Self {
        let handler_flows = RefCell::new(vec![None; handlers.len()]);

        Self {
            outer,
            exception_register,
            handlers,
            handler_flows,
        }
    }

    /// Returns the outer exception scope used for propagation.
    pub(super) fn outer(&self) -> ExceptionScopeStack {
        self.outer.clone()
    }

    /// Returns the scope-local register that stores the active exception.
    pub(super) fn exception_register(&self) -> Marked<RegisterId, ExceptionMarker> {
        self.exception_register
    }

    /// Returns the handlers in source order.
    pub(super) fn handlers(&self) -> &[ExceptionHandlerDispatch] {
        &self.handlers
    }

    /// Records that `flow_state` can enter `handler_index`.
    pub(super) fn record_handler_flow(&self, handler_index: usize, flow_state: &FlowState) {
        let mut handler_flows = self.handler_flows.borrow_mut();
        let slot = &mut handler_flows[handler_index];

        match slot {
            Some(flows) => flows.push(flow_state.clone()),
            None => *slot = Some(NEVec::new(flow_state.clone())),
        }
    }

    /// Returns the intersected entry flow per handler, or `None` for handlers
    /// that were never reached by any failure site.
    pub(super) fn handler_entry_flows(&self) -> Vec<Option<FlowState>> {
        self.handler_flows
            .borrow()
            .iter()
            .map(|flows| flows.clone().map(FlowState::intersect_branches))
            .collect()
    }
}

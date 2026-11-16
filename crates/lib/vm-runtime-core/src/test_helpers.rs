//! Shared helpers for this crate's tests.

use std::cell::Cell;
use std::rc::Rc;

use waymark_vm_runtime_promise_core::{PromiseStateId, TracePromiseStateIds};

use crate::PromiseStateHandle;

/// A promise state's value for the tests: a number, holding no handle.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct TestNumber(pub(crate) i32);

impl TracePromiseStateIds for TestNumber {
    fn trace_promise_state_ids(&self, _visit: &mut dyn FnMut(PromiseStateId)) {}
}

/// A promise state's value for the tests: promise state handles, and a count
/// of the times a trace went through them.
#[derive(Debug)]
pub(crate) struct TestHandles {
    /// The handles the value holds.
    pub(crate) handles: Vec<PromiseStateHandle>,

    /// The traces through this value, shared with the test.
    pub(crate) traces: Rc<Cell<usize>>,
}

impl TracePromiseStateIds for TestHandles {
    fn trace_promise_state_ids(&self, visit: &mut dyn FnMut(PromiseStateId)) {
        self.traces.set(self.traces.get() + 1);
        for handle in &self.handles {
            visit(handle.id());
        }
    }
}

/// A raised exception for the tests that holds promise state handles, and a
/// count of the times a trace went through them.
#[derive(Debug)]
pub(crate) struct TestHandlesException {
    /// The handles the exception holds.
    pub(crate) handles: Vec<PromiseStateHandle>,

    /// The traces through this exception, shared with the test.
    pub(crate) traces: Rc<Cell<usize>>,
}

impl waymark_vm_runtime_exception::HasMatchPattern for TestHandlesException {
    type Pattern = ();
}

impl TracePromiseStateIds for TestHandlesException {
    fn trace_promise_state_ids(&self, visit: &mut dyn FnMut(PromiseStateId)) {
        self.traces.set(self.traces.get() + 1);
        for handle in &self.handles {
            visit(handle.id());
        }
    }
}

impl TracePromiseStateIds for TestException {
    fn trace_promise_state_ids(&self, _visit: &mut dyn FnMut(PromiseStateId)) {}
}

/// A raised exception for the tests: a class name, matched by equality.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TestException(pub(crate) &'static str);

impl waymark_vm_runtime_exception::HasMatchPattern for TestException {
    type Pattern = &'static str;
}

impl waymark_vm_runtime_exception::Match for TestException {
    fn matches(&self, pattern: &Self::Pattern) -> bool {
        self.0 == *pattern
    }
}

/// A raised exception for the snapshot tests: serde can read it back from
/// owned bytes.
#[cfg(feature = "serde")]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct RestoreTestException(pub(crate) String);

#[cfg(feature = "serde")]
impl waymark_vm_runtime_exception::HasMatchPattern for RestoreTestException {
    type Pattern = String;
}

#[cfg(feature = "serde")]
impl TracePromiseStateIds for RestoreTestException {
    fn trace_promise_state_ids(&self, _visit: &mut dyn FnMut(PromiseStateId)) {}
}

//! Test harness for the coreset interpreter integration tests.
//!
//! Defines a minimal [`TestSpec`], a hand-rolled promise-aware [`TestValue`]
//! that satisfies the coreset interpreter's trait bounds, and a
//! single-function runtime builder shared by all thematic test submodules.
//!
//! # Why not `PromiseValue`?
//!
//! `waymark-vm-runtime-promise-value` sits *above* the interpreter crates in
//! the build graph (it depends on them), so a per-interpreter test crate
//! must not pull it back in as a dev-dependency — that would be a backward
//! edge against the established layering. Hand-roll the promise-aware value
//! type here instead. The sibling interpreter test crates (extcallset,
//! pureset, fullset) follow the same rule.

use waymark_vm_bytecode::Executable;
use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_interpreter_coreset::CoreSetInterpreter;
use waymark_vm_runtime::{CallSpec, Runtime};
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_exception::{
    Exception, FromException, IntoException, NotAnOwnedExceptionError,
};
use waymark_vm_runtime_promise_core::{
    PromiseStateId, Resolvable, Suspendable, UnresolvedPromiseError,
};
use waymark_vm_runtime_test::{FunctionId, StateId};

pub type Instruction = CoreSet<TestSpec>;
pub type Interpreter = CoreSetInterpreter<TestSpec, Executable<Instruction>, TestValue>;
pub type TestRuntime = Runtime<Executable<Instruction>, Interpreter, TestValue>;

// --- Spec ---

#[derive(Debug)]
pub struct TestSpec;

impl waymark_vm_instructions_coreset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type FunctionId = FunctionId;
    type StateId = StateId;
}

// --- Value ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestReadyValue {
    Int(i64),
    Exception(Box<Exception<TestValue>>),
}

/// Minimal promise-aware value: ready holds a [`TestReadyValue`], pending
/// holds a promise state id.
///
/// Hand-rolled on purpose: see the module-level note on why we don't reuse
/// `PromiseValue` here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestValue {
    Ready(TestReadyValue),
    Pending(PromiseStateId),
}

impl waymark_vm_runtime_value::RootValueAccess for TestValue {
    type RootValue = Self;
}

impl waymark_vm_interpreter_coreset::value::CaptureCallArgument for TestValue {
    fn capture_call_argument(&self) -> Self {
        self.clone()
    }
}

impl waymark_vm_interpreter_coreset::value::ShouldJump for TestValue {
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        match self {
            Self::Ready(TestReadyValue::Int(value)) => Ok(*value != 0),
            Self::Ready(TestReadyValue::Exception(_)) => Ok(true),
            Self::Pending(_) => Err(waymark_vm_interpreter_coreset::value::NotAConditionalError),
        }
    }
}

impl waymark_vm_interpreter_coreset::value::FromRaceArmIndex for TestValue {
    fn from_race_arm_index(
        arm_index: usize,
    ) -> Result<Self, waymark_vm_interpreter_coreset::value::FromRaceArmIndexError> {
        let arm_index = i64::try_from(arm_index)
            .map_err(|_| waymark_vm_interpreter_coreset::value::FromRaceArmIndexError)?;
        Ok(Self::Ready(TestReadyValue::Int(arm_index)))
    }
}

impl Suspendable for TestValue {
    fn from_pending(promise_state_id: PromiseStateId) -> Self {
        Self::Pending(promise_state_id)
    }

    fn as_pending(&self) -> Option<PromiseStateId> {
        match self {
            Self::Pending(promise_state_id) => Some(*promise_state_id),
            Self::Ready(_) => None,
        }
    }
}

impl Resolvable for TestValue {
    type ReadyValue = TestReadyValue;

    fn from_ready(value: TestReadyValue) -> Self {
        Self::Ready(value)
    }

    fn into_ready(self) -> Result<TestReadyValue, (UnresolvedPromiseError, Self)> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Pending(promise_state_id) => Err((
                UnresolvedPromiseError { promise_state_id },
                Self::Pending(promise_state_id),
            )),
        }
    }

    fn as_ready(&self) -> Result<&TestReadyValue, UnresolvedPromiseError> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Pending(promise_state_id) => Err(UnresolvedPromiseError {
                promise_state_id: *promise_state_id,
            }),
        }
    }

    fn as_ready_mut(&mut self) -> Result<&mut TestReadyValue, UnresolvedPromiseError> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Pending(promise_state_id) => Err(UnresolvedPromiseError {
                promise_state_id: *promise_state_id,
            }),
        }
    }
}

impl FromException for TestValue {
    fn from_exception(exception: Exception<Self::RootValue>) -> Self {
        Self::Ready(TestReadyValue::Exception(Box::new(exception)))
    }
}

impl IntoException for TestValue {
    fn into_exception(self) -> Result<Exception<Self::RootValue>, NotAnOwnedExceptionError<Self>> {
        match self {
            Self::Ready(TestReadyValue::Exception(exception)) => Ok(*exception),
            value => Err(NotAnOwnedExceptionError { value }),
        }
    }
}

// --- Runtime builder ---

/// Build a runtime that enters [`FunctionId(0)`] with the given ready
/// arguments.
pub fn new_runtime_with_args(
    executable: Executable<Instruction>,
    args: Vec<TestReadyValue>,
) -> TestRuntime {
    Runtime::with_custom_entrypoint(
        Interpreter::default(),
        executable,
        CallSpec {
            func: FunctionId(0),
            args: args.into_iter().map(TestValue::Ready).collect(),
        },
    )
    .expect("function 0 should exist")
}

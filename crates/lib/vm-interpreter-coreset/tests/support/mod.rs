//! Test harness for the coreset interpreter integration tests.
//!
//! Defines a minimal [`TestSpec`], a hand-rolled promise-aware [`TestValue`]
//! that satisfies the coreset interpreter's trait bounds, and a
//! single-function runtime builder shared by all thematic test submodules.
//!
//! The coreset raises nothing, so the raised exception is
//! [`core::convert::Infallible`].
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
use waymark_vm_runtime_core::{
    Promisable, PromisableReadiness, PromisableReadinessMut, PromisableReadinessRef,
    PromiseStateHandle, RegisterId,
};
use waymark_vm_runtime_test::{FunctionId, StateId};

pub type Instruction = CoreSet<TestSpec>;
pub type Interpreter =
    CoreSetInterpreter<TestSpec, Executable<Instruction>, TestValue, core::convert::Infallible>;
pub type TestRuntime =
    Runtime<Executable<Instruction>, Interpreter, TestValue, core::convert::Infallible>;

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
pub enum TestMaterialValue {
    Int(i64),
}

/// Minimal promise-aware value: ready holds a [`TestMaterialValue`], pending
/// holds a promise state handle.
///
/// Hand-rolled on purpose: see the module-level note on why we don't reuse
/// `PromiseValue` here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestValue {
    Ready(TestMaterialValue),
    Pending(PromiseStateHandle),
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
            Self::Ready(TestMaterialValue::Int(value)) => Ok(*value != 0),
            Self::Pending(_) => Err(waymark_vm_interpreter_coreset::value::NotAConditionalError),
        }
    }
}

impl Promisable for TestValue {
    type ReadyValue = TestMaterialValue;

    fn from_ready(value: TestMaterialValue) -> Self {
        Self::Ready(value)
    }

    fn from_pending(promise_state_handle: PromiseStateHandle) -> Self {
        Self::Pending(promise_state_handle)
    }

    fn promisable_readiness(&self) -> PromisableReadinessRef<'_, TestMaterialValue> {
        match self {
            Self::Ready(value) => PromisableReadinessRef::Ready(value),
            Self::Pending(promise_state_handle) => {
                PromisableReadinessRef::Pending(promise_state_handle)
            }
        }
    }

    fn promisable_readiness_mut(&mut self) -> PromisableReadinessMut<'_, TestMaterialValue> {
        match self {
            Self::Ready(value) => PromisableReadinessMut::Ready(value),
            Self::Pending(promise_state_handle) => {
                PromisableReadinessMut::Pending(promise_state_handle)
            }
        }
    }

    fn into_promisable_readiness(self) -> PromisableReadiness<TestMaterialValue> {
        match self {
            Self::Ready(value) => PromisableReadiness::Ready(value),
            Self::Pending(promise_state_handle) => {
                PromisableReadiness::Pending(promise_state_handle)
            }
        }
    }
}

// --- Runtime builder ---

/// Build a runtime that enters [`FunctionId(0)`] with the given ready
/// arguments.
pub fn new_runtime_with_args(
    executable: Executable<Instruction>,
    args: Vec<TestMaterialValue>,
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

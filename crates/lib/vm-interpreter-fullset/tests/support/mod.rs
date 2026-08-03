//! Test harness for the fullset interpreter integration tests.
//!
//! Defines a [`TestSpec`] that wires the pure, core, and extcall instruction
//! sets onto common id types, a hand-rolled promise-aware [`TestValue`] that
//! implements every trait the interpreter requires, and helpers for building
//! a [`Runtime`] from a single-function `Executable`.
//!
//! # Why not `PromiseValue`?
//!
//! `waymark-vm-runtime-promise-value` sits *above* the interpreter crates in
//! the build graph (it depends on them), so a per-interpreter test crate
//! must not pull it back in as a dev-dependency — that would be a backward
//! edge against the established layering. Hand-roll the promise-aware value
//! type here instead. The sibling interpreter test crates (coreset,
//! pureset, extcallset) follow the same rule.

use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_bytecode::Executable;
use waymark_vm_instructions_fullset::FullSet;
use waymark_vm_instructions_pureset::{BinaryOpKind, UnaryOpKind};
use waymark_vm_interpreter_fullset::FullSetInterpreter;
use waymark_vm_runtime::{CallSpec, Runtime};
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_exception::{
    Exception, FromException, IntoException, NotAnOwnedExceptionError,
};
use waymark_vm_runtime_promise_core::{PromiseStateId, UnresolvedPromiseError};
use waymark_vm_runtime_test::{FunctionId, StateId};

pub type Instruction = FullSet<TestSpec>;
pub type Interpreter = FullSetInterpreter<TestSpec, Executable<Instruction>, TestValue>;
pub type TestRuntime = Runtime<Executable<Instruction>, Interpreter, TestValue>;

// --- Spec ---

#[derive(Debug)]
pub struct TestSpec;

impl waymark_vm_instructions_coreset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type FunctionId = FunctionId;
    type StateId = StateId;
}

impl waymark_vm_instructions_extcallset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type StateId = StateId;
    type ActionRef = TestActionRef;
}

impl waymark_vm_instructions_pureset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type ConstValue = TestConstValue;
}

// --- Constants & action refs ---

#[derive(Debug, Clone)]
pub enum TestConstValue {
    Int(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestActionRef(pub usize);

// --- Errors ---

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TestSleepDurationError {
    #[error("the value cannot be used as a sleep duration")]
    UnsupportedValue,

    #[error("sleep duration must be non-zero")]
    Zero,

    #[error("sleep duration cannot be negative")]
    Negative,
}

// --- Value ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestReadyValue {
    Int(i64),
    Bool(bool),
    Text(String),
    List(Vec<TestValue>),
    Exception(Box<Exception<TestValue>>),
}

impl waymark_vm_runtime_value::RootValueAccess for TestReadyValue {
    type RootValue = TestValue;
}

fn is_truthy(value: &TestReadyValue) -> bool {
    match value {
        TestReadyValue::Int(value) => *value != 0,
        TestReadyValue::Bool(value) => *value,
        TestReadyValue::Text(value) => !value.is_empty(),
        TestReadyValue::List(items) => !items.is_empty(),
        TestReadyValue::Exception(_) => true,
    }
}

impl waymark_vm_interpreter_coreset::value::CaptureCallArgument for TestReadyValue {
    fn capture_call_argument(&self) -> Self {
        self.clone()
    }
}

impl waymark_vm_interpreter_coreset::value::ShouldJump for TestReadyValue {
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        Ok(is_truthy(self))
    }
}

impl waymark_vm_interpreter_pureset::value::CaptureCopy for TestReadyValue {
    fn capture_copy(&self) -> Self {
        self.clone()
    }
}

impl waymark_vm_interpreter_pureset::value::LoadConst<&TestConstValue> for TestReadyValue {
    fn load_const(const_value: &TestConstValue) -> Self {
        match const_value {
            TestConstValue::Int(val) => Self::Int(*val),
        }
    }
}

impl waymark_vm_interpreter_pureset::value::AsScalar for TestReadyValue {
    type Scalar = TestReadyValue;

    fn as_scalar(
        &self,
    ) -> Result<&Self::Scalar, waymark_vm_interpreter_pureset::value::AsScalarError> {
        Ok(self)
    }

    fn from_scalar(scalar: Self::Scalar) -> Self {
        scalar
    }
}

impl waymark_vm_interpreter_pureset::value::BinaryOps for TestReadyValue {
    fn add(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match (a, b) {
            (Self::Int(a), Self::Int(b)) => Ok(Self::Int(*a + *b)),
            _ => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                    operation: BinaryOpKind::Add,
                },
            ),
        }
    }
}

impl waymark_vm_interpreter_pureset::value::UnaryOps for TestReadyValue {
    fn neg(
        value: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::UnaryOperationError> {
        match value {
            Self::Int(value) => Ok(Self::Int(-*value)),
            _ => Err(
                waymark_vm_interpreter_pureset::value::UnaryOperationError::UnsupportedOperation {
                    operation: UnaryOpKind::Neg,
                },
            ),
        }
    }

    fn not(
        value: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::UnaryOperationError> {
        Ok(Self::Bool(!is_truthy(value)))
    }
}

impl waymark_vm_interpreter_pureset::value::MakeList for TestReadyValue {
    fn make_list<I>(items: I) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeListError>
    where
        I: IntoIterator<Item = Self::RootValue>,
    {
        Ok(Self::List(items.into_iter().collect()))
    }
}

impl waymark_vm_interpreter_pureset::value::ListAppend for TestReadyValue {
    fn list_append(
        list: &Self,
        item: Self::RootValue,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::ListAppendError> {
        let Self::List(existing) = list else {
            return Err(waymark_vm_interpreter_pureset::value::ListAppendError::NotListable);
        };
        let mut grown = existing.clone();
        grown.push(item);
        Ok(Self::List(grown))
    }
}

impl waymark_vm_interpreter_pureset::value::AsDictKey for TestReadyValue {
    fn as_dict_key(&self) -> Result<&str, waymark_vm_interpreter_pureset::value::AsDictKeyError> {
        Err(waymark_vm_interpreter_pureset::value::AsDictKeyError::UnsupportedKeyType)
    }
}

impl waymark_vm_interpreter_pureset::value::MakeDict for TestReadyValue {
    fn make_dict<I>(
        entries: I,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeDictError>
    where
        I: IntoIterator<Item = (String, Self::RootValue)>,
    {
        let _ = entries;
        Err(waymark_vm_interpreter_pureset::value::MakeDictError::NotDictable)
    }
}

impl waymark_vm_interpreter_pureset::value::AsExceptionTypeId for TestReadyValue {
    fn as_exception_type_id(
        &self,
    ) -> Result<&str, waymark_vm_interpreter_pureset::value::AsExceptionTypeIdError> {
        Err(waymark_vm_interpreter_pureset::value::AsExceptionTypeIdError::UnsupportedTypeIdType)
    }
}

impl waymark_vm_interpreter_pureset::value::MakeException for TestReadyValue {
    fn make_exception(type_id: String, details: Self::RootValue) -> Self {
        Self::Exception(Box::new(Exception { type_id, details }))
    }
}

impl waymark_vm_runtime_exception::ExceptionFromIntermediate<String> for TestReadyValue {
    fn from_intermediate_exception(exception: Exception<String>) -> Exception<Self::RootValue> {
        Exception {
            type_id: exception.type_id,
            details: TestValue::Ready(Self::Text(exception.details)),
        }
    }
}

impl waymark_vm_interpreter_pureset::value::Length for TestReadyValue {
    type Length = usize;

    fn length(&self) -> Result<usize, waymark_vm_interpreter_pureset::value::LengthError> {
        match self {
            Self::List(items) => Ok(items.len()),
            Self::Int(_) | Self::Bool(_) | Self::Text(_) | Self::Exception(_) => {
                Err(waymark_vm_interpreter_pureset::value::LengthError::UnsupportedValue)
            }
        }
    }

    fn from_length(
        length: usize,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::FromLengthError> {
        let value = i64::try_from(length).map_err(|_| {
            waymark_vm_interpreter_pureset::value::FromLengthError::ResultOutOfBounds
        })?;
        Ok(Self::Int(value))
    }
}

impl waymark_vm_interpreter_pureset::value::IndexOp for TestReadyValue {}

impl waymark_vm_interpreter_pureset::value::DotOp for TestReadyValue {}

impl waymark_vm_interpreter_extcallset::value::SleepDuration for TestReadyValue {
    type Error = TestSleepDurationError;

    fn to_sleep_duration(&self) -> Result<NonZeroDuration, Self::Error> {
        match self {
            Self::Int(value) => {
                let seconds: u64 = (*value).try_into().map_err(|_| Self::Error::Negative)?;
                NonZeroDuration::from_secs(seconds).ok_or(Self::Error::Zero)
            }
            Self::Bool(_) | Self::Text(_) | Self::List(_) | Self::Exception(_) => {
                Err(Self::Error::UnsupportedValue)
            }
        }
    }
}

impl waymark_vm_interpreter_extcallset::value::CaptureActionCallArgument for TestReadyValue {
    type Error = core::convert::Infallible;

    type ActionCallArgument = TestReadyValue;

    fn capture_action_call_argument(&self) -> Result<Self::ActionCallArgument, Self::Error> {
        Ok(self.clone())
    }
}

impl From<&TestConstValue> for TestReadyValue {
    fn from(value: &TestConstValue) -> Self {
        match value {
            TestConstValue::Int(value) => Self::Int(*value),
        }
    }
}

// --- Promise-aware wrapper ---
//
// Hand-rolled on purpose: see the module-level note on why we don't reuse
// `PromiseValue` here. Each impl mirrors the corresponding blanket impl in
// `waymark-vm-runtime-promise-value` so the wrapper's semantics match what
// the higher layers expect.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestValue {
    Ready(TestReadyValue),
    Pending(PromiseStateId),
}

#[derive(Debug, thiserror::Error)]
pub enum TestValueError<Inner> {
    #[error(transparent)]
    UnresolvedPromise(#[from] UnresolvedPromiseError),

    #[error(transparent)]
    Ready(Inner),
}

impl TestValue {
    fn require_ready_ref(&self) -> Result<&TestReadyValue, UnresolvedPromiseError> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Pending(promise_state_id) => Err(UnresolvedPromiseError {
                promise_state_id: *promise_state_id,
            }),
        }
    }
}

impl waymark_vm_runtime_value::RootValueAccess for TestValue {
    type RootValue = TestValue;
}

impl waymark_vm_runtime_promise_core::Suspendable for TestValue {
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

impl waymark_vm_runtime_promise_core::Resolvable for TestValue {
    type ReadyValue = TestReadyValue;

    fn from_ready(value: Self::ReadyValue) -> Self {
        Self::Ready(value)
    }

    fn into_ready(self) -> Result<Self::ReadyValue, (UnresolvedPromiseError, Self)> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Pending(promise_state_id) => Err((
                UnresolvedPromiseError { promise_state_id },
                Self::Pending(promise_state_id),
            )),
        }
    }

    fn as_ready(&self) -> Result<&Self::ReadyValue, UnresolvedPromiseError> {
        self.require_ready_ref()
    }

    fn as_ready_mut(&mut self) -> Result<&mut Self::ReadyValue, UnresolvedPromiseError> {
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

impl waymark_vm_interpreter_coreset::value::CaptureCallArgument for TestValue {
    fn capture_call_argument(&self) -> Self {
        match self {
            Self::Ready(value) => Self::Ready(value.capture_call_argument()),
            Self::Pending(promise_state_id) => Self::Pending(*promise_state_id),
        }
    }
}

impl waymark_vm_interpreter_coreset::value::ShouldJump for TestValue {
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        let value = self
            .require_ready_ref()
            .map_err(|_| waymark_vm_interpreter_coreset::value::NotAConditionalError)?;
        value.should_jump()
    }
}

impl waymark_vm_interpreter_pureset::value::CaptureCopy for TestValue {
    fn capture_copy(&self) -> Self {
        match self {
            Self::Ready(value) => Self::Ready(value.capture_copy()),
            Self::Pending(promise_state_id) => Self::Pending(*promise_state_id),
        }
    }
}

impl waymark_vm_interpreter_pureset::value::LoadConst<&TestConstValue> for TestValue {
    fn load_const(const_value: &TestConstValue) -> Self {
        Self::Ready(TestReadyValue::load_const(const_value))
    }
}

impl waymark_vm_interpreter_pureset::value::AsScalar for TestValue {
    type Scalar = TestReadyValue;

    fn as_scalar(
        &self,
    ) -> Result<&Self::Scalar, waymark_vm_interpreter_pureset::value::AsScalarError> {
        self.require_ready_ref()
            .map_err(|_| waymark_vm_interpreter_pureset::value::AsScalarError::NotAScalar)
    }

    fn from_scalar(scalar: Self::Scalar) -> Self {
        Self::Ready(scalar)
    }
}

impl waymark_vm_interpreter_pureset::value::BinaryOps for TestValue {
    fn add(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        let a = a.require_ready_ref().map_err(|_| {
            waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                operation: BinaryOpKind::Add,
            }
        })?;
        let b = b.require_ready_ref().map_err(|_| {
            waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                operation: BinaryOpKind::Add,
            }
        })?;
        Ok(Self::Ready(TestReadyValue::add(a, b)?))
    }
}

impl waymark_vm_interpreter_pureset::value::UnaryOps for TestValue {
    fn neg(
        value: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::UnaryOperationError> {
        let value = value.require_ready_ref().map_err(|_| {
            waymark_vm_interpreter_pureset::value::UnaryOperationError::UnsupportedOperation {
                operation: UnaryOpKind::Neg,
            }
        })?;
        Ok(Self::Ready(TestReadyValue::neg(value)?))
    }

    fn not(
        value: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::UnaryOperationError> {
        let value = value.require_ready_ref().map_err(|_| {
            waymark_vm_interpreter_pureset::value::UnaryOperationError::UnsupportedOperation {
                operation: UnaryOpKind::Not,
            }
        })?;
        Ok(Self::Ready(TestReadyValue::not(value)?))
    }
}

impl waymark_vm_interpreter_pureset::value::MakeList for TestValue {
    fn make_list<I>(items: I) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeListError>
    where
        I: IntoIterator<Item = Self::RootValue>,
    {
        Ok(Self::Ready(TestReadyValue::List(
            items.into_iter().collect(),
        )))
    }
}

impl waymark_vm_interpreter_pureset::value::ListAppend for TestValue {
    fn list_append(
        list: &Self,
        item: Self::RootValue,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::ListAppendError> {
        let list = list
            .require_ready_ref()
            .map_err(|_| waymark_vm_interpreter_pureset::value::ListAppendError::NotListable)?;
        let grown =
            <TestReadyValue as waymark_vm_interpreter_pureset::value::ListAppend>::list_append(
                list, item,
            )?;
        Ok(Self::Ready(grown))
    }
}

impl waymark_vm_interpreter_pureset::value::AsDictKey for TestValue {
    fn as_dict_key(&self) -> Result<&str, waymark_vm_interpreter_pureset::value::AsDictKeyError> {
        let value = self.require_ready_ref().map_err(|_| {
            waymark_vm_interpreter_pureset::value::AsDictKeyError::UnsupportedKeyType
        })?;
        value.as_dict_key()
    }
}

impl waymark_vm_interpreter_pureset::value::MakeDict for TestValue {
    fn make_dict<I>(
        entries: I,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeDictError>
    where
        I: IntoIterator<Item = (String, Self::RootValue)>,
    {
        let _ = entries;
        Err(waymark_vm_interpreter_pureset::value::MakeDictError::NotDictable)
    }
}

impl waymark_vm_interpreter_pureset::value::AsExceptionTypeId for TestValue {
    fn as_exception_type_id(
        &self,
    ) -> Result<&str, waymark_vm_interpreter_pureset::value::AsExceptionTypeIdError> {
        let value = self.require_ready_ref().map_err(|_| {
            waymark_vm_interpreter_pureset::value::AsExceptionTypeIdError::UnsupportedTypeIdType
        })?;
        value.as_exception_type_id()
    }
}

impl waymark_vm_interpreter_pureset::value::MakeException for TestValue {
    fn make_exception(type_id: String, details: Self::RootValue) -> Self {
        Self::Ready(TestReadyValue::make_exception(type_id, details))
    }
}

impl<IntermediateDetails>
    waymark_vm_runtime_exception::ExceptionFromIntermediate<IntermediateDetails> for TestValue
where
    TestReadyValue: waymark_vm_runtime_exception::ExceptionFromIntermediate<IntermediateDetails>,
{
    fn from_intermediate_exception(
        exception: Exception<IntermediateDetails>,
    ) -> Exception<Self::RootValue> {
        <TestReadyValue as waymark_vm_runtime_exception::ExceptionFromIntermediate<
            IntermediateDetails,
        >>::from_intermediate_exception(exception)
    }
}

impl waymark_vm_interpreter_pureset::value::Length for TestValue {
    type Length = usize;

    fn length(&self) -> Result<Self::Length, waymark_vm_interpreter_pureset::value::LengthError> {
        let value = self
            .require_ready_ref()
            .map_err(|_| waymark_vm_interpreter_pureset::value::LengthError::UnsupportedValue)?;
        value.length()
    }

    fn from_length(
        length: Self::Length,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::FromLengthError> {
        Ok(Self::Ready(TestReadyValue::from_length(length)?))
    }
}

impl waymark_vm_interpreter_pureset::value::IndexOp for TestValue {}

impl waymark_vm_interpreter_pureset::value::DotOp for TestValue {}

impl waymark_vm_interpreter_extcallset::value::SleepDuration for TestValue {
    type Error = TestValueError<TestSleepDurationError>;

    fn to_sleep_duration(&self) -> Result<NonZeroDuration, Self::Error> {
        let value = self.require_ready_ref()?;
        value.to_sleep_duration().map_err(TestValueError::Ready)
    }
}

impl waymark_vm_interpreter_extcallset::value::CaptureActionCallArgument for TestValue {
    type Error = TestValueError<core::convert::Infallible>;
    type ActionCallArgument = TestReadyValue;

    fn capture_action_call_argument(&self) -> Result<Self::ActionCallArgument, Self::Error> {
        let value = self.require_ready_ref()?;
        value
            .capture_action_call_argument()
            .map_err(TestValueError::Ready)
    }
}

// --- Runtime builders ---

/// Build a runtime that enters [`FunctionId(0)`] via the conventional
/// argument-less entrypoint.
pub fn new_runtime(executable: Executable<Instruction>) -> TestRuntime {
    Runtime::with_conventional_entrypoint(Interpreter::default(), executable)
        .expect("function 0 should exist")
}

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

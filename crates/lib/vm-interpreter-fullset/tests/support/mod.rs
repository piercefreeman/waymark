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
pub type Interpreter =
    FullSetInterpreter<TestSpec, Executable<Instruction>, TestOperations, TestValue>;
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

impl waymark_vm_interpreter_pureset::operations::BinaryOps<TestReadyValue> for TestOperations {
    type Error = TestOperationError;

    fn add(a: &TestReadyValue, b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        match (a, b) {
            (TestReadyValue::Int(a), TestReadyValue::Int(b)) => Ok(TestReadyValue::Int(*a + *b)),
            _ => Err(TestOperationError::type_error(format!(
                "{} is not supported for these operands",
                BinaryOpKind::Add
            ))),
        }
    }

    fn sub(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Sub
        )))
    }

    fn mul(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Mul
        )))
    }

    fn div(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Div
        )))
    }

    fn floor_div(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::FloorDiv
        )))
    }

    fn modulo(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Mod
        )))
    }

    fn eq(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Eq
        )))
    }

    fn ne(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Ne
        )))
    }

    fn lt(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Lt
        )))
    }

    fn le(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Le
        )))
    }

    fn gt(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Gt
        )))
    }

    fn ge(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Ge
        )))
    }

    fn contains(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::In
        )))
    }

    fn not_contains(
        _a: &TestReadyValue,
        _b: &TestReadyValue,
    ) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::NotIn
        )))
    }

    fn and(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::And
        )))
    }

    fn or(_a: &TestReadyValue, _b: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Or
        )))
    }
}

impl waymark_vm_interpreter_pureset::operations::UnaryOps<TestReadyValue> for TestOperations {
    type Error = TestOperationError;

    fn neg(value: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        match value {
            TestReadyValue::Int(value) => Ok(TestReadyValue::Int(-*value)),
            _ => Err(TestOperationError::type_error(format!(
                "{} is not supported for this operand",
                UnaryOpKind::Neg
            ))),
        }
    }

    fn not(value: &TestReadyValue) -> Result<TestReadyValue, Self::Error> {
        Ok(TestReadyValue::Bool(!is_truthy(value)))
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

/// The operations the fullset interpreter is instantiated with in these
/// tests. A bare local marker: the interpreter is generic over any
/// operations type, so no wrapper machinery is involved.
pub enum TestOperations {}

/// The error space of the test operations: a variation owns the errors
/// of its semantic operations, including the exception type id each one
/// raises.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct TestOperationError {
    pub type_id: &'static str,
    pub message: String,
}

impl TestOperationError {
    fn new(type_id: &'static str, message: impl Into<String>) -> Self {
        Self {
            type_id,
            message: message.into(),
        }
    }

    fn type_error(message: impl Into<String>) -> Self {
        Self::new("TypeError", message)
    }
}

impl waymark_vm_runtime_exception::TypedException for TestOperationError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        waymark_vm_runtime_exception::Exception {
            type_id: self.type_id.to_owned(),
            details: self.message,
        }
    }
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

#[derive(Debug, thiserror::Error)]
#[error("the value is not a conditional")]
pub struct TestNotAConditionalError;

impl waymark_vm_interpreter_coreset::operations::CaptureCallArgument<TestValue> for TestOperations {
    fn capture_call_argument(value: &TestValue) -> TestValue {
        match value {
            TestValue::Ready(value) => TestValue::Ready(value.clone()),
            TestValue::Pending(promise_state_id) => TestValue::Pending(*promise_state_id),
        }
    }
}

impl waymark_vm_interpreter_coreset::operations::ShouldJump<TestValue> for TestOperations {
    type Error = TestNotAConditionalError;

    fn should_jump(value: &TestValue) -> Result<bool, Self::Error> {
        let value = value
            .require_ready_ref()
            .map_err(|_| TestNotAConditionalError)?;
        Ok(is_truthy(value))
    }
}

impl waymark_vm_interpreter_pureset::operations::CaptureCopy<TestValue> for TestOperations {
    fn capture_copy(value: &TestValue) -> TestValue {
        value.clone()
    }
}

impl waymark_vm_interpreter_pureset::operations::LoadConst<TestValue, &TestConstValue>
    for TestOperations
{
    fn load_const(const_value: &TestConstValue) -> TestValue {
        TestValue::Ready(TestReadyValue::from(const_value))
    }
}

impl waymark_vm_interpreter_pureset::operations::AsScalarValue<TestValue> for TestOperations {
    type ScalarValue = TestReadyValue;
    type Error = TestOperationError;

    fn as_scalar_value(value: &TestValue) -> Result<&Self::ScalarValue, Self::Error> {
        value
            .require_ready_ref()
            .map_err(|_| TestOperationError::type_error("not a scalar"))
    }

    fn from_scalar_value(scalar: Self::ScalarValue) -> TestValue {
        TestValue::Ready(scalar)
    }
}

impl waymark_vm_interpreter_pureset::operations::MakeList<TestValue> for TestOperations {
    fn make_list<I>(
        items: I,
    ) -> Result<TestValue, waymark_vm_interpreter_pureset::operations::MakeListError>
    where
        I: IntoIterator<Item = TestValue>,
    {
        Ok(TestValue::Ready(TestReadyValue::List(
            items.into_iter().collect(),
        )))
    }
}

impl waymark_vm_interpreter_pureset::operations::ListAppend<TestValue> for TestOperations {
    fn list_append(
        list: &TestValue,
        item: TestValue,
    ) -> Result<TestValue, waymark_vm_interpreter_pureset::operations::ListAppendError> {
        let list = list.require_ready_ref().map_err(|_| {
            waymark_vm_interpreter_pureset::operations::ListAppendError::NotListable
        })?;
        let TestReadyValue::List(existing) = list else {
            return Err(waymark_vm_interpreter_pureset::operations::ListAppendError::NotListable);
        };
        let mut grown = existing.clone();
        grown.push(item);
        Ok(TestValue::Ready(TestReadyValue::List(grown)))
    }
}

impl waymark_vm_interpreter_pureset::operations::AsDictKey<TestValue> for TestOperations {
    type Error = TestOperationError;

    fn as_dict_key(_value: &TestValue) -> Result<&str, Self::Error> {
        Err(TestOperationError::type_error(
            "dict keys of this type are not supported",
        ))
    }
}

impl waymark_vm_interpreter_pureset::operations::MakeDict<TestValue> for TestOperations {
    fn make_dict<I>(
        entries: I,
    ) -> Result<TestValue, waymark_vm_interpreter_pureset::operations::MakeDictError>
    where
        I: IntoIterator<Item = (String, TestValue)>,
    {
        let _ = entries;
        Err(waymark_vm_interpreter_pureset::operations::MakeDictError::NotDictable)
    }
}

impl waymark_vm_interpreter_pureset::operations::AsExceptionTypeId<TestValue> for TestOperations {
    fn as_exception_type_id(
        value: &TestValue,
    ) -> Result<&str, waymark_vm_interpreter_pureset::operations::AsExceptionTypeIdError> {
        let value = value.require_ready_ref().map_err(|_| {
            waymark_vm_interpreter_pureset::operations::AsExceptionTypeIdError::UnsupportedTypeIdType
        })?;
        match value {
            TestReadyValue::Text(value) => Ok(value),
            _ => Err(
                waymark_vm_interpreter_pureset::operations::AsExceptionTypeIdError::UnsupportedTypeIdType,
            ),
        }
    }
}

impl waymark_vm_interpreter_pureset::operations::MakeException<TestValue> for TestOperations {
    fn make_exception(type_id: String, details: TestValue) -> TestValue {
        TestValue::Ready(TestReadyValue::Exception(Box::new(Exception {
            type_id,
            details,
        })))
    }
}

impl waymark_vm_runtime_exception::ExceptionFromIntermediate<String, TestValue> for TestOperations {
    fn from_intermediate_exception(exception: Exception<String>) -> Exception<TestValue> {
        Exception {
            type_id: exception.type_id,
            details: TestValue::Ready(TestReadyValue::Text(exception.details)),
        }
    }
}

impl waymark_vm_interpreter_pureset::operations::Length<TestValue> for TestOperations {
    type Length = usize;
    type Error = TestOperationError;
    type FromLengthError = TestOperationError;

    fn length(value: &TestValue) -> Result<Self::Length, Self::Error> {
        let value = value.require_ready_ref().map_err(|_| {
            TestOperationError::type_error("determining length is not supported for this value")
        })?;
        match value {
            TestReadyValue::List(items) => Ok(items.len()),
            TestReadyValue::Int(_)
            | TestReadyValue::Bool(_)
            | TestReadyValue::Text(_)
            | TestReadyValue::Exception(_) => Err(TestOperationError::type_error(
                "determining length is not supported for this value",
            )),
        }
    }

    fn from_length(length: Self::Length) -> Result<TestValue, Self::FromLengthError> {
        let value = i64::try_from(length).map_err(|_| {
            TestOperationError::new("OverflowError", "length result is out of bounds")
        })?;
        Ok(TestValue::Ready(TestReadyValue::Int(value)))
    }
}

impl waymark_vm_interpreter_pureset::operations::IndexOp<TestValue> for TestOperations {
    type Error = TestOperationError;

    fn index(_object: &TestValue, _index: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(
            "indexed access is not supported for these operands",
        ))
    }
}

impl waymark_vm_interpreter_pureset::operations::DotOp<TestValue> for TestOperations {
    type Error = TestOperationError;

    fn dot(_object: &TestValue, _attribute: &str) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(
            "attribute access is not supported for this value",
        ))
    }
}

impl waymark_vm_interpreter_extcallset::operations::SleepDuration<TestValue> for TestOperations {
    type Error = TestValueError<TestSleepDurationError>;

    fn to_sleep_duration(value: &TestValue) -> Result<NonZeroDuration, Self::Error> {
        let value = value.require_ready_ref()?;
        match value {
            TestReadyValue::Int(value) => {
                let seconds: u64 = (*value)
                    .try_into()
                    .map_err(|_| TestValueError::Ready(TestSleepDurationError::Negative))?;
                NonZeroDuration::from_secs(seconds)
                    .ok_or(TestValueError::Ready(TestSleepDurationError::Zero))
            }
            TestReadyValue::Bool(_)
            | TestReadyValue::Text(_)
            | TestReadyValue::List(_)
            | TestReadyValue::Exception(_) => Err(TestValueError::Ready(
                TestSleepDurationError::UnsupportedValue,
            )),
        }
    }
}

impl waymark_vm_interpreter_extcallset::operations::CaptureActionCallArgument<TestValue>
    for TestOperations
{
    type Error = TestValueError<core::convert::Infallible>;
    type ActionCallArgument = TestReadyValue;

    fn capture_action_call_argument(
        value: &TestValue,
    ) -> Result<Self::ActionCallArgument, Self::Error> {
        let value = value.require_ready_ref()?;
        Ok(value.clone())
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

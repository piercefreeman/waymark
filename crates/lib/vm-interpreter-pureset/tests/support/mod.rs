//! Test harness for the pureset interpreter integration tests.
//!
//! Provides a minimal spec, a value type that implements every pureset
//! value trait, a `RuntimeInstruction` that wraps `PureSet` plus a couple of
//! test-only instructions, and a driver that runs a single straight-line
//! function to its terminal effect.

use std::collections::BTreeMap;

use waymark_vm_instructions_pureset::{BinaryOpKind, PureSet, UnaryOpKind};
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_pureset::{Error, PureSetInterpreter};
use waymark_vm_runtime::{RunError, Runtime};
use waymark_vm_runtime_core::{Frame, RegisterId};
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};

// --- Spec ---

#[derive(Debug)]
pub struct TestSpec;

impl waymark_vm_instructions_pureset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type ConstValue = TestConstValue;
}

// --- Constants ---

#[derive(Debug, Clone)]
pub enum TestConstValue {
    Int(i64),
    Text(&'static str),
    OverflowLength,
}

// --- Value ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestValue {
    Int(i64),
    Bool(bool),
    Text(String),
    List(Vec<TestValue>),
    Dict(BTreeMap<String, TestValue>),
    Exception {
        type_id: String,
        details: Box<TestValue>,
    },
    /// Test-only sentinel for values that should be rejected by pureset
    /// operations without coupling these tests to promise-specific runtime
    /// behavior.
    Unusable,
    OverflowLength,
}

impl waymark_vm_runtime_value::RootValueAccess for TestValue {
    type RootValue = TestValue;
}

/// A local variation marker: the interpreter is generic over any
/// operations type; these tests instantiate it with the operations
/// wrapper over this marker.
pub enum TestVariation {}

pub type TestOperations = waymark_vm_interpreter_operations::Operations<TestVariation>;

static_assertions::assert_impl_all!(
    TestOperations: waymark_vm_interpreter_pureset::Operations<TestValue>
);
static_assertions::assert_impl_all!(
    TestOperations: waymark_vm_interpreter_pureset::operations::Exceptions<TestValue>
);

/// The error space of the test operations.
///
/// A variation owns the errors of its semantic operations, including the
/// exception type id each one raises; this fixture keeps one type for all
/// of them.
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

impl waymark_vm_interpreter_pureset::operations::CaptureCopy<TestValue> for TestOperations {
    fn capture_copy(value: &TestValue) -> TestValue {
        value.clone()
    }
}

impl waymark_vm_interpreter_pureset::operations::LoadConst<TestValue, &TestConstValue>
    for TestOperations
{
    fn load_const(const_value: &TestConstValue) -> TestValue {
        match const_value {
            TestConstValue::Int(value) => TestValue::Int(*value),
            TestConstValue::Text(value) => TestValue::Text((*value).to_owned()),
            TestConstValue::OverflowLength => TestValue::OverflowLength,
        }
    }
}

fn is_truthy(value: &TestValue) -> bool {
    match value {
        TestValue::Int(value) => *value != 0,
        TestValue::Bool(value) => *value,
        TestValue::Text(value) => !value.is_empty(),
        TestValue::List(items) => !items.is_empty(),
        TestValue::Dict(entries) => !entries.is_empty(),
        TestValue::Exception { .. } => true,
        TestValue::Unusable => unreachable!("unusable values are not scalar"),
        TestValue::OverflowLength => true,
    }
}

fn normalized_index(index: i64, len: usize) -> Option<usize> {
    if index >= 0 {
        let index = usize::try_from(index).ok()?;
        return (index < len).then_some(index);
    }

    let distance_from_end = usize::try_from(index.unsigned_abs()).ok()?;
    (distance_from_end <= len).then_some(len - distance_from_end)
}

impl waymark_vm_interpreter_pureset::operations::AsScalarValue<TestValue> for TestOperations {
    type ScalarValue = TestValue;
    type Error = TestOperationError;

    fn as_scalar_value(value: &TestValue) -> Result<&Self::ScalarValue, Self::Error> {
        match value {
            TestValue::Unusable => Err(TestOperationError::type_error("not a scalar")),
            _ => Ok(value),
        }
    }

    fn from_scalar_value(scalar: Self::ScalarValue) -> TestValue {
        scalar
    }
}

impl waymark_vm_interpreter_pureset::operations::BinaryOps<TestValue> for TestOperations {
    type Error = TestOperationError;

    fn add(a: &TestValue, b: &TestValue) -> Result<TestValue, Self::Error> {
        match (a, b) {
            (TestValue::Int(a), TestValue::Int(b)) => Ok(TestValue::Int(*a + *b)),
            _ => Err(TestOperationError::type_error(format!(
                "{} is not supported for these operands",
                BinaryOpKind::Add
            ))),
        }
    }

    fn sub(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Sub
        )))
    }

    fn mul(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Mul
        )))
    }

    fn div(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Div
        )))
    }

    fn floor_div(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::FloorDiv
        )))
    }

    fn modulo(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Mod
        )))
    }

    fn eq(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Eq
        )))
    }

    fn ne(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Ne
        )))
    }

    fn lt(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Lt
        )))
    }

    fn le(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Le
        )))
    }

    fn gt(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Gt
        )))
    }

    fn ge(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Ge
        )))
    }

    fn contains(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::In
        )))
    }

    fn not_contains(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::NotIn
        )))
    }

    fn and(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::And
        )))
    }

    fn or(_a: &TestValue, _b: &TestValue) -> Result<TestValue, Self::Error> {
        Err(TestOperationError::type_error(format!(
            "{} is not supported for these operands",
            BinaryOpKind::Or
        )))
    }
}

impl waymark_vm_interpreter_pureset::operations::UnaryOps<TestValue> for TestOperations {
    type Error = TestOperationError;

    fn neg(value: &TestValue) -> Result<TestValue, Self::Error> {
        match value {
            TestValue::Int(value) => Ok(TestValue::Int(-*value)),
            _ => Err(TestOperationError::type_error(format!(
                "{} is not supported for this operand",
                UnaryOpKind::Neg
            ))),
        }
    }

    fn not(value: &TestValue) -> Result<TestValue, Self::Error> {
        Ok(TestValue::Bool(!is_truthy(value)))
    }
}

impl waymark_vm_interpreter_pureset::operations::MakeList<TestValue> for TestOperations {
    fn make_list<I>(
        items: I,
    ) -> Result<TestValue, waymark_vm_interpreter_pureset::operations::MakeListError>
    where
        I: IntoIterator<Item = TestValue>,
    {
        Ok(TestValue::List(items.into_iter().collect()))
    }
}

impl waymark_vm_interpreter_pureset::operations::ListAppend<TestValue> for TestOperations {
    fn list_append(
        list: &TestValue,
        item: TestValue,
    ) -> Result<TestValue, waymark_vm_interpreter_pureset::operations::ListAppendError> {
        let TestValue::List(existing) = list else {
            return Err(waymark_vm_interpreter_pureset::operations::ListAppendError::NotListable);
        };
        let mut grown = existing.clone();
        grown.push(item);
        Ok(TestValue::List(grown))
    }
}

impl waymark_vm_interpreter_pureset::operations::AsDictKey<TestValue> for TestOperations {
    type Error = TestOperationError;

    fn as_dict_key(value: &TestValue) -> Result<&str, Self::Error> {
        match value {
            TestValue::Text(value) => Ok(value),
            TestValue::Int(_)
            | TestValue::Bool(_)
            | TestValue::List(_)
            | TestValue::Dict(_)
            | TestValue::Exception { .. }
            | TestValue::Unusable
            | TestValue::OverflowLength => Err(TestOperationError::type_error(
                "dict keys of this type are not supported",
            )),
        }
    }
}

impl waymark_vm_interpreter_pureset::operations::MakeDict<TestValue> for TestOperations {
    fn make_dict<I>(
        entries: I,
    ) -> Result<TestValue, waymark_vm_interpreter_pureset::operations::MakeDictError>
    where
        I: IntoIterator<Item = (String, TestValue)>,
    {
        let mut dict = BTreeMap::new();

        for (key, value) in entries {
            dict.insert(key, value);
        }

        Ok(TestValue::Dict(dict))
    }
}

impl waymark_vm_interpreter_pureset::operations::AsExceptionTypeId<TestValue> for TestOperations {
    fn as_exception_type_id(
        value: &TestValue,
    ) -> Result<&str, waymark_vm_interpreter_pureset::operations::AsExceptionTypeIdError> {
        match value {
            TestValue::Text(value) => Ok(value),
            TestValue::Int(_)
            | TestValue::Bool(_)
            | TestValue::List(_)
            | TestValue::Dict(_)
            | TestValue::Exception { .. }
            | TestValue::Unusable
            | TestValue::OverflowLength => Err(
                waymark_vm_interpreter_pureset::operations::AsExceptionTypeIdError::UnsupportedTypeIdType,
            ),
        }
    }
}

impl waymark_vm_interpreter_pureset::operations::MakeException<TestValue> for TestOperations {
    fn make_exception(type_id: String, details: TestValue) -> TestValue {
        TestValue::Exception {
            type_id,
            details: Box::new(details),
        }
    }
}

impl waymark_vm_runtime_exception::ExceptionFromIntermediate<String> for TestValue {
    fn from_intermediate_exception(
        exception: waymark_vm_runtime_exception::Exception<String>,
    ) -> waymark_vm_runtime_exception::Exception<TestValue> {
        waymark_vm_runtime_exception::Exception {
            type_id: exception.type_id,
            details: TestValue::Text(exception.details),
        }
    }
}

pub enum TestLength {
    Valid(i64),
    Overflow,
}

impl waymark_vm_interpreter_pureset::operations::Length<TestValue> for TestOperations {
    type Length = TestLength;

    type Error = TestOperationError;
    type FromLengthError = TestOperationError;

    fn length(value: &TestValue) -> Result<Self::Length, Self::Error> {
        match value {
            TestValue::Text(value) => Ok(value
                .len()
                .try_into()
                .map(TestLength::Valid)
                .unwrap_or(TestLength::Overflow)),
            TestValue::List(items) => Ok(items
                .len()
                .try_into()
                .map(TestLength::Valid)
                .unwrap_or(TestLength::Overflow)),
            TestValue::Dict(entries) => Ok(entries
                .len()
                .try_into()
                .map(TestLength::Valid)
                .unwrap_or(TestLength::Overflow)),
            TestValue::Unusable => Err(TestOperationError::type_error(
                "determining length is not supported for this value",
            )),
            TestValue::OverflowLength => Ok(TestLength::Overflow),
            TestValue::Int(_) | TestValue::Bool(_) | TestValue::Exception { .. } => {
                Err(TestOperationError::type_error(
                    "determining length is not supported for this value",
                ))
            }
        }
    }

    fn from_length(length: Self::Length) -> Result<TestValue, Self::FromLengthError> {
        match length {
            TestLength::Valid(value) => Ok(TestValue::Int(value)),
            TestLength::Overflow => Err(TestOperationError::new(
                "OverflowError",
                "length result is out of bounds",
            )),
        }
    }
}

impl waymark_vm_interpreter_pureset::operations::IndexOp<TestValue> for TestOperations {
    type Error = TestOperationError;

    fn index(object: &TestValue, index: &TestValue) -> Result<TestValue, Self::Error> {
        match (object, index) {
            (TestValue::List(items), TestValue::Int(index)) => {
                let index = normalized_index(*index, items.len()).ok_or(
                    TestOperationError::new("IndexError", "index is out of bounds"),
                )?;

                Ok(items[index].clone())
            }
            (TestValue::Dict(entries), TestValue::Text(key)) => entries
                .get(key)
                .cloned()
                .ok_or(TestOperationError::new("KeyError", "key is missing")),
            _ => Err(TestOperationError::type_error(
                "indexed access is not supported for these operands",
            )),
        }
    }
}

impl waymark_vm_interpreter_pureset::operations::DotOp<TestValue> for TestOperations {
    type Error = TestOperationError;

    fn dot(object: &TestValue, attribute: &str) -> Result<TestValue, Self::Error> {
        match object {
            TestValue::Dict(entries) => {
                entries
                    .get(attribute)
                    .cloned()
                    .ok_or(TestOperationError::new(
                        "AttributeError",
                        "attribute is missing",
                    ))
            }
            _ => Err(TestOperationError::type_error(
                "attribute access is not supported for this value",
            )),
        }
    }
}

// --- Test runtime ---

#[derive(Debug)]
pub enum RuntimeInstruction {
    Pure(PureSet<TestSpec>),
    SetUnusable {
        dst: RegisterId,
    },
    EmitRegister(RegisterId),
    /// Emit the exception pending on the frame as the terminal effect.
    ///
    /// The pureset interpreter raises by recording the exception on
    /// the frame; this test runtime has no exception handling machinery,
    /// so tests surface the pending exception for assertions with this
    /// instruction.
    EmitPendingException,
}

impl From<PureSet<TestSpec>> for RuntimeInstruction {
    fn from(value: PureSet<TestSpec>) -> Self {
        Self::Pure(value)
    }
}

#[derive(Default)]
pub struct RuntimeInterpreter {
    pure: PureSetInterpreter<TestSpec, FunctionId, StateId, TestOperations, TestValue>,
}

impl<'s, 'r, Executable>
    waymark_vm_interpreter::CaptureRuntimeView<
        's,
        waymark_vm_runtime_core::FullRuntimeView<'r, Executable, FunctionId, StateId, TestValue>,
    > for RuntimeInterpreter
{
    type Captured = ();

    fn capture_runtime_view(
        _source: &'s mut waymark_vm_runtime_core::FullRuntimeView<
            'r,
            Executable,
            FunctionId,
            StateId,
            TestValue,
        >,
    ) -> Self::Captured {
    }
}

impl waymark_vm_interpreter::Interpreter for RuntimeInterpreter {
    type RuntimeView<'r> = ();
    type Frame = Frame<FunctionId, StateId, TestValue>;
    type Instruction = RuntimeInstruction;
    type Error = Error;
    type Effect = TestValue;

    fn execute<'r>(
        &self,
        _runtime_view: Self::RuntimeView<'r>,
        mut frame: Self::Frame,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        match instruction {
            RuntimeInstruction::Pure(instruction) => self
                .pure
                .execute((), frame, instruction)
                .map(|outcome| outcome.map_effect(|effect| match effect {})),
            RuntimeInstruction::SetUnusable { dst } => {
                frame.regs.set(*dst, TestValue::Unusable);
                Ok(ExecutionOutcome::Continue(frame))
            }
            RuntimeInstruction::EmitRegister(register) => {
                let value = frame.regs[*register].clone();
                Ok(ExecutionOutcome::ExitFrameWithEffect(value))
            }
            RuntimeInstruction::EmitPendingException => {
                let exception = frame
                    .exception
                    .take()
                    .expect("a pending exception should be present on the frame");
                Ok(ExecutionOutcome::ExitFrameWithEffect(
                    TestValue::Exception {
                        type_id: exception.type_id,
                        details: Box::new(exception.details),
                    },
                ))
            }
        }
    }
}

// --- Driver ---

/// Build a single-function executable from `instrs` (one state) and run it to
/// its terminal effect.
pub fn run(regs: usize, instrs: Vec<RuntimeInstruction>) -> Result<TestValue, RunError<Error>> {
    let exec = executable(vec![function::<RuntimeInstruction>(regs, vec![instrs])]);
    let mut runtime = Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), exec)
        .expect("function 0 should exist");
    runtime.run().map(|emitted_effect| emitted_effect.effect)
}

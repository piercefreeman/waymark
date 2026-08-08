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

static_assertions::assert_impl_all!(TestValue: waymark_vm_interpreter_pureset::Value);
static_assertions::assert_impl_all!(
    TestOperations: waymark_vm_interpreter_pureset::Operations<TestValue>
);
static_assertions::assert_impl_all!(
    TestOperations: waymark_vm_interpreter_pureset::operations::Exceptions<TestValue>
);

/// A local variation marker: the interpreter is generic over any
/// operations type; these tests instantiate it with the operations
/// wrapper over this marker.
pub enum TestVariation {}

pub type TestOperations = waymark_vm_interpreter_operations::Operations<TestVariation>;

impl waymark_vm_interpreter_coreset::value::CaptureCallArgument for TestValue {
    fn capture_call_argument(&self) -> Self {
        self.clone()
    }
}

impl waymark_vm_interpreter_pureset::value::CaptureCopy for TestValue {
    fn capture_copy(&self) -> Self {
        self.clone()
    }
}

impl waymark_vm_interpreter_pureset::value::LoadConst<&TestConstValue> for TestValue {
    fn load_const(const_value: &TestConstValue) -> Self {
        match const_value {
            TestConstValue::Int(value) => Self::Int(*value),
            TestConstValue::Text(value) => Self::Text((*value).to_owned()),
            TestConstValue::OverflowLength => Self::OverflowLength,
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

impl waymark_vm_interpreter_pureset::value::AsScalar for TestValue {
    type Scalar = Self;

    fn as_scalar(
        &self,
    ) -> Result<&Self::Scalar, waymark_vm_interpreter_pureset::value::AsScalarError> {
        match self {
            Self::Unusable => Err(waymark_vm_interpreter_pureset::value::AsScalarError::NotAScalar),
            _ => Ok(self),
        }
    }

    fn from_scalar(scalar: Self::Scalar) -> Self {
        scalar
    }
}

impl waymark_vm_interpreter_pureset::value::BinaryOps for TestValue {
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

impl waymark_vm_interpreter_pureset::value::UnaryOps for TestValue {
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

impl waymark_vm_interpreter_pureset::value::MakeList for TestValue {
    fn make_list<I>(items: I) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeListError>
    where
        I: IntoIterator<Item = Self>,
    {
        Ok(Self::List(items.into_iter().collect()))
    }
}

impl waymark_vm_interpreter_pureset::value::ListAppend for TestValue {
    fn list_append(
        list: &Self,
        item: Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::ListAppendError> {
        let Self::List(existing) = list else {
            return Err(waymark_vm_interpreter_pureset::value::ListAppendError::NotListable);
        };
        let mut grown = existing.clone();
        grown.push(item);
        Ok(Self::List(grown))
    }
}

impl waymark_vm_interpreter_pureset::value::AsDictKey for TestValue {
    fn as_dict_key(&self) -> Result<&str, waymark_vm_interpreter_pureset::value::AsDictKeyError> {
        match self {
            TestValue::Text(value) => Ok(value),
            TestValue::Int(_)
            | TestValue::Bool(_)
            | TestValue::List(_)
            | TestValue::Dict(_)
            | TestValue::Exception { .. }
            | TestValue::Unusable
            | TestValue::OverflowLength => {
                Err(waymark_vm_interpreter_pureset::value::AsDictKeyError::UnsupportedKeyType)
            }
        }
    }
}

impl waymark_vm_interpreter_pureset::value::MakeDict for TestValue {
    fn make_dict<I>(
        entries: I,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeDictError>
    where
        I: IntoIterator<Item = (String, Self)>,
    {
        let mut dict = BTreeMap::new();

        for (key, value) in entries {
            dict.insert(key, value);
        }

        Ok(Self::Dict(dict))
    }
}

impl waymark_vm_interpreter_pureset::value::AsExceptionTypeId for TestValue {
    fn as_exception_type_id(
        &self,
    ) -> Result<&str, waymark_vm_interpreter_pureset::value::AsExceptionTypeIdError> {
        match self {
            TestValue::Text(value) => Ok(value),
            TestValue::Int(_)
            | TestValue::Bool(_)
            | TestValue::List(_)
            | TestValue::Dict(_)
            | TestValue::Exception { .. }
            | TestValue::Unusable
            | TestValue::OverflowLength => Err(
                waymark_vm_interpreter_pureset::value::AsExceptionTypeIdError::UnsupportedTypeIdType,
            ),
        }
    }
}

impl waymark_vm_interpreter_pureset::value::MakeException for TestValue {
    fn make_exception(type_id: String, details: Self) -> Self {
        Self::Exception {
            type_id,
            details: Box::new(details),
        }
    }
}

impl waymark_vm_runtime_exception::ExceptionFromIntermediate<String> for TestValue {
    fn from_intermediate_exception(
        exception: waymark_vm_runtime_exception::Exception<String>,
    ) -> waymark_vm_runtime_exception::Exception<Self::RootValue> {
        waymark_vm_runtime_exception::Exception {
            type_id: exception.type_id,
            details: Self::Text(exception.details),
        }
    }
}

pub enum TestLength {
    Valid(i64),
    Overflow,
}

impl waymark_vm_interpreter_pureset::value::Length for TestValue {
    type Length = TestLength;

    fn length(&self) -> Result<Self::Length, waymark_vm_interpreter_pureset::value::LengthError> {
        match self {
            Self::Text(value) => Ok(value
                .len()
                .try_into()
                .map(TestLength::Valid)
                .unwrap_or(TestLength::Overflow)),
            Self::List(items) => Ok(items
                .len()
                .try_into()
                .map(TestLength::Valid)
                .unwrap_or(TestLength::Overflow)),
            Self::Dict(entries) => Ok(entries
                .len()
                .try_into()
                .map(TestLength::Valid)
                .unwrap_or(TestLength::Overflow)),
            Self::Unusable => {
                Err(waymark_vm_interpreter_pureset::value::LengthError::UnsupportedValue)
            }
            Self::OverflowLength => Ok(TestLength::Overflow),
            Self::Int(_) | Self::Bool(_) | Self::Exception { .. } => {
                Err(waymark_vm_interpreter_pureset::value::LengthError::UnsupportedValue)
            }
        }
    }

    fn from_length(
        length: Self::Length,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::FromLengthError> {
        match length {
            TestLength::Valid(value) => Ok(Self::Int(value)),
            TestLength::Overflow => {
                Err(waymark_vm_interpreter_pureset::value::FromLengthError::ResultOutOfBounds)
            }
        }
    }
}

impl waymark_vm_interpreter_pureset::value::IndexOp for TestValue {
    fn index(
        object: &Self,
        index: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::IndexOperationError> {
        match (object, index) {
            (Self::List(items), Self::Int(index)) => {
                let index = normalized_index(*index, items.len()).ok_or(
                    waymark_vm_interpreter_pureset::value::IndexOperationError::IndexOutOfBounds,
                )?;

                Ok(items[index].clone())
            }
            (Self::Dict(entries), Self::Text(key)) => entries
                .get(key)
                .cloned()
                .ok_or(waymark_vm_interpreter_pureset::value::IndexOperationError::MissingKey),
            _ => Err(
                waymark_vm_interpreter_pureset::value::IndexOperationError::UnsupportedOperation,
            ),
        }
    }
}

impl waymark_vm_interpreter_pureset::value::DotOp for TestValue {
    fn dot(
        object: &Self,
        attribute: &str,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::DotOperationError> {
        match object {
            Self::Dict(entries) => entries
                .get(attribute)
                .cloned()
                .ok_or(waymark_vm_interpreter_pureset::value::DotOperationError::MissingAttribute),
            _ => {
                Err(waymark_vm_interpreter_pureset::value::DotOperationError::UnsupportedOperation)
            }
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

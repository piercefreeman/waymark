use std::collections::BTreeMap;

use waymark_vm_instructions_pureset::{BinaryOpKind, PureSet, UnaryOpKind};
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_pureset::{BinaryOperandPosition, Error, PureSetInterpreter};
use waymark_vm_runtime::{RunError, Runtime};
use waymark_vm_runtime_core::{CaptureRuntimeView, Frame, RegisterId};
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};

#[derive(Debug)]
struct TestSpec;

impl waymark_vm_instructions_pureset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type ConstValue = TestConstValue;
}

#[derive(Debug, Clone)]
enum TestConstValue {
    Int(i64),
    Text(&'static str),
    OverflowLength,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestValue {
    Int(i64),
    Bool(bool),
    Text(&'static str),
    List(Vec<TestValue>),
    Dict(BTreeMap<String, TestValue>),
    // Test-only sentinel for values that should be rejected by pureset operations
    // without coupling these tests to promise-specific runtime behavior.
    Unusable,
    OverflowLength,
}

impl waymark_vm_runtime_value::RootValueAccess for TestValue {
    type RootValue = TestValue;
}

static_assertions::assert_impl_all!(TestValue: waymark_vm_interpreter_pureset::Value);

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
            TestConstValue::Text(value) => Self::Text(value),
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

impl waymark_vm_interpreter_pureset::value::AsDictKey for TestValue {
    fn as_dict_key(&self) -> Result<&str, waymark_vm_interpreter_pureset::value::AsDictKeyError> {
        match self {
            TestValue::Text(value) => Ok(value),
            TestValue::Int(_)
            | TestValue::Bool(_)
            | TestValue::List(_)
            | TestValue::Dict(_)
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

enum TestLength {
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
            Self::Int(_) | Self::Bool(_) => {
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
                .get(*key)
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

#[derive(Debug)]
enum RuntimeInstruction {
    Pure(PureSet<TestSpec>),
    SetUnusable { dst: RegisterId },
    EmitRegister(RegisterId),
}

impl From<PureSet<TestSpec>> for RuntimeInstruction {
    fn from(value: PureSet<TestSpec>) -> Self {
        Self::Pure(value)
    }
}

#[derive(Default)]
struct RuntimeInterpreter {
    pure: PureSetInterpreter<TestSpec, FunctionId, StateId, TestValue>,
}

impl<Executable> CaptureRuntimeView<Executable, FunctionId, StateId, TestValue>
    for RuntimeInterpreter
{
    type RuntimeView<'v>
        = ()
    where
        Executable: 'v,
        FunctionId: 'v,
        StateId: 'v,
        TestValue: 'v;

    fn capture_runtime_view<'r>(
        _view: waymark_vm_runtime_core::FullRuntimeView<
            'r,
            Executable,
            FunctionId,
            StateId,
            TestValue,
        >,
    ) -> Self::RuntimeView<'r> {
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
        }
    }
}

#[test]
fn runtime_executes_load_const_and_add_to_a_terminal_effect() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        3,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(2),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(3),
            }
            .into(),
            PureSet::Binary {
                kind: waymark_vm_instructions_pureset::BinaryOpKind::Add,
                op: waymark_vm_instructions_pureset::BinaryOp {
                    dst: RegisterId(2),
                    a: RegisterId(0),
                    b: RegisterId(1),
                },
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert_eq!(
        runtime
            .run()
            .expect("runtime should emit the computed pure result"),
        TestValue::Int(5)
    );
}

#[test]
fn runtime_executes_copy_between_registers() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        2,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(9),
            }
            .into(),
            PureSet::Copy {
                dst: RegisterId(1),
                src: RegisterId(0),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert_eq!(
        runtime
            .run()
            .expect("runtime should emit the copied pure result"),
        TestValue::Int(9)
    );
}

#[test]
fn runtime_executes_length_to_a_terminal_effect() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        4,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(2),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Text("hello"),
            }
            .into(),
            PureSet::MakeList {
                dst: RegisterId(2),
                items: vec![RegisterId(0), RegisterId(1)],
            }
            .into(),
            PureSet::Length {
                dst: RegisterId(3),
                src: RegisterId(2),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(3)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert_eq!(
        runtime.run().expect("runtime should emit the list length"),
        TestValue::Int(2)
    );
}

#[test]
fn runtime_surfaces_length_errors_from_the_pureset_interpreter() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        2,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(9),
            }
            .into(),
            PureSet::Length {
                dst: RegisterId(1),
                src: RegisterId(0),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::Length(waymark_vm_interpreter_pureset::value::LengthError::UnsupportedValue)
        )))
    ));
}

#[test]
fn runtime_surfaces_from_length_errors_from_the_pureset_interpreter() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        2,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::OverflowLength,
            }
            .into(),
            PureSet::Length {
                dst: RegisterId(1),
                src: RegisterId(0),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::FromLength(
                waymark_vm_interpreter_pureset::value::FromLengthError::ResultOutOfBounds
            )
        )))
    ));
}

#[test]
fn runtime_surfaces_add_errors_from_the_pureset_interpreter() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        2,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Text("left"),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(3),
            }
            .into(),
            PureSet::Binary {
                kind: waymark_vm_instructions_pureset::BinaryOpKind::Add,
                op: waymark_vm_instructions_pureset::BinaryOp {
                    dst: RegisterId(0),
                    a: RegisterId(0),
                    b: RegisterId(1),
                },
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(0)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::BinaryOperation {
                operation: BinaryOpKind::Add,
                source:
                    waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                        operation: BinaryOpKind::Add,
                    },
            }
        )))
    ));
}

#[test]
fn runtime_surfaces_unusable_add_operand_errors_from_unusable_values() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        2,
        vec![vec![
            RuntimeInstruction::SetUnusable { dst: RegisterId(0) },
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(3),
            }
            .into(),
            PureSet::Binary {
                kind: waymark_vm_instructions_pureset::BinaryOpKind::Add,
                op: waymark_vm_instructions_pureset::BinaryOp {
                    dst: RegisterId(0),
                    a: RegisterId(0),
                    b: RegisterId(1),
                },
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(0)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::UnusableBinaryOperand {
                operation: BinaryOpKind::Add,
                operand_pos: BinaryOperandPosition::First,
                source: waymark_vm_interpreter_pureset::value::AsScalarError::NotAScalar,
            }
        )))
    ));
}

#[test]
fn runtime_surfaces_length_errors_from_unusable_values() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        2,
        vec![vec![
            RuntimeInstruction::SetUnusable { dst: RegisterId(0) },
            PureSet::Length {
                dst: RegisterId(1),
                src: RegisterId(0),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::Length(waymark_vm_interpreter_pureset::value::LengthError::UnsupportedValue)
        )))
    ));
}

#[test]
fn runtime_executes_unary_not_to_a_terminal_effect() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        2,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(0),
            }
            .into(),
            PureSet::Unary {
                kind: waymark_vm_instructions_pureset::UnaryOpKind::Not,
                op: waymark_vm_instructions_pureset::UnaryOp {
                    dst: RegisterId(1),
                    src: RegisterId(0),
                },
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert_eq!(
        runtime
            .run()
            .expect("runtime should emit the unary-not result"),
        TestValue::Bool(true)
    );
}

#[test]
fn runtime_converts_non_numeric_constants_through_the_spec_type() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        1,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Text("hello"),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(0)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert_eq!(
        runtime
            .run()
            .expect("runtime should emit the converted constant"),
        TestValue::Text("hello")
    );
}

#[test]
fn runtime_executes_make_list_to_a_terminal_effect() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        3,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(2),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Text("hello"),
            }
            .into(),
            PureSet::MakeList {
                dst: RegisterId(2),
                items: vec![RegisterId(0), RegisterId(1)],
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert_eq!(
        runtime
            .run()
            .expect("runtime should emit the constructed list"),
        TestValue::List(vec![TestValue::Int(2), TestValue::Text("hello")])
    );
}

#[test]
fn runtime_executes_make_dict_to_a_terminal_effect() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        4,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Text("key"),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Text("hello"),
            }
            .into(),
            PureSet::MakeDict {
                dst: RegisterId(2),
                entries: vec![waymark_vm_instructions_pureset::DictEntry {
                    key: RegisterId(0),
                    value: RegisterId(1),
                }],
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert_eq!(
        runtime
            .run()
            .expect("runtime should emit the constructed dict"),
        TestValue::Dict(BTreeMap::from([(
            "key".to_owned(),
            TestValue::Text("hello"),
        )]))
    );
}

#[test]
fn runtime_executes_index_to_a_terminal_effect() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        4,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(2),
            }
            .into(),
            PureSet::MakeList {
                dst: RegisterId(1),
                items: vec![RegisterId(0)],
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(2),
                value: TestConstValue::Int(-1),
            }
            .into(),
            PureSet::Index {
                dst: RegisterId(3),
                object: RegisterId(1),
                index: RegisterId(2),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(3)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert_eq!(
        runtime
            .run()
            .expect("runtime should emit the indexed result"),
        TestValue::Int(2)
    );
}

#[test]
fn runtime_executes_dot_to_a_terminal_effect() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        4,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Text("field"),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(9),
            }
            .into(),
            PureSet::MakeDict {
                dst: RegisterId(2),
                entries: vec![waymark_vm_instructions_pureset::DictEntry {
                    key: RegisterId(0),
                    value: RegisterId(1),
                }],
            }
            .into(),
            PureSet::Dot {
                dst: RegisterId(3),
                object: RegisterId(2),
                attribute: "field".to_owned(),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(3)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert_eq!(
        runtime
            .run()
            .expect("runtime should emit the dotted result"),
        TestValue::Int(9)
    );
}

#[test]
fn runtime_surfaces_make_dict_key_type_errors_from_the_pureset_interpreter() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        4,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(2),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Text("hello"),
            }
            .into(),
            PureSet::MakeDict {
                dst: RegisterId(2),
                entries: vec![waymark_vm_instructions_pureset::DictEntry {
                    key: RegisterId(0),
                    value: RegisterId(1),
                }],
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::UnusableDictKey {
                entry_pos,
                source: waymark_vm_interpreter_pureset::value::AsDictKeyError::UnsupportedKeyType,
            }
        ))) if entry_pos == 0
    ));
}

#[test]
fn runtime_surfaces_index_errors_from_unusable_values() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        4,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(2),
            }
            .into(),
            PureSet::MakeList {
                dst: RegisterId(1),
                items: vec![RegisterId(0)],
            }
            .into(),
            RuntimeInstruction::SetUnusable { dst: RegisterId(2) },
            PureSet::Index {
                dst: RegisterId(3),
                object: RegisterId(1),
                index: RegisterId(2),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(3)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::IndexOperation {
                source:
                    waymark_vm_interpreter_pureset::value::IndexOperationError::UnsupportedOperation,
            }
        )))
    ));
}

#[test]
fn runtime_surfaces_missing_dot_attribute_errors_from_the_pureset_interpreter() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        4,
        vec![vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Text("present"),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(9),
            }
            .into(),
            PureSet::MakeDict {
                dst: RegisterId(2),
                entries: vec![waymark_vm_instructions_pureset::DictEntry {
                    key: RegisterId(0),
                    value: RegisterId(1),
                }],
            }
            .into(),
            PureSet::Dot {
                dst: RegisterId(3),
                object: RegisterId(2),
                attribute: "missing".to_owned(),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(3)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::DotOperation {
                attribute,
                source:
                    waymark_vm_interpreter_pureset::value::DotOperationError::MissingAttribute,
            }
        ))) if attribute == "missing"
    ));
}

#[test]
fn runtime_copies_unusable_make_list_items_to_a_terminal_effect() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        2,
        vec![vec![
            RuntimeInstruction::SetUnusable { dst: RegisterId(0) },
            PureSet::MakeList {
                dst: RegisterId(1),
                items: vec![RegisterId(0)],
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert_eq!(
        runtime
            .run()
            .expect("runtime should preserve unusable list items"),
        TestValue::List(vec![TestValue::Unusable])
    );
}

#[test]
fn runtime_surfaces_unusable_make_dict_key_errors_from_unusable_values() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        3,
        vec![vec![
            RuntimeInstruction::SetUnusable { dst: RegisterId(0) },
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(3),
            }
            .into(),
            PureSet::MakeDict {
                dst: RegisterId(2),
                entries: vec![waymark_vm_instructions_pureset::DictEntry {
                    key: RegisterId(0),
                    value: RegisterId(1),
                }],
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ]],
    )]);

    let mut runtime =
        Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), executable)
            .expect("function 0 should exist");

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::UnusableDictKey {
                entry_pos,
                source: waymark_vm_interpreter_pureset::value::AsDictKeyError::UnsupportedKeyType,
            }
        ))) if entry_pos == 0
    ));
}

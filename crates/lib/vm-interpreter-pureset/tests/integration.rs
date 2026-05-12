use waymark_vm_instructions_pureset::PureSet;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_pureset::{BinaryOperandPosition, Error, PureSetInterpreter};
use waymark_vm_runtime::{RunError, Runtime};
use waymark_vm_runtime_core::{CaptureRuntimeView, Frame, Promise, PromiseStateId, RegisterId};
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestValue {
    Int(i64),
    Text(&'static str),
    List(Vec<TestValue>),
}

impl From<TestConstValue> for TestValue {
    fn from(value: TestConstValue) -> Self {
        match value {
            TestConstValue::Int(value) => Self::Int(value),
            TestConstValue::Text(value) => Self::Text(value),
        }
    }
}

impl waymark_vm_interpreter_pureset::value::Add for TestValue {
    fn add(a: &Self, b: &Self) -> Result<Self, waymark_vm_interpreter_pureset::value::AddError> {
        match (a, b) {
            (Self::Int(a), Self::Int(b)) => Ok(Self::Int(a + b)),
            _ => Err(waymark_vm_interpreter_pureset::value::AddError::NotAddable),
        }
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

#[derive(Debug)]
enum RuntimeInstruction {
    Pure(PureSet<TestSpec>),
    SetPending {
        dst: RegisterId,
        promise_state_id: PromiseStateId,
    },
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
    type Frame = Frame<FunctionId, StateId, Promise<TestValue>>;
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
            RuntimeInstruction::SetPending {
                dst,
                promise_state_id,
            } => {
                frame.regs.set(*dst, Promise::Pending(*promise_state_id));
                Ok(ExecutionOutcome::Continue(frame))
            }
            RuntimeInstruction::EmitRegister(register) => {
                let value = frame.regs[*register].clone().require_resolved().unwrap();
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
            PureSet::Add {
                dst: RegisterId(2),
                a: RegisterId(0),
                b: RegisterId(1),
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
            PureSet::Add {
                dst: RegisterId(0),
                a: RegisterId(0),
                b: RegisterId(1),
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
            Error::Add(waymark_vm_interpreter_pureset::value::AddError::NotAddable)
        )))
    ));
}

#[test]
fn runtime_surfaces_unresolved_add_operand_errors_from_the_pureset_interpreter() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        2,
        vec![vec![
            RuntimeInstruction::SetPending {
                dst: RegisterId(0),
                promise_state_id: PromiseStateId(7),
            },
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(3),
            }
            .into(),
            PureSet::Add {
                dst: RegisterId(0),
                a: RegisterId(0),
                b: RegisterId(1),
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
            Error::UnresolvedAddOperand {
                operand_pos: BinaryOperandPosition::First,
                source: waymark_vm_runtime_core::UnresolvedPromiseError { promise_state_id },
            }
        ))) if promise_state_id == PromiseStateId(7)
    ));
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
fn runtime_surfaces_unresolved_make_list_item_errors_from_the_pureset_interpreter() {
    let executable = executable(vec![function::<RuntimeInstruction>(
        2,
        vec![vec![
            RuntimeInstruction::SetPending {
                dst: RegisterId(0),
                promise_state_id: PromiseStateId(7),
            },
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

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::UnresolvedListItem {
                item_pos,
                source: waymark_vm_runtime_core::UnresolvedPromiseError { promise_state_id },
            }
        ))) if item_pos == 0 && promise_state_id == PromiseStateId(7)
    ));
}

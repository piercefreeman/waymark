use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_fullset::FullSet;
use waymark_vm_instructions_pureset::PureSet;
use waymark_vm_interpreter_fullset::{Effect, FullSetInterpreter};
use waymark_vm_runtime::{CallSpec, Runtime};
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};

type Instruction = FullSet<TestSpec>;

#[derive(Debug)]
struct TestSpec;

impl waymark_vm_instructions_coreset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type FunctionId = FunctionId;
    type ExtCallId = TestExtCallId;
    type StateId = StateId;
}

impl waymark_vm_instructions_pureset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type ConstValue = TestConstValue;
}

#[derive(Debug, Clone)]
enum TestConstValue {
    Int(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestExtCallId(usize);

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestValue {
    Int(i64),
    Bool(bool),
    List(Vec<TestValue>),
}

fn is_truthy(value: &TestValue) -> bool {
    match value {
        TestValue::Int(value) => *value != 0,
        TestValue::Bool(value) => *value,
        TestValue::List(items) => !items.is_empty(),
    }
}

impl waymark_vm_interpreter_coreset::value::ShouldJump for TestValue {
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        Ok(is_truthy(self))
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
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Add,
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
                    operation: waymark_vm_interpreter_pureset::value::UnaryOperationKind::Neg,
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

impl From<TestConstValue> for TestValue {
    fn from(value: TestConstValue) -> Self {
        match value {
            TestConstValue::Int(value) => Self::Int(value),
        }
    }
}

#[test]
fn runtime_executes_pure_and_core_instructions_to_completion() {
    let executable = executable(vec![function::<Instruction>(
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
            CoreSet::Return { src: RegisterId(2) }.into(),
        ]],
    )]);

    let mut runtime = Runtime::with_conventional_entrypoint(
        FullSetInterpreter::<TestSpec, _, TestValue>::default(),
        executable,
    )
    .expect("function 0 should exist");

    let effect = runtime
        .run()
        .expect("mixed fullset program should complete successfully");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(value, TestValue::Int(5));
        }
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::ExtCall { .. }) => {
            panic!("program should not emit an extcall")
        }
        Effect::PureSet(effect) => match effect {},
    }
}

#[test]
fn runtime_resumes_extcalls_and_finishes_with_pure_work() {
    let executable = executable(vec![function::<Instruction>(
        4,
        vec![
            vec![
                CoreSet::ExtCall {
                    dst: RegisterId(1),
                    extcall_id: TestExtCallId(7),
                    args: vec![RegisterId(0)],
                    resume: StateId(1),
                }
                .into(),
            ],
            vec![
                CoreSet::Await {
                    dst: RegisterId(2),
                    src: RegisterId(1),
                    resume: StateId(2),
                }
                .into(),
            ],
            vec![
                PureSet::LoadConst {
                    dst: RegisterId(3),
                    value: TestConstValue::Int(1),
                }
                .into(),
                PureSet::Add {
                    dst: RegisterId(3),
                    a: RegisterId(2),
                    b: RegisterId(3),
                }
                .into(),
                CoreSet::Return { src: RegisterId(3) }.into(),
            ],
        ],
    )]);

    let mut runtime = Runtime::with_custom_entrypoint(
        FullSetInterpreter::<TestSpec, _, TestValue>::default(),
        executable,
        CallSpec {
            func: FunctionId(0),
            args: vec![TestValue::Int(41)],
        },
    )
    .expect("function 0 should exist");

    let effect = runtime.run().expect("first run should emit the extcall");

    let promise_state_id = match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::ExtCall {
            promise_state_id,
            extcall_id,
            args,
        }) => {
            assert_eq!(extcall_id, TestExtCallId(7));
            assert_eq!(args, vec![TestValue::Int(41)]);
            promise_state_id
        }
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(_)) => {
            panic!("program should suspend on the extcall before completion")
        }
        Effect::PureSet(effect) => match effect {},
    };

    runtime
        .resolve_promise(promise_state_id, TestValue::Int(41))
        .expect("extcall promise should resolve cleanly");

    let effect = runtime
        .run()
        .expect("second run should finish after resuming the extcall");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(value, TestValue::Int(42));
        }
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::ExtCall { .. }) => {
            panic!("resolved extcall should not emit another extcall")
        }
        Effect::PureSet(effect) => match effect {},
    }
}

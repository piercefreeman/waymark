use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_instructions_fullset::FullSet;
use waymark_vm_instructions_pureset::{BinaryOpKind, PureSet, UnaryOpKind};
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

#[derive(Debug, Clone)]
enum TestConstValue {
    Int(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestActionRef(usize);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
enum TestSleepDurationError {
    #[error("the value cannot be used as a sleep duration")]
    UnsupportedValue,

    #[error("sleep duration must be non-zero")]
    Zero,

    #[error("sleep duration cannot be negative")]
    Negative,
}

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

impl waymark_vm_interpreter_extcallset::value::SleepDuration for TestValue {
    type Error = TestSleepDurationError;

    fn to_sleep_duration(&self) -> Result<NonZeroDuration, Self::Error> {
        match self {
            Self::Int(value) => {
                let seconds: u64 = (*value).try_into().map_err(|_| Self::Error::Negative)?;
                NonZeroDuration::from_secs(seconds).ok_or(Self::Error::Zero)
            }
            Self::Bool(_) | Self::List(_) => Err(Self::Error::UnsupportedValue),
        }
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
            PureSet::Binary {
                kind: waymark_vm_instructions_pureset::BinaryOpKind::Add,
                op: waymark_vm_instructions_pureset::BinaryOp {
                    dst: RegisterId(2),
                    a: RegisterId(0),
                    b: RegisterId(1),
                },
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
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall { .. }) => {
            panic!("program should not emit an action call")
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::Sleep { .. }) => {
            panic!("program should not emit a sleep effect")
        }
        Effect::PureSet(effect) => match effect {},
    }
}

#[test]
fn runtime_resumes_sleep_effects_and_finishes_with_pure_work() {
    let executable = executable(vec![function::<Instruction>(
        4,
        vec![
            vec![
                PureSet::LoadConst {
                    dst: RegisterId(0),
                    value: TestConstValue::Int(2),
                }
                .into(),
                ExtCallSet::Sleep {
                    dst: RegisterId(1),
                    duration: RegisterId(0),
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
                    value: TestConstValue::Int(7),
                }
                .into(),
                CoreSet::Return { src: RegisterId(3) }.into(),
            ],
        ],
    )]);

    let mut runtime = Runtime::with_conventional_entrypoint(
        FullSetInterpreter::<TestSpec, _, TestValue>::default(),
        executable,
    )
    .expect("function 0 should exist");

    let effect = runtime
        .run()
        .expect("first run should emit the sleep effect");

    let promise_state_id = match effect {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::Sleep {
            promise_state_id,
            duration,
        }) => {
            assert_eq!(duration, NonZeroDuration::from_secs(2).unwrap());
            promise_state_id
        }
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(_)) => {
            panic!("program should suspend on sleep before completion")
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall { .. }) => {
            panic!("program should emit a sleep effect, not an action call")
        }
        Effect::PureSet(effect) => match effect {},
    };

    runtime
        .resolve_promise(promise_state_id, TestValue::Int(0))
        .expect("sleep promise should resolve cleanly");

    let effect = runtime
        .run()
        .expect("second run should finish after resuming sleep");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(value, TestValue::Int(7));
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall { .. }) => {
            panic!("resolved sleep should not emit an action call")
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::Sleep { .. }) => {
            panic!("resolved sleep should not emit another sleep effect")
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
                ExtCallSet::ActionCall {
                    dst: RegisterId(1),
                    action_ref: TestActionRef(7),
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
                PureSet::Binary {
                    kind: waymark_vm_instructions_pureset::BinaryOpKind::Add,
                    op: waymark_vm_instructions_pureset::BinaryOp {
                        dst: RegisterId(3),
                        a: RegisterId(2),
                        b: RegisterId(3),
                    },
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

    let effect = runtime
        .run()
        .expect("first run should emit the action call");

    let promise_state_id = match effect {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            action_ref,
            args,
        }) => {
            assert_eq!(action_ref, TestActionRef(7));
            assert_eq!(args, vec![TestValue::Int(41)]);
            promise_state_id
        }
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(_)) => {
            panic!("program should suspend on the action call before completion")
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::Sleep { .. }) => {
            panic!("program should suspend on an extcall, not a sleep effect")
        }
        Effect::PureSet(effect) => match effect {},
    };

    runtime
        .resolve_promise(promise_state_id, TestValue::Int(41))
        .expect("action call promise should resolve cleanly");

    let effect = runtime
        .run()
        .expect("second run should finish after resuming the action call");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(value, TestValue::Int(42));
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall { .. }) => {
            panic!("resolved action call should not emit another action call")
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::Sleep { .. }) => {
            panic!("resolved extcall should not emit a sleep effect")
        }
        Effect::PureSet(effect) => match effect {},
    }
}

use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_interpreter_coreset::{CoreSetInterpreter, Effect};
use waymark_vm_runtime::{CallSpec, Runtime};
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_promise_value::PromiseValue;
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};

#[derive(Debug)]
struct TestSpec;

impl waymark_vm_instructions_coreset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type FunctionId = FunctionId;
    type StateId = StateId;
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestReadyValue {
    Int(i64),
}

type TestValue = PromiseValue<TestReadyValue>;

impl waymark_vm_runtime_value::RootValueAccess for TestReadyValue {
    type RootValue = TestValue;
}

static_assertions::assert_impl_all!(TestValue: waymark_vm_interpreter_coreset::Value);

impl waymark_vm_interpreter_coreset::value::CaptureCallArgument for TestReadyValue {
    fn capture_call_argument(&self) -> Self {
        self.clone()
    }
}

impl waymark_vm_interpreter_coreset::value::ShouldJump for TestReadyValue {
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        let Self::Int(value) = self;
        Ok(*value != 0)
    }
}

#[test]
fn runtime_executes_call_await_and_return_to_completion() {
    let executable = executable(vec![
        function(
            2,
            vec![
                vec![
                    CoreSet::Call {
                        dst: RegisterId(1),
                        function_id: FunctionId(1),
                        args: vec![RegisterId(0)],
                    },
                    CoreSet::Await {
                        dst: RegisterId(0),
                        src: RegisterId(1),
                        resume: StateId(1),
                    },
                ],
                vec![CoreSet::Return { src: RegisterId(0) }],
            ],
        ),
        function(1, vec![vec![CoreSet::Return { src: RegisterId(0) }]]),
    ]);

    let mut runtime = Runtime::with_custom_entrypoint(
        CoreSetInterpreter::<TestSpec, _, TestValue>::default(),
        executable,
        CallSpec {
            func: FunctionId(0),
            args: vec![TestValue::Ready(TestReadyValue::Int(7))],
        },
    )
    .expect("function 0 should exist");

    let effect = runtime
        .run()
        .expect("runtime should complete after the nested call resolves");

    let Effect::Complete(value) = effect;
    assert_eq!(value, TestReadyValue::Int(7));
}

#[test]
fn runtime_follows_jump_and_jump_if_before_returning() {
    let executable = executable(vec![function(
        2,
        vec![
            vec![CoreSet::JumpIf {
                target_state: StateId(1),
                cond: RegisterId(0),
            }],
            vec![CoreSet::Jump {
                target_state: StateId(2),
            }],
            vec![CoreSet::Return { src: RegisterId(1) }],
        ],
    )]);

    let mut runtime = Runtime::with_custom_entrypoint(
        CoreSetInterpreter::<TestSpec, _, TestValue>::default(),
        executable,
        CallSpec {
            func: FunctionId(0),
            args: vec![
                TestValue::Ready(TestReadyValue::Int(1)),
                TestValue::Ready(TestReadyValue::Int(9)),
            ],
        },
    )
    .expect("function 0 should exist");

    let effect = runtime
        .run()
        .expect("runtime should follow the branch states to completion");

    let Effect::Complete(value) = effect;
    assert_eq!(value, TestReadyValue::Int(9));
}

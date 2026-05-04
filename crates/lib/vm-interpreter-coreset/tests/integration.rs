use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_interpreter_coreset::{CoreSetInterpreter, Effect};
use waymark_vm_runtime::{CallSpec, Runtime};
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};

#[derive(Debug)]
struct TestSpec;

impl waymark_vm_instructions_coreset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type FunctionId = FunctionId;
    type ExtCallId = TestExtCallId;
    type StateId = StateId;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestExtCallId(usize);

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestValue {
    Int(i64),
}

impl waymark_vm_interpreter_coreset::value::ShouldJump for TestValue {
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
            args: vec![TestValue::Int(7)],
        },
    )
    .expect("function 0 should exist");

    let effect = runtime
        .run()
        .expect("runtime should complete after the nested call resolves");

    match effect {
        Effect::Complete(value) => assert_eq!(value, TestValue::Int(7)),
        Effect::ExtCall { .. } => panic!("program should not emit an extcall"),
    }
}

#[test]
fn runtime_emits_extcall_and_completes_after_resolution() {
    let executable = executable(vec![function(
        3,
        vec![
            vec![CoreSet::ExtCall {
                dst: RegisterId(1),
                extcall_id: TestExtCallId(5),
                args: vec![RegisterId(0)],
                resume: StateId(1),
            }],
            vec![CoreSet::Await {
                dst: RegisterId(2),
                src: RegisterId(1),
                resume: StateId(2),
            }],
            vec![CoreSet::Return { src: RegisterId(2) }],
        ],
    )]);

    let mut runtime = Runtime::with_custom_entrypoint(
        CoreSetInterpreter::<TestSpec, _, TestValue>::default(),
        executable,
        CallSpec {
            func: FunctionId(0),
            args: vec![TestValue::Int(13)],
        },
    )
    .expect("function 0 should exist");

    let promise_state_id = match runtime.run().expect("first run should emit the extcall") {
        Effect::ExtCall {
            promise_state_id,
            extcall_id,
            args,
        } => {
            assert_eq!(extcall_id, TestExtCallId(5));
            assert_eq!(args, vec![TestValue::Int(13)]);
            promise_state_id
        }
        Effect::Complete(_) => panic!("runtime should suspend on the extcall first"),
    };

    runtime
        .resolve_promise(promise_state_id, TestValue::Int(41))
        .expect("extcall promise should resolve cleanly");

    let effect = runtime
        .run()
        .expect("runtime should complete after the promise resolves");

    match effect {
        Effect::Complete(value) => assert_eq!(value, TestValue::Int(41)),
        Effect::ExtCall { .. } => panic!("resolved extcall should not emit another extcall"),
    }
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
            args: vec![TestValue::Int(1), TestValue::Int(9)],
        },
    )
    .expect("function 0 should exist");

    let effect = runtime
        .run()
        .expect("runtime should follow the branch states to completion");

    match effect {
        Effect::Complete(value) => assert_eq!(value, TestValue::Int(9)),
        Effect::ExtCall { .. } => panic!("branching program should not emit an extcall"),
    }
}

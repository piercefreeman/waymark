//! Integration test for completing extcall action calls with exception errors.

use waymark_vm_bytecode::Executable;
use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_excset::ExcSet;
use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_runtime::{CallSpec, Runtime};
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};
use waymark_vm_value_python::exception::classes;
use waymark_vm_value_python::{Exception, RaisedException, ReadyValue, Value};

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
    type ActionRef = usize;
}

impl waymark_vm_instructions_excset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type StateId = StateId;
    type ConstException = TestConstException;
    type ConstExceptionPattern = TestConstExceptionPattern;
}

/// The bytecode's handler pattern: the class names a handler lists.
#[derive(Debug)]
struct TestConstExceptionPattern(Vec<String>);

impl From<&TestConstExceptionPattern> for waymark_vm_value_python::raised_exception::Pattern {
    fn from(pattern: &TestConstExceptionPattern) -> Self {
        Self::Classes(pattern.0.clone())
    }
}

/// The bytecode's exception: this test raises none, but the spec names one.
#[derive(Debug)]
struct TestConstException;

impl From<&TestConstException> for RaisedException {
    fn from(_exception: &TestConstException) -> Self {
        classes::VALUE_ERROR.exception(Value::Ready(ReadyValue::None))
    }
}

/// The core, extcall and exception sets: enough for an action call, an
/// await on it, and the unwind of the rejection the await observes.
#[derive(Debug)]
#[expect(
    clippy::enum_variant_names,
    reason = "the variants are named after the instruction sets they carry, as the fullset's are"
)]
enum Instruction {
    CoreSet(CoreSet<TestSpec>),
    ExtCallSet(ExtCallSet<TestSpec>),
    ExcSet(ExcSet<TestSpec>),
}

impl From<CoreSet<TestSpec>> for Instruction {
    fn from(value: CoreSet<TestSpec>) -> Self {
        Self::CoreSet(value)
    }
}

impl From<ExtCallSet<TestSpec>> for Instruction {
    fn from(value: ExtCallSet<TestSpec>) -> Self {
        Self::ExtCallSet(value)
    }
}

impl From<ExcSet<TestSpec>> for Instruction {
    fn from(value: ExcSet<TestSpec>) -> Self {
        Self::ExcSet(value)
    }
}

/// The composite of the three sets over the Python value and exception.
#[derive(Default, waymark_vm_interpreter_composite::Interpreter)]
#[interpreter(
    instruction = Instruction,
    frame = waymark_vm_runtime_core::Frame<FunctionId, StateId, Value, RaisedException>,
    view = waymark_vm_runtime_core::FullRuntimeView<
        'r,
        Executable<Instruction>,
        FunctionId,
        StateId,
        Value,
        RaisedException,
    >,
)]
struct RuntimeInterpreter {
    #[interpreter(variant = CoreSet, instruction = CoreSet<TestSpec>)]
    core_set: waymark_vm_interpreter_coreset::CoreSetInterpreter<
        TestSpec,
        Executable<Instruction>,
        Value,
        RaisedException,
    >,

    #[interpreter(variant = ExtCallSet, instruction = ExtCallSet<TestSpec>)]
    extcall_set: waymark_vm_interpreter_extcallset::ExtCallSetInterpreter<
        TestSpec,
        FunctionId,
        StateId,
        Value,
        RaisedException,
    >,

    #[interpreter(variant = ExcSet, instruction = ExcSet<TestSpec>)]
    exc_set: waymark_vm_interpreter_excset::ExcSetInterpreter<
        TestSpec,
        FunctionId,
        Value,
        RaisedException,
    >,
}

/// The composite's effect over the Python value and exception.
type TestEffect = Effect<
    waymark_vm_interpreter_coreset::Effect<ReadyValue>,
    waymark_vm_interpreter_extcallset::Effect<usize, ReadyValue>,
    waymark_vm_interpreter_excset::Effect<RaisedException>,
>;

/// An action call whose await sits inside a handler block listing
/// `pattern`: the handler returns the caught exception from its register.
fn guarded_action_call(pattern: Vec<&str>) -> Executable<Instruction> {
    executable(vec![function::<Instruction>(
        3,
        vec![
            vec![
                ExcSet::PushExceptionHandlers {
                    handlers: vec![waymark_vm_exception_handler::ExceptionHandler {
                        handler_state: StateId(3),
                        pattern: TestConstExceptionPattern(
                            pattern.into_iter().map(ToOwned::to_owned).collect(),
                        ),
                        exception_dst: Some(RegisterId(2)),
                    }],
                }
                .into(),
                ExtCallSet::ActionCall {
                    dst: RegisterId(1),
                    action_ref: 7,
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
                ExcSet::PopExceptionHandlers { count: 1 }.into(),
                CoreSet::Return { src: RegisterId(2) }.into(),
            ],
            vec![CoreSet::Return { src: RegisterId(2) }.into()],
        ],
    )])
}

/// Runs `executable` up to its action call and rejects the call with
/// `rejection`, returning the effect the rejection surfaces as.
fn reject_the_action_call(
    executable: Executable<Instruction>,
    rejection: RaisedException,
) -> TestEffect {
    let mut runtime = Runtime::with_custom_entrypoint(
        RuntimeInterpreter::default(),
        executable,
        CallSpec {
            func: FunctionId(0),
            args: vec![ReadyValue::Int(41)],
        },
    )
    .expect("function 0 should exist");

    let emitted_effect = runtime
        .run()
        .expect("first run should emit the action call");
    let Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
        promise_state_id,
        ..
    }) = emitted_effect.effect
    else {
        panic!("first run should emit an action call");
    };

    runtime
        .reject_promise(promise_state_id, rejection)
        .expect("action call promise should reject cleanly");

    runtime
        .run()
        .expect("rejected action call should surface")
        .effect
}

#[test]
fn handlers_catch_a_rejection_by_a_base_class_of_its_exception() {
    let key_error =
        classes::KEY_ERROR.exception(Value::Ready(ReadyValue::String("absent".to_owned())));

    let effect =
        reject_the_action_call(guarded_action_call(vec!["LookupError"]), key_error.clone());

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(value, ReadyValue::Exception(Box::new(key_error)));
        }
        effect => panic!("the handler should return the caught exception: {effect:?}"),
    }
}

#[test]
fn handlers_listing_exception_do_not_catch_the_runtime_timeout() {
    let timeout = classes::ACTION_TIMEOUT.exception(Value::Ready(ReadyValue::None));

    let effect = reject_the_action_call(guarded_action_call(vec!["Exception"]), timeout.clone());

    match effect {
        Effect::ExcSet(waymark_vm_interpreter_excset::Effect::UnhandledException(exception)) => {
            assert_eq!(exception, timeout);
        }
        effect => panic!("the timeout should pass the handler and surface: {effect:?}"),
    }
}

#[test]
fn handlers_listing_exception_do_not_catch_a_lost_execution() {
    let lost = classes::ACTION_EXECUTION_LOST.exception(Value::Ready(ReadyValue::None));

    let effect = reject_the_action_call(guarded_action_call(vec!["Exception"]), lost.clone());

    match effect {
        Effect::ExcSet(waymark_vm_interpreter_excset::Effect::UnhandledException(exception)) => {
            assert_eq!(exception, lost);
        }
        effect => panic!("the lost execution should pass the handler and surface: {effect:?}"),
    }
}

#[test]
fn handlers_listing_exception_catch_an_execution_that_never_started() {
    let not_started =
        classes::ACTION_EXECUTION_NOT_STARTED.exception(Value::Ready(ReadyValue::None));

    let effect =
        reject_the_action_call(guarded_action_call(vec!["Exception"]), not_started.clone());

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(value, ReadyValue::Exception(Box::new(not_started)));
        }
        effect => panic!("the handler should return the caught exception: {effect:?}"),
    }
}

#[test]
fn action_call_can_resume_with_an_exception_error() {
    let executable = executable(vec![function::<Instruction>(
        3,
        vec![
            vec![
                ExtCallSet::ActionCall {
                    dst: RegisterId(1),
                    action_ref: 7,
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
            vec![CoreSet::Return { src: RegisterId(2) }.into()],
        ],
    )]);

    let mut runtime = Runtime::with_custom_entrypoint(
        RuntimeInterpreter::default(),
        executable,
        CallSpec {
            func: FunctionId(0),
            args: vec![ReadyValue::Int(41)],
        },
    )
    .expect("function 0 should exist");

    let emitted_effect = runtime
        .run()
        .expect("first run should emit the action call");
    let Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
        promise_state_id,
        action_ref,
        args,
    }) = emitted_effect.effect
    else {
        panic!("first run should emit an action call");
    };

    assert_eq!(action_ref, 7);
    assert_eq!(args, vec![ReadyValue::Int(41)]);

    let rejection = Exception {
        type_id: "ValueError".to_owned(),
        mro_type_ids: vec!["Exception".to_owned(), "BaseException".to_owned()],
        details: Value::Ready(ReadyValue::String("boom".to_owned())),
    };
    runtime
        .reject_promise(promise_state_id, rejection.clone())
        .expect("action call promise should reject cleanly");

    let emitted_effect = runtime.run().expect("rejected action call should surface");
    match emitted_effect.effect {
        Effect::ExcSet(waymark_vm_interpreter_excset::Effect::UnhandledException(exception)) => {
            assert_eq!(exception, rejection)
        }
        Effect::CoreSet(effect) => {
            panic!("the rejection should not complete the program: {effect:?}")
        }
        Effect::ExtCallSet(effect) => {
            panic!("the rejection should not suspend the program again: {effect:?}")
        }
    }
}

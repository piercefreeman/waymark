//! `ExcSet` instructions execute synchronously inside the fullset interpreter.

use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_excset::ExcSet;
use waymark_vm_interpreter_fullset::Effect;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{executable, function};

use crate::support::{Instruction, TestReadyValue, TestValue, new_runtime_with_args};

#[test]
fn runtime_executes_is_exception_checks_to_completion() {
    let executable = executable(vec![function::<Instruction>(
        3,
        vec![vec![
            ExcSet::IsException {
                dst: RegisterId(2),
                src: RegisterId(0),
                exception_type_id: Some(RegisterId(1)),
            }
            .into(),
            CoreSet::Return { src: RegisterId(2) }.into(),
        ]],
    )]);

    let mut runtime = new_runtime_with_args(
        executable,
        vec![
            TestReadyValue::exception(
                "synthetic.failure",
                TestValue::Ready(TestReadyValue::Int(7)),
            ),
            TestReadyValue::Text("synthetic.failure"),
        ],
    );

    let effect = runtime
        .run()
        .expect("excset check should complete without suspending");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(value, TestReadyValue::Bool(true));
        }
        Effect::ExcSet(effect) => match effect {},
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall { .. }) => {
            panic!("excset check should not emit an action call")
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::Sleep { .. }) => {
            panic!("excset check should not emit a sleep effect")
        }
        Effect::PureSet(effect) => match effect {},
    }
}

#[test]
fn runtime_executes_exception_details_to_completion() {
    let executable = executable(vec![function::<Instruction>(
        2,
        vec![vec![
            ExcSet::ExceptionDetails {
                dst: RegisterId(1),
                src: RegisterId(0),
            }
            .into(),
            CoreSet::Return { src: RegisterId(1) }.into(),
        ]],
    )]);

    let mut runtime = new_runtime_with_args(
        executable,
        vec![TestReadyValue::exception(
            "synthetic.failure",
            TestValue::Ready(TestReadyValue::Int(7)),
        )],
    );

    let effect = runtime
        .run()
        .expect("exception details should complete without suspending");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(value, TestReadyValue::Int(7));
        }
        Effect::ExcSet(effect) => match effect {},
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall { .. }) => {
            panic!("exception details should not emit an action call")
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::Sleep { .. }) => {
            panic!("exception details should not emit a sleep effect")
        }
        Effect::PureSet(effect) => match effect {},
    }
}

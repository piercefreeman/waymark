//! Function call → await → return control flow.

use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_interpreter_coreset::Effect;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};

use crate::support::{Instruction, TestReadyValue, new_runtime_with_args};

#[test]
fn runtime_executes_call_await_and_return_to_completion() {
    let executable = executable(vec![
        function::<Instruction>(
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
        function::<Instruction>(1, vec![vec![CoreSet::Return { src: RegisterId(0) }]]),
    ]);

    let mut runtime = new_runtime_with_args(executable, vec![TestReadyValue::Int(7)]);

    let emitted_effect = runtime
        .run()
        .expect("call/await/return program should complete");

    match emitted_effect.effect {
        Effect::Complete(value) => assert_eq!(value, TestReadyValue::Int(7)),
        Effect::UnhandledException(exception) => {
            panic!("program should complete successfully, got {exception:?}")
        }
    }
}

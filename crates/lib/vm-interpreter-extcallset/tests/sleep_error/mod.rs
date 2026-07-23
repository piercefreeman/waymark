//! Invalid sleep durations surface as interpreter errors through the runtime.

use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_interpreter_extcallset::{Error as InterpreterError, SleepError};
use waymark_vm_runtime::RunError;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{StateId, executable, function};

use crate::support::{
    RuntimeInstruction, TestReadyValue, TestSleepDurationError, new_runtime_with_args,
};

#[test]
fn runtime_surfaces_invalid_sleep_duration_errors_from_the_interpreter() {
    let mut runtime = new_runtime_with_args(
        executable(vec![function::<RuntimeInstruction>(
            2,
            vec![vec![
                ExtCallSet::Sleep {
                    dst: RegisterId(1),
                    duration: RegisterId(0),
                    resume: StateId(1),
                    unskippable: false,
                }
                .into(),
            ]],
        )]),
        vec![TestReadyValue(0)],
    );

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            InterpreterError::Sleep(SleepError::InvalidDuration {
                source: TestSleepDurationError::Zero,
            })
        )))
    ));
}

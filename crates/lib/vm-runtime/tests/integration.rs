use waymark_vm_runtime::{CallSpec, RunError};
use waymark_vm_runtime_core::{
    CaptureRuntimeView, FullRuntimeView, RegisterId, ResolvePromiseError,
};
use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_vm_runtime_test::{
    FunctionId, StateId, TestEffect, TestExecutable, TestExecutionError, TestInstruction,
    TestInterpreter, TestReadyValue, TestValue, executable, function, runtime,
    runtime_with_entrypoint, try_runtime,
};

struct HookInterpreter {
    mode: HookMode,
}

enum HookMode {
    ExitWithEffect(&'static str),
    RedirectExceptionToState(StateId),
}

impl CaptureRuntimeView<TestExecutable, FunctionId, StateId, TestValue> for HookInterpreter {
    type RuntimeView<'r> = FullRuntimeView<'r, TestExecutable, FunctionId, StateId, TestValue>;

    fn capture_runtime_view<'r>(
        view: FullRuntimeView<'r, TestExecutable, FunctionId, StateId, TestValue>,
    ) -> Self::RuntimeView<'r> {
        view
    }
}

impl waymark_vm_interpreter::Interpreter for HookInterpreter {
    type RuntimeView<'r> = FullRuntimeView<'r, TestExecutable, FunctionId, StateId, TestValue>;
    type Frame = waymark_vm_runtime::FrameFor<TestExecutable, TestValue>;
    type Instruction = TestInstruction;
    type Error = TestExecutionError;
    type Effect = TestEffect;

    fn enter_state<'r>(
        &self,
        _runtime: Self::RuntimeView<'r>,
        mut frame: Self::Frame,
    ) -> Result<waymark_vm_interpreter::ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error>
    {
        match self.mode {
            HookMode::ExitWithEffect(message) => Ok(
                waymark_vm_interpreter::ExecutionOutcome::ExitFrameWithEffect(TestEffect::Message(
                    message,
                )),
            ),
            HookMode::RedirectExceptionToState(handler_state) => {
                if frame.exception.is_some() {
                    frame.exception = None;
                    frame.state = handler_state;
                }
                Ok(waymark_vm_interpreter::ExecutionOutcome::Continue(frame))
            }
        }
    }

    fn execute<'r>(
        &self,
        runtime: Self::RuntimeView<'r>,
        frame: Self::Frame,
        instruction: &Self::Instruction,
    ) -> Result<waymark_vm_interpreter::ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error>
    {
        waymark_vm_interpreter::Interpreter::execute(&TestInterpreter, runtime, frame, instruction)
    }
}

#[test]
fn with_custom_entrypoint_uses_requested_function_and_ready_arguments() {
    let mut runtime = runtime_with_entrypoint(
        executable(vec![
            function(0, vec![vec![TestInstruction::Emit("wrong")]]),
            function(3, vec![vec![TestInstruction::EmitRegister(RegisterId(1))]]),
        ]),
        CallSpec {
            func: FunctionId(1),
            args: vec![TestReadyValue::Int(7), TestReadyValue::Int(9)],
        },
    );

    assert_eq!(
        runtime.run().expect("custom entrypoint should emit"),
        TestEffect::Value(TestReadyValue::Int(9))
    );
}

#[test]
fn with_conventional_entrypoint_rejects_missing_default_function() {
    let err = match try_runtime(executable(Vec::new())) {
        Ok(_) => panic!("missing function 0 should fail the conventional entrypoint"),
        Err(err) => err,
    };

    assert_eq!(
        err.to_string(),
        format!(
            "function {:?} is not found in the functions table",
            FunctionId(0)
        )
    );
}

#[test]
fn run_skips_yielded_frames_until_an_effect_is_emitted() {
    let mut runtime = runtime(executable(vec![
        function(
            0,
            vec![vec![TestInstruction::EnqueueFrameAndExit {
                func: FunctionId(1),
                state: StateId(0),
                num_regs: 0,
            }]],
        ),
        function(0, vec![vec![TestInstruction::Emit("next")]]),
    ]));

    assert_eq!(
        runtime.run().expect("queued frame should emit"),
        TestEffect::Message("next")
    );
}

#[test]
fn run_returns_step_error_when_a_state_has_no_instructions() {
    let mut runtime = runtime_with_entrypoint(
        executable(vec![function(0, vec![Vec::new()])]),
        CallSpec {
            func: FunctionId(0),
            args: Vec::<TestReadyValue>::new(),
        },
    );

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(
            waymark_vm_runtime::step::Error::NoInstructions
        ))
    ));
}

#[test]
fn run_returns_step_error_when_a_ready_frame_points_at_a_missing_state() {
    let mut runtime = runtime(executable(vec![function(
        0,
        vec![vec![TestInstruction::EnqueueFrameAndExit {
            func: FunctionId(0),
            state: StateId(99),
            num_regs: 0,
        }]],
    )]));

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(
            waymark_vm_runtime::step::Error::InvalidState
        ))
    ));
}

#[test]
fn resolve_promise_resumes_a_suspended_frame_and_emits_the_resolved_value() {
    let mut runtime = runtime(executable(vec![function(
        1,
        vec![
            vec![TestInstruction::Suspend {
                dst: RegisterId(0),
                resume: StateId(1),
            }],
            vec![TestInstruction::EmitRegister(RegisterId(0))],
        ],
    )]));

    assert!(matches!(runtime.run(), Err(RunError::NoReadyFrame)));

    runtime
        .resolve_promise(PromiseStateId(0), TestReadyValue::Int(41))
        .expect("prepared promise should resolve");

    assert_eq!(
        runtime.run().expect("resumed frame should emit"),
        TestEffect::Value(TestReadyValue::Int(41))
    );
}

#[test]
fn resolve_promise_rejects_unknown_promise_ids_without_disturbing_waiting_work() {
    let mut runtime = runtime(executable(vec![function(
        1,
        vec![
            vec![TestInstruction::Suspend {
                dst: RegisterId(0),
                resume: StateId(1),
            }],
            vec![TestInstruction::EmitRegister(RegisterId(0))],
        ],
    )]));

    assert!(matches!(runtime.run(), Err(RunError::NoReadyFrame)));

    let err = runtime
        .resolve_promise(PromiseStateId(9), TestReadyValue::Int(41))
        .expect_err("unknown promise IDs should be rejected");

    let ResolvePromiseError::PromiseStateNotFound(err) = err else {
        panic!("invalid promise IDs should surface a not-found error");
    };
    assert_eq!(err.promise_state_id, PromiseStateId(9));

    runtime
        .resolve_promise(PromiseStateId(0), TestReadyValue::Int(41))
        .expect("the waiting promise should still resolve cleanly");

    assert_eq!(
        runtime.run().expect("resumed frame should still emit"),
        TestEffect::Value(TestReadyValue::Int(41))
    );
}

#[test]
fn resolve_promise_preserves_the_first_value_when_duplicate_resolution_occurs() {
    let mut runtime = runtime(executable(vec![function(
        1,
        vec![
            vec![TestInstruction::Suspend {
                dst: RegisterId(0),
                resume: StateId(1),
            }],
            vec![TestInstruction::EmitRegister(RegisterId(0))],
        ],
    )]));

    assert!(matches!(runtime.run(), Err(RunError::NoReadyFrame)));

    runtime
        .resolve_promise(PromiseStateId(0), TestReadyValue::Int(7))
        .expect("first promise resolution should succeed");

    let err = runtime
        .resolve_promise(PromiseStateId(0), TestReadyValue::Int(11))
        .expect_err("already-ready promise should reject a new value");

    let ResolvePromiseError::AlreadyResolved(err) = err else {
        panic!("duplicate resolutions should report already-resolved errors");
    };

    assert_eq!(err.new_value, TestReadyValue::Int(11));
    assert_eq!(
        runtime
            .run()
            .expect("resumed frame should keep the first value"),
        TestEffect::Value(TestReadyValue::Int(7))
    );
}

#[test]
fn reject_promise_bubbles_uncaught_exceptions() {
    let mut runtime = runtime(executable(vec![function(
        1,
        vec![
            vec![TestInstruction::Suspend {
                dst: RegisterId(0),
                resume: StateId(1),
            }],
            vec![TestInstruction::EmitException],
        ],
    )]));

    assert!(matches!(runtime.run(), Err(RunError::NoReadyFrame)));

    runtime
        .reject_promise(
            PromiseStateId(0),
            Exception {
                type_id: "ValueError".to_owned(),
                details: TestReadyValue::Int(41),
            },
        )
        .expect("exceptional promise result should resolve");

    assert_eq!(
        runtime
            .run()
            .expect("resumed frame should emit the raised exception"),
        TestEffect::UnhandledException(Exception {
            type_id: "ValueError".to_owned(),
            details: TestReadyValue::Int(41),
        })
    );
}

#[test]
fn state_entry_hook_can_exit_before_instruction_dispatch() {
    let executable = executable(vec![function(
        0,
        vec![vec![TestInstruction::Fail(
            "instruction dispatch should be skipped",
        )]],
    )]);

    let mut runtime = waymark_vm_runtime::Runtime::with_conventional_entrypoint(
        HookInterpreter {
            mode: HookMode::ExitWithEffect("hook"),
        },
        executable,
    )
    .expect("entrypoint should exist");

    assert_eq!(
        runtime
            .run()
            .expect("state-entry hook should emit before dispatch"),
        TestEffect::Message("hook")
    );
}

#[test]
fn state_entry_hook_can_redirect_exceptional_resumes_when_the_interpreter_wants() {
    let executable = executable(vec![function(
        1,
        vec![
            vec![TestInstruction::Suspend {
                dst: RegisterId(0),
                resume: StateId(1),
            }],
            vec![TestInstruction::Fail(
                "resumed state should be skipped after redirect",
            )],
            vec![TestInstruction::Emit("handled")],
        ],
    )]);

    let mut runtime = waymark_vm_runtime::Runtime::with_conventional_entrypoint(
        HookInterpreter {
            mode: HookMode::RedirectExceptionToState(StateId(2)),
        },
        executable,
    )
    .expect("entrypoint should exist");

    assert!(matches!(runtime.run(), Err(RunError::NoReadyFrame)));

    runtime
        .reject_promise(
            PromiseStateId(0),
            Exception {
                type_id: "ValueError".to_owned(),
                details: TestReadyValue::Int(41),
            },
        )
        .expect("exceptional promise result should resolve");

    assert_eq!(
        runtime
            .run()
            .expect("state-entry hook should redirect into the handler state"),
        TestEffect::Message("handled")
    );
}

#[test]
fn snapshot_suspended_and_from_snapshot_preserves_suspended_promises() {
    let make_executable = || {
        executable(vec![function(
            1,
            vec![
                vec![TestInstruction::Suspend {
                    dst: RegisterId(0),
                    resume: StateId(1),
                }],
                vec![TestInstruction::EmitRegister(RegisterId(0))],
            ],
        )])
    };

    // Create runtime and run until it suspends.
    let mut runtime = waymark_vm_runtime::Runtime::with_conventional_entrypoint(
        TestInterpreter,
        make_executable(),
    )
    .expect("entrypoint should exist");

    assert!(matches!(runtime.run(), Err(RunError::NoReadyFrame)));

    // Snapshot the suspended runtime.
    let mut buf = Vec::new();
    runtime
        .snapshot(&mut rmp_serde::Serializer::new(&mut buf))
        .expect("suspended runtime should snapshot");

    // Restore into a new runtime with a fresh executable.
    let mut de = rmp_serde::Deserializer::new(&buf[..]);
    let mut restored =
        waymark_vm_runtime::Runtime::from_snapshot(TestInterpreter, make_executable(), &mut de)
            .expect("snapshotted bytes should restore");

    // Resolve the promise in the restored runtime.
    restored
        .resolve_promise(PromiseStateId(0), TestReadyValue::Int(42))
        .expect("promise should resolve after restore");

    assert_eq!(
        restored.run().expect("restored runtime should emit"),
        TestEffect::Value(TestReadyValue::Int(42))
    );
}

#[test]
fn snapshot_suspended_and_from_snapshot_preserves_rejected_promises() {
    let make_executable = || {
        executable(vec![function(
            1,
            vec![
                vec![TestInstruction::Suspend {
                    dst: RegisterId(0),
                    resume: StateId(1),
                }],
                vec![TestInstruction::EmitException],
            ],
        )])
    };

    let mut runtime = waymark_vm_runtime::Runtime::with_conventional_entrypoint(
        TestInterpreter,
        make_executable(),
    )
    .expect("entrypoint should exist");

    assert!(matches!(runtime.run(), Err(RunError::NoReadyFrame)));

    // Snapshot.
    let mut buf = Vec::new();
    runtime
        .snapshot(&mut rmp_serde::Serializer::new(&mut buf))
        .expect("suspended runtime should snapshot");

    // Restore.
    let mut de = rmp_serde::Deserializer::new(&buf[..]);
    let mut restored =
        waymark_vm_runtime::Runtime::from_snapshot(TestInterpreter, make_executable(), &mut de)
            .expect("snapshotted bytes should restore");

    // Reject the promise.
    restored
        .reject_promise(
            PromiseStateId(0),
            Exception {
                type_id: "ValueError".to_owned(),
                details: TestReadyValue::Int(41),
            },
        )
        .expect("promise should reject after restore");

    assert_eq!(
        restored
            .run()
            .expect("restored runtime should emit exception"),
        TestEffect::UnhandledException(Exception {
            type_id: "ValueError".to_owned(),
            details: TestReadyValue::Int(41),
        })
    );
}

#[test]
fn allow_live_snapshots_runtime_with_ready_frames() {
    let runtime = runtime(executable(vec![function(
        0,
        vec![vec![TestInstruction::Emit("still-alive")]],
    )]));

    let mut buf = Vec::new();
    runtime
        .snapshot(&mut rmp_serde::Serializer::new(&mut buf))
        .expect("snapshot_active should snapshot with ready frames");

    let mut de = rmp_serde::Deserializer::new(&buf[..]);
    let mut restored = waymark_vm_runtime::Runtime::from_snapshot(
        TestInterpreter,
        executable(vec![function(
            0,
            vec![vec![TestInstruction::Emit("still-alive")]],
        )]),
        &mut de,
    )
    .expect("live snapshot should restore");

    assert_eq!(
        restored.run().expect("restored runtime should emit"),
        TestEffect::Message("still-alive")
    );
}

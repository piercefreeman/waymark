//! Behavioral tests for the composite `Interpreter` derive.
//!
//! A self-contained toy world: one view type, one frame type, and a single
//! configurable sub-interpreter type used for both fields of the composite.
//! The assertions prove the semantics of the generated implementation —
//! per-field capture, hook chaining while the frame stays in the same
//! state, instruction dispatch, and error/effect variant mapping.

use waymark_vm_interpreter::{ExecutionOutcome, Interpreter};
use waymark_vm_runtime_view_capture::CaptureRuntimeView;

/// The composite's runtime view data type.
#[derive(Default)]
struct TestView;

/// The frame: a state identifier plus a log of `(interpreter, hook)`
/// invocations.
struct TestFrame {
    state: u32,
    log: Vec<(&'static str, &'static str)>,
}

impl TestFrame {
    fn initial() -> Self {
        Self {
            state: 0,
            log: Vec::new(),
        }
    }
}

impl waymark_vm_interpreter_composite_core::DetectStateSwitch for TestFrame {
    type StateToken = u32;

    fn capture_state_token(&self) -> Self::StateToken {
        self.state
    }

    fn state_switched(&self, token: &Self::StateToken) -> bool {
        self.state != *token
    }
}

/// What a sub-interpreter does when one of its hooks or `execute` runs.
#[derive(Debug, Clone, Copy)]
enum Command {
    Continue,
    SwitchState(u32),
    ExitFrame,
    Effect(&'static str),
    Fail(&'static str),
}

#[derive(Debug, PartialEq, Eq)]
struct SubError(&'static str);

impl core::fmt::Display for SubError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "sub failed: {}", self.0)
    }
}

impl core::error::Error for SubError {}

#[derive(Debug)]
struct SubInstruction(Command);

/// The configurable sub-interpreter; both composite fields use this type.
struct Sub {
    name: &'static str,
    enter_state: Command,
    before_execute: Command,
    after_execute: Command,
}

impl Sub {
    fn quiet(name: &'static str) -> Self {
        Self {
            name,
            enter_state: Command::Continue,
            before_execute: Command::Continue,
            after_execute: Command::Continue,
        }
    }

    fn run(
        &self,
        hook: &'static str,
        command: Command,
        mut frame: TestFrame,
    ) -> Result<ExecutionOutcome<TestFrame, &'static str>, SubError> {
        frame.log.push((self.name, hook));
        match command {
            Command::Continue => Ok(ExecutionOutcome::Continue(frame)),
            Command::SwitchState(state) => {
                frame.state = state;
                Ok(ExecutionOutcome::Continue(frame))
            }
            Command::ExitFrame => Ok(ExecutionOutcome::ExitFrame),
            Command::Effect(effect) => Ok(ExecutionOutcome::ExitFrameWithEffect(effect)),
            Command::Fail(message) => Err(SubError(message)),
        }
    }
}

impl<'s> CaptureRuntimeView<'s, TestView> for &'s mut TestView {
    fn capture_runtime_view(source: &'s mut TestView) -> Self {
        source
    }
}

impl Interpreter for Sub {
    type RuntimeView<'r> = &'r mut TestView;
    type Frame = TestFrame;
    type Instruction = SubInstruction;
    type Error = SubError;
    type Effect = &'static str;

    fn enter_state<'r>(
        &self,
        _runtime_view: Self::RuntimeView<'r>,
        frame: Self::Frame,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        self.run("enter_state", self.enter_state, frame)
    }

    fn before_execute<'r>(
        &self,
        _runtime_view: Self::RuntimeView<'r>,
        frame: Self::Frame,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        self.run("before_execute", self.before_execute, frame)
    }

    fn after_execute<'r>(
        &self,
        _runtime_view: Self::RuntimeView<'r>,
        frame: Self::Frame,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        self.run("after_execute", self.after_execute, frame)
    }

    fn execute<'r>(
        &self,
        _runtime_view: Self::RuntimeView<'r>,
        frame: Self::Frame,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        self.run("execute", instruction.0, frame)
    }
}

#[derive(Debug)]
enum ComposedInstruction {
    First(SubInstruction),
    Second(SubInstruction),
}

#[derive(waymark_vm_interpreter_composite::Interpreter)]
#[interpreter(
    instruction = ComposedInstruction,
    frame = TestFrame,
    view = TestView,
)]
struct Composite {
    #[interpreter(variant = First, instruction = SubInstruction)]
    first: Sub,

    #[interpreter(variant = Second, instruction = SubInstruction)]
    second: Sub,
}

fn quiet_composite() -> Composite {
    Composite {
        first: Sub::quiet("first"),
        second: Sub::quiet("second"),
    }
}

#[test]
fn hooks_chain_in_field_order_while_the_state_holds() {
    let composite = quiet_composite();

    let outcome = composite
        .enter_state(TestView, TestFrame::initial())
        .expect("quiet hooks should succeed");

    let ExecutionOutcome::Continue(frame) = outcome else {
        panic!("quiet hooks should continue");
    };
    assert_eq!(
        frame.log,
        vec![("first", "enter_state"), ("second", "enter_state")]
    );
}

#[test]
fn hook_chain_stops_after_a_state_switch() {
    let mut composite = quiet_composite();
    composite.first.enter_state = Command::SwitchState(9);

    let outcome = composite
        .enter_state(TestView, TestFrame::initial())
        .expect("state switch is not an error");

    let ExecutionOutcome::Continue(frame) = outcome else {
        panic!("state switch should continue in the new state");
    };
    assert_eq!(frame.state, 9);
    assert_eq!(frame.log, vec![("first", "enter_state")]);
}

#[test]
fn hook_chain_short_circuits_on_frame_exit() {
    let mut composite = quiet_composite();
    composite.first.before_execute = Command::ExitFrame;

    let outcome = composite
        .before_execute(TestView, TestFrame::initial())
        .expect("frame exit is not an error");

    assert!(matches!(outcome, ExecutionOutcome::ExitFrame));
}

#[test]
fn hook_effects_are_mapped_into_the_field_variant() {
    let mut composite = quiet_composite();
    composite.second.after_execute = Command::Effect("signal");

    let outcome = composite
        .after_execute(TestView, TestFrame::initial())
        .expect("emitting an effect is not an error");

    let ExecutionOutcome::ExitFrameWithEffect(Effect::Second(effect)) = outcome else {
        panic!("the second interpreter's effect should map into its variant");
    };
    assert_eq!(effect, "signal");
}

#[test]
fn hook_errors_are_mapped_into_the_field_variant() {
    let mut composite = quiet_composite();
    composite.second.enter_state = Command::Fail("broken");

    let result = composite.enter_state(TestView, TestFrame::initial());

    let Err(Error::Second(inner)) = result else {
        panic!("the second interpreter's error should map into its variant");
    };
    assert_eq!(inner, SubError("broken"));
}

#[test]
fn execute_dispatches_by_instruction_variant() {
    let composite = quiet_composite();

    let outcome = composite
        .execute(
            TestView,
            TestFrame::initial(),
            &ComposedInstruction::Second(SubInstruction(Command::Effect("routed"))),
        )
        .expect("dispatch should succeed");

    let ExecutionOutcome::ExitFrameWithEffect(Effect::Second(effect)) = outcome else {
        panic!("the instruction should route to the second interpreter");
    };
    assert_eq!(effect, "routed");

    let outcome = composite
        .execute(
            TestView,
            TestFrame::initial(),
            &ComposedInstruction::First(SubInstruction(Command::Continue)),
        )
        .expect("dispatch should succeed");
    let ExecutionOutcome::Continue(frame) = outcome else {
        panic!("a continuing instruction should continue");
    };
    assert_eq!(frame.log, vec![("first", "execute")]);
}

#[test]
fn generated_error_is_display_and_source_transparent() {
    let error: Error<SubError, SubError> = Error::First(SubError("oops"));
    assert_eq!(error.to_string(), "sub failed: oops");
    assert!(core::error::Error::source(&error).is_none());
}

#[test]
fn generated_error_forwards_source() {
    /// An error with a source, for proving the generated `Error` forwards
    /// `source()` through its transparent variants.
    #[derive(Debug)]
    struct SourcedError(SubError);

    impl core::fmt::Display for SourcedError {
        fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            write!(formatter, "sourced: {}", self.0)
        }
    }

    impl core::error::Error for SourcedError {
        fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    let error: Error<SourcedError, SubError> = Error::First(SourcedError(SubError("oops")));
    assert_eq!(error.to_string(), "sourced: sub failed: oops");
    let source = core::error::Error::source(&error).expect("the inner source should be forwarded");
    assert_eq!(source.to_string(), "sub failed: oops");
}

mod renamed_crate {
    //! The `crate = …` override: the derive resolves through the given
    //! path instead of the literal `waymark_vm_interpreter_composite`.

    use super::{Command, ComposedInstruction, Sub, SubInstruction, TestFrame, TestView};
    use waymark_vm_interpreter::{ExecutionOutcome, Interpreter};
    use waymark_vm_interpreter_composite as renamed_composite;

    #[derive(renamed_composite::Interpreter)]
    #[interpreter(
        crate = renamed_composite,
        instruction = ComposedInstruction,
        frame = TestFrame,
        view = TestView,
    )]
    struct RenamedComposite {
        #[interpreter(variant = First, instruction = SubInstruction)]
        first: Sub,

        #[interpreter(variant = Second, instruction = SubInstruction)]
        second: Sub,
    }

    #[test]
    fn derive_honors_the_crate_path_override() {
        let composite = RenamedComposite {
            first: Sub::quiet("first"),
            second: Sub::quiet("second"),
        };

        let outcome = composite
            .execute(
                TestView,
                TestFrame::initial(),
                &ComposedInstruction::First(SubInstruction(Command::Effect("ok"))),
            )
            .expect("dispatch should succeed");

        assert!(matches!(
            outcome,
            ExecutionOutcome::ExitFrameWithEffect(Effect::First("ok"))
        ));
    }
}

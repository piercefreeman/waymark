//! Ephemeral wrapper-function generation for policy-annotated action calls.
//!
//! Policy brackets (retry/timeout) lower inside a dedicated per-call-site
//! function that wraps the plain action call. Every call site - sequential
//! or fan-out - then invokes the wrapper through an ordinary function call,
//! so the policy machinery has exactly one lowering path and the wrapper
//! frame owns the argument registers for the whole policy lifetime.

use waymark_vm_ast_old::Literal;
use waymark_vm_bytecode_core::FunctionId;
use waymark_vm_instructions_coreset::SelectArm;
use waymark_vm_runtime_core::RegisterId;

use crate::Marked;
use crate::function::extras::ExtraFunctions;

use super::bytecode::emitter::FunctionEmitter;
use super::env::LocalFrame;
use super::plan::Unsupported;
use super::suspend::PromiseMarker;
use super::{Error, ErrorFor};

/// The exception type id raised when a per-attempt timeout fires.
const ACTION_TIMEOUT_TYPE_ID: &str = "ActionTimeout";

/// Generates the wrapper function for one policy-annotated action call site
/// and returns its id.
///
/// The wrapper takes the action's keyword arguments as positional inputs in
/// call-site order and forwards them to the wrapped action. All timeout
/// brackets collapse into one per-attempt duration by minimum over nonzero
/// seconds; zero-second brackets are ignored.
pub fn create<Spec, Lowering>(
    extra_fns: &mut ExtraFunctions<Spec>,
    action_name: &str,
    action_ref: <Spec as waymark_vm_instructions_extcallset::Spec>::ActionRef,
    kwarg_count: usize,
    policies: &[waymark_vm_ast_old::PolicyBracket],
) -> Result<FunctionId, ErrorFor<Spec, Lowering>>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    let mut timeout_seconds: Option<u64> = None;
    for policy in policies {
        match policy {
            waymark_vm_ast_old::PolicyBracket::Retry(_) => {
                return Err(Unsupported::RetryPolicy {
                    action_name: action_name.to_owned(),
                }
                .into());
            }
            waymark_vm_ast_old::PolicyBracket::Timeout(timeout_policy) => {
                let seconds = timeout_policy.timeout.seconds;
                if seconds == 0 {
                    continue;
                }
                timeout_seconds = Some(match timeout_seconds {
                    Some(existing) => existing.min(seconds),
                    None => seconds,
                });
            }
        }
    }

    let mut emitter = FunctionEmitter::<Spec>::new();
    let mut local_frame = LocalFrame::new();

    // Inputs occupy the first registers of the frame in call order.
    let arg_registers: Vec<_> = (0..kwarg_count)
        .map(|_| local_frame.allocate_register())
        .collect();

    let promise_register = Marked::mark(local_frame.allocate_register());
    let resume_after_call = emitter.reserve_state();
    emitter.emit_extcall(
        promise_register,
        action_ref,
        arg_registers,
        resume_after_call,
    );
    emitter.switch_to(resume_after_call);

    match timeout_seconds {
        None => emit_plain_tail(&mut emitter, &mut local_frame, promise_register),
        Some(seconds) => emit_timed_tail::<Spec, Lowering>(
            &mut emitter,
            &mut local_frame,
            promise_register,
            action_name,
            seconds,
        )?,
    }

    let function = waymark_vm_bytecode::Function {
        states: emitter.finish(),
        num_regs: local_frame.num_registers(),
    };

    Ok(extra_fns.insert(function))
}

/// Emits the plain resolution tail: await the action promise and return its
/// value.
fn emit_plain_tail<Spec>(
    emitter: &mut FunctionEmitter<Spec>,
    local_frame: &mut LocalFrame,
    promise_register: Marked<RegisterId, PromiseMarker>,
) where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    let result_register = local_frame.allocate_register();
    let resume_after_await = emitter.reserve_state();
    emitter.emit_await(result_register, promise_register, resume_after_await);
    emitter.switch_to(resume_after_await);
    emitter.emit_return(result_register);
}

/// Emits the timed resolution tail: race the action promise against the
/// timeout sleep. The action arm returns the action's value; the sleep arm
/// raises an `ActionTimeout` exception.
fn emit_timed_tail<Spec, Lowering>(
    emitter: &mut FunctionEmitter<Spec>,
    local_frame: &mut LocalFrame,
    promise_register: Marked<RegisterId, PromiseMarker>,
    action_name: &str,
    seconds: u64,
) -> Result<(), ErrorFor<Spec, Lowering>>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    let seconds_literal = i64::try_from(seconds).map_err(|_| Error::TimeoutDurationOutOfRange {
        action_name: action_name.to_owned(),
        seconds,
    })?;
    let duration_value =
        Lowering::lower_literal(&Literal::Int(seconds_literal)).map_err(Error::LiteralLowering)?;
    let duration_register = local_frame.allocate_register();
    emitter.emit_load_const(duration_register, duration_value);

    let sleep_register = Marked::mark(local_frame.allocate_register());
    let resume_after_sleep = emitter.reserve_state();
    emitter.emit_sleep(sleep_register, duration_register, resume_after_sleep, true);
    emitter.switch_to(resume_after_sleep);

    let result_register = local_frame.allocate_register();
    let sleep_result_register = local_frame.allocate_register();
    let resume_on_action = emitter.reserve_state();
    let resume_on_timeout = emitter.reserve_state();
    emitter.emit_select(vec![
        SelectArm {
            src: *promise_register,
            dst: result_register,
            resume: resume_on_action,
        },
        SelectArm {
            src: *sleep_register,
            dst: sleep_result_register,
            resume: resume_on_timeout,
        },
    ]);

    emitter.switch_to(resume_on_action);
    emitter.emit_return(result_register);

    emitter.switch_to(resume_on_timeout);
    let type_id_value =
        Lowering::lower_literal(&Literal::String(ACTION_TIMEOUT_TYPE_ID.to_owned()))
            .map_err(Error::LiteralLowering)?;
    let type_id_register = local_frame.allocate_register();
    emitter.emit_load_const(type_id_register, type_id_value);

    let details_value = Lowering::lower_literal(&Literal::None).map_err(Error::LiteralLowering)?;
    let details_register = local_frame.allocate_register();
    emitter.emit_load_const(details_register, details_value);

    let exception_register = local_frame.allocate_register();
    emitter.emit_make_exception(exception_register, type_id_register, details_register);
    emitter.emit_raise(exception_register);

    Ok(())
}

#[cfg(test)]
mod tests {
    use waymark_vm_ast_old::{DurationLiteral, PolicyBracket, RetryPolicy, TimeoutPolicy};
    use waymark_vm_bytecode_core::FunctionId;
    use waymark_vm_compiler_for_ast_old_test_support::{TestActionRef, TestLowering, TestSpec};

    use super::super::{Error, Unsupported};
    use super::create;
    use crate::function::extras::ExtraFunctions;

    fn timeout_policy(seconds: u64) -> PolicyBracket {
        PolicyBracket::Timeout(TimeoutPolicy {
            timeout: DurationLiteral { seconds },
        })
    }

    /// Generates one wrapper for the policies and renders its bytecode.
    fn display_wrapper(kwarg_count: usize, policies: &[PolicyBracket]) -> String {
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(1);
        create::<TestSpec, TestLowering>(
            &mut extra_fns,
            "notify",
            TestActionRef("notify".to_owned()),
            kwarg_count,
            policies,
        )
        .expect("the wrapper should compile");
        let functions = extra_fns.finish();
        waymark_vm_bytecode_fmt::display(&functions[0]).to_string()
    }

    #[test]
    fn generates_the_plain_action_wrapper_body() {
        insta::assert_snapshot!(display_wrapper(2, &[]), @r#"
        s0:
          ExtCallSet(ActionCall { dst: r2, action_ref: TestActionRef("notify"), args: [r0, r1], resume: s1 })
        s1:
          CoreSet(Await { dst: r3, src: r2, resume: s2 })
        s2:
          CoreSet(Return { src: r3 })
        "#);
    }

    #[test]
    fn generates_the_timed_action_wrapper_body() {
        insta::assert_snapshot!(display_wrapper(1, &[timeout_policy(30)]), @r#"
        s0:
          ExtCallSet(ActionCall { dst: r1, action_ref: TestActionRef("notify"), args: [r0], resume: s1 })
        s1:
          PureSet(LoadConst { dst: r2, value: Int(30) })
          ExtCallSet(Sleep { dst: r3, duration: r2, resume: s2, unskippable: true })
        s2:
          CoreSet(Select { arms: [SelectArm { src: r1, dst: r4, resume: s3 }, SelectArm { src: r3, dst: r5, resume: s4 }] })
        s3:
          CoreSet(Return { src: r4 })
        s4:
          PureSet(LoadConst { dst: r6, value: String("ActionTimeout") })
          PureSet(LoadConst { dst: r7, value: None })
          PureSet(MakeException { dst: r8, type_id: r6, details: r7 })
          CoreSet(Raise { src: r8 })
        "#);
    }

    #[test]
    fn collapses_multiple_timeouts_to_the_minimum_nonzero() {
        insta::assert_snapshot!(
            display_wrapper(
                0,
                &[timeout_policy(30), timeout_policy(0), timeout_policy(10)],
            ),
            @r#"
        s0:
          ExtCallSet(ActionCall { dst: r0, action_ref: TestActionRef("notify"), args: [], resume: s1 })
        s1:
          PureSet(LoadConst { dst: r1, value: Int(10) })
          ExtCallSet(Sleep { dst: r2, duration: r1, resume: s2, unskippable: true })
        s2:
          CoreSet(Select { arms: [SelectArm { src: r0, dst: r3, resume: s3 }, SelectArm { src: r2, dst: r4, resume: s4 }] })
        s3:
          CoreSet(Return { src: r3 })
        s4:
          PureSet(LoadConst { dst: r5, value: String("ActionTimeout") })
          PureSet(LoadConst { dst: r6, value: None })
          PureSet(MakeException { dst: r7, type_id: r5, details: r6 })
          CoreSet(Raise { src: r7 })
        "#
        );
    }

    #[test]
    fn ignores_zero_second_timeouts_entirely() {
        insta::assert_snapshot!(display_wrapper(0, &[timeout_policy(0)]), @r#"
        s0:
          ExtCallSet(ActionCall { dst: r0, action_ref: TestActionRef("notify"), args: [], resume: s1 })
        s1:
          CoreSet(Await { dst: r1, src: r0, resume: s2 })
        s2:
          CoreSet(Return { src: r1 })
        "#);
    }

    #[test]
    fn assigns_sequential_ids_after_the_source_functions() {
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(2);

        let first = create::<TestSpec, TestLowering>(
            &mut extra_fns,
            "first",
            TestActionRef("first".to_owned()),
            0,
            &[],
        )
        .expect("first wrapper should compile");
        let second = create::<TestSpec, TestLowering>(
            &mut extra_fns,
            "second",
            TestActionRef("second".to_owned()),
            1,
            &[],
        )
        .expect("second wrapper should compile");

        assert_eq!(first, FunctionId(2));
        assert_eq!(second, FunctionId(3));
        assert_eq!(extra_fns.finish().len(), 2);
    }

    #[test]
    fn rejects_retry_policies_pending_their_lowering() {
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(0);

        let retry_error = create::<TestSpec, TestLowering>(
            &mut extra_fns,
            "notify",
            TestActionRef("notify".to_owned()),
            0,
            &[PolicyBracket::Retry(RetryPolicy {
                exception_types: Vec::new(),
                max_retries: 2,
                backoff: None,
            })],
        )
        .expect_err("retry policies should not lower yet");
        assert!(matches!(
            retry_error,
            Error::Unsupported(Unsupported::RetryPolicy { action_name })
                if action_name == "notify"
        ));

        // A retry bracket rejects even when combined with timeout brackets.
        let mixed_error = create::<TestSpec, TestLowering>(
            &mut extra_fns,
            "notify",
            TestActionRef("notify".to_owned()),
            0,
            &[
                timeout_policy(30),
                PolicyBracket::Retry(RetryPolicy {
                    exception_types: Vec::new(),
                    max_retries: 2,
                    backoff: None,
                }),
            ],
        )
        .expect_err("mixed retry policies should not lower yet");
        assert!(matches!(
            mixed_error,
            Error::Unsupported(Unsupported::RetryPolicy { action_name })
                if action_name == "notify"
        ));

        assert!(extra_fns.finish().is_empty());
    }
}

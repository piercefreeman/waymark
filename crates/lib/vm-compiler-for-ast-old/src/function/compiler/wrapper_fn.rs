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
use waymark_vm_instructions_pureset::BinaryOpKind;
use waymark_vm_runtime_core::RegisterId;

use crate::Marked;
use crate::function::extras::ExtraFunctions;

use super::bytecode::emitter::FunctionEmitter;
use super::env::LocalFrame;
use super::suspend::PromiseMarker;
use super::{Error, ErrorFor};

/// The exception type id raised when a per-attempt timeout fires.
const ACTION_TIMEOUT_TYPE_ID: &str = waymark_vm_exception_type_ids::ACTION_TIMEOUT;

/// One digested retry bracket.
struct RetryPlan {
    /// The exception types this bracket retries; empty retries everything.
    exception_types: Vec<String>,

    /// The retry budget: extra attempts beyond the first.
    max_retries: u32,

    /// The fixed backoff to sleep before each retry, in seconds.
    backoff_seconds: Option<u64>,
}

impl RetryPlan {
    /// Digests one retry bracket, normalizing the catch-all filter the same
    /// way `try`/`except` normalizes `Exception`.
    fn digest(retry_policy: &waymark_vm_ast_old::RetryPolicy) -> Self {
        let exception_types = if retry_policy.exception_types == ["Exception"] {
            Vec::new()
        } else {
            retry_policy.exception_types.clone()
        };
        let backoff_seconds = retry_policy
            .backoff
            .as_ref()
            .map(|backoff| backoff.seconds)
            .filter(|&seconds| seconds != 0);

        Self {
            exception_types,
            max_retries: retry_policy.max_retries,
            backoff_seconds,
        }
    }

    /// Returns whether this bracket statically retries the compiled-in
    /// `ActionTimeout` exception.
    ///
    /// The catch-all filter deliberately does not: with no cancellation the
    /// timed-out attempt may still be running, so retrying timeouts requires
    /// an explicit opt-in - matching the legacy runner semantics.
    fn retries_timeouts(&self) -> bool {
        self.exception_types
            .iter()
            .any(|exception_type| exception_type == ACTION_TIMEOUT_TYPE_ID)
    }
}

/// Generates the wrapper function for one policy-annotated action call site
/// and returns its id.
///
/// The wrapper takes the action's keyword arguments as positional inputs in
/// call-site order and forwards them to the wrapped action.
///
/// The policy brackets are an order-insensitive flat bag with the legacy
/// runner semantics: all timeout brackets collapse into one per-attempt
/// duration by minimum over nonzero seconds (zero-second brackets are
/// ignored), and retry brackets aggregate by matching the raised exception
/// against each bracket in descending `max_retries` order against a shared
/// attempt counter.
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
    let mut retry_plans: Vec<RetryPlan> = Vec::new();
    for policy in policies {
        match policy {
            waymark_vm_ast_old::PolicyBracket::Retry(retry_policy) => {
                retry_plans.push(RetryPlan::digest(retry_policy));
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
    // Descending budgets with first-match-wins handler dispatch reproduce
    // the legacy max-of-matching-brackets aggregation.
    retry_plans.sort_by_key(|retry_plan| core::cmp::Reverse(retry_plan.max_retries));

    let mut emitter = FunctionEmitter::<Spec>::new();
    let mut local_frame = LocalFrame::new();

    // Inputs occupy the first registers of the frame in call order.
    let arg_registers: Vec<_> = (0..kwarg_count)
        .map(|_| local_frame.allocate_register())
        .collect();

    if retry_plans.is_empty() {
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
    } else {
        emit_retry_loop::<Spec, Lowering>(
            &mut emitter,
            &mut local_frame,
            action_name,
            action_ref,
            arg_registers,
            timeout_seconds,
            &retry_plans,
        )?;
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
    emit_action_timeout_raise::<Spec, Lowering>(emitter, local_frame)?;

    Ok(())
}

/// Emits the construction and raise of the `ActionTimeout` exception.
fn emit_action_timeout_raise<Spec, Lowering>(
    emitter: &mut FunctionEmitter<Spec>,
    local_frame: &mut LocalFrame,
) -> Result<(), ErrorFor<Spec, Lowering>>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
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

/// Emits the retrying wrapper body: an attempt loop with the retry brackets
/// as exception handlers around each attempt, and the optional per-attempt
/// timeout raced inside the protected region.
///
/// A shared attempt counter is checked against each bracket's budget at the
/// bracket's own handler, and each bracket sleeps its own fixed backoff -
/// outside the protected region, so a failure during backoff propagates
/// instead of counting as an attempt. The timeout select arm routes at
/// compile time: into the retry bookkeeping of the first bracket statically
/// listing `ActionTimeout`, or straight to the raise - after popping the
/// attempt's handler block, so the routing never re-enters the handlers at
/// runtime.
fn emit_retry_loop<Spec, Lowering>(
    emitter: &mut FunctionEmitter<Spec>,
    local_frame: &mut LocalFrame,
    action_name: &str,
    action_ref: <Spec as waymark_vm_instructions_extcallset::Spec>::ActionRef,
    arg_registers: Vec<RegisterId>,
    timeout_seconds: Option<u64>,
    retry_plans: &[RetryPlan],
) -> Result<(), ErrorFor<Spec, Lowering>>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    let used_register = local_frame.allocate_register();
    let one_register = local_frame.allocate_register();
    let exception_register = local_frame.allocate_register();
    let promise_register = Marked::mark(local_frame.allocate_register());

    // Attempt counter setup; the counter and the increment constant live
    // across all attempts.
    let zero_value = Lowering::lower_literal(&Literal::Int(0)).map_err(Error::LiteralLowering)?;
    emitter.emit_load_const(used_register, zero_value);
    let one_value = Lowering::lower_literal(&Literal::Int(1)).map_err(Error::LiteralLowering)?;
    emitter.emit_load_const(one_register, one_value);

    let attempt_state = emitter.reserve_state();
    emitter.emit_jump(attempt_state);
    emitter.switch_to(attempt_state);

    // One handler and one retry-bookkeeping state per bracket.
    let handler_states: Vec<_> = retry_plans
        .iter()
        .map(|_| emitter.reserve_state())
        .collect();
    let retry_states: Vec<_> = retry_plans
        .iter()
        .map(|_| emitter.reserve_state())
        .collect();

    let handlers = retry_plans
        .iter()
        .zip(handler_states.iter().copied())
        .map(
            |(retry_plan, handler_state)| waymark_vm_exception_handler::ExceptionHandler {
                handler_state,
                exception_types: retry_plan.exception_types.clone(),
                exception_dst: Some(exception_register),
            },
        )
        .collect();
    emitter.emit_push_exception_handlers(handlers);

    let resume_after_call = emitter.reserve_state();
    emitter.emit_extcall(
        promise_register,
        action_ref,
        arg_registers,
        resume_after_call,
    );
    emitter.switch_to(resume_after_call);

    let ok_state = emitter.reserve_state();
    let max_register;
    let cond_register;

    match timeout_seconds {
        None => {
            let result_register = local_frame.allocate_register();
            max_register = local_frame.allocate_register();
            cond_register = local_frame.allocate_register();

            emitter.emit_await(result_register, promise_register, ok_state);

            emitter.switch_to(ok_state);
            emitter.emit_unwind(0);
            emitter.emit_return(result_register);
        }
        Some(seconds) => {
            let seconds_literal =
                i64::try_from(seconds).map_err(|_| Error::TimeoutDurationOutOfRange {
                    action_name: action_name.to_owned(),
                    seconds,
                })?;
            let duration_value = Lowering::lower_literal(&Literal::Int(seconds_literal))
                .map_err(Error::LiteralLowering)?;
            let duration_register = local_frame.allocate_register();
            emitter.emit_load_const(duration_register, duration_value);

            let sleep_register = Marked::mark(local_frame.allocate_register());
            let resume_after_sleep = emitter.reserve_state();
            emitter.emit_sleep(sleep_register, duration_register, resume_after_sleep, true);
            emitter.switch_to(resume_after_sleep);

            let result_register = local_frame.allocate_register();
            let sleep_result_register = local_frame.allocate_register();
            max_register = local_frame.allocate_register();
            cond_register = local_frame.allocate_register();
            let timeout_state = emitter.reserve_state();
            emitter.emit_select(vec![
                SelectArm {
                    src: *promise_register,
                    dst: result_register,
                    resume: ok_state,
                },
                SelectArm {
                    src: *sleep_register,
                    dst: sleep_result_register,
                    resume: timeout_state,
                },
            ]);

            emitter.switch_to(ok_state);
            emitter.emit_unwind(0);
            emitter.emit_return(result_register);

            // The timeout arm resumed normally, so the attempt's handler
            // block is still active - pop it first: the compile-time routing
            // below must never be caught by the attempt's own handlers.
            emitter.switch_to(timeout_state);
            emitter.emit_unwind(0);
            let routed_retry = retry_plans.iter().position(RetryPlan::retries_timeouts);
            match routed_retry {
                Some(position) => {
                    let max_value = Lowering::lower_literal(&Literal::Int(i64::from(
                        retry_plans[position].max_retries,
                    )))
                    .map_err(Error::LiteralLowering)?;
                    emitter.emit_load_const(max_register, max_value);
                    emitter.emit_binary(
                        BinaryOpKind::Lt,
                        cond_register,
                        used_register,
                        max_register,
                    );
                    emitter.emit_jump_if(retry_states[position], cond_register);
                    emit_action_timeout_raise::<Spec, Lowering>(emitter, local_frame)?;
                }
                None => {
                    emit_action_timeout_raise::<Spec, Lowering>(emitter, local_frame)?;
                }
            }
        }
    }

    // Per-bracket handlers: check the shared counter against this bracket's
    // budget; retry or re-raise the caught exception on exhaustion. The
    // handler block was already popped by the raise transfer.
    let mut backoff_registers: Option<(RegisterId, Marked<RegisterId, PromiseMarker>, RegisterId)> =
        None;
    for (retry_plan, (handler_state, retry_state)) in retry_plans
        .iter()
        .zip(handler_states.into_iter().zip(retry_states))
    {
        emitter.switch_to(handler_state);
        let max_value = Lowering::lower_literal(&Literal::Int(i64::from(retry_plan.max_retries)))
            .map_err(Error::LiteralLowering)?;
        emitter.emit_load_const(max_register, max_value);
        emitter.emit_binary(BinaryOpKind::Lt, cond_register, used_register, max_register);
        emitter.emit_jump_if(retry_state, cond_register);
        emitter.emit_raise(exception_register);

        emitter.switch_to(retry_state);
        emitter.emit_binary(
            BinaryOpKind::Add,
            used_register,
            used_register,
            one_register,
        );
        if let Some(seconds) = retry_plan.backoff_seconds {
            let seconds_literal =
                i64::try_from(seconds).map_err(|_| Error::BackoffDurationOutOfRange {
                    action_name: action_name.to_owned(),
                    seconds,
                })?;
            let backoff_value = Lowering::lower_literal(&Literal::Int(seconds_literal))
                .map_err(Error::LiteralLowering)?;
            let (backoff_duration, backoff_promise, backoff_result) = *backoff_registers
                .get_or_insert_with(|| {
                    (
                        local_frame.allocate_register(),
                        Marked::mark(local_frame.allocate_register()),
                        local_frame.allocate_register(),
                    )
                });
            emitter.emit_load_const(backoff_duration, backoff_value);
            let resume_after_backoff = emitter.reserve_state();
            emitter.emit_sleep(
                backoff_promise,
                backoff_duration,
                resume_after_backoff,
                false,
            );
            emitter.switch_to(resume_after_backoff);
            let resume_after_backoff_await = emitter.reserve_state();
            emitter.emit_await(backoff_result, backoff_promise, resume_after_backoff_await);
            emitter.switch_to(resume_after_backoff_await);
        }
        emitter.emit_jump(attempt_state);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use waymark_vm_ast_old::{DurationLiteral, PolicyBracket, RetryPolicy, TimeoutPolicy};
    use waymark_vm_bytecode_core::FunctionId;
    use waymark_vm_compiler_for_ast_old_test_support::{TestActionRef, TestLowering, TestSpec};

    use super::super::Error;
    use super::create;
    use crate::function::extras::ExtraFunctions;

    fn timeout_policy(seconds: u64) -> PolicyBracket {
        PolicyBracket::Timeout(TimeoutPolicy {
            timeout: DurationLiteral { seconds },
        })
    }

    fn retry_policy(
        max_retries: u32,
        exception_types: Vec<&str>,
        backoff_seconds: Option<u64>,
    ) -> PolicyBracket {
        PolicyBracket::Retry(RetryPolicy {
            exception_types: exception_types.into_iter().map(ToOwned::to_owned).collect(),
            max_retries,
            backoff: backoff_seconds.map(|seconds| DurationLiteral { seconds }),
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
    fn generates_the_retrying_wrapper_body() {
        insta::assert_snapshot!(
            display_wrapper(1, &[retry_policy(2, Vec::new(), None)]),
            @r#"
        s0:
          PureSet(LoadConst { dst: r1, value: Int(0) })
          PureSet(LoadConst { dst: r2, value: Int(1) })
          CoreSet(Jump { target_state: s1 })
        s1:
          CoreSet(PushExceptionHandlers { handlers: [ExceptionHandler { handler_state: s2, exception_types: [], exception_dst: Some(r3) }] })
          ExtCallSet(ActionCall { dst: r4, action_ref: TestActionRef("notify"), args: [r0], resume: s4 })
        s2:
          PureSet(LoadConst { dst: r6, value: Int(2) })
          PureSet(Binary { kind: Lt, op: BinaryOp { dst: r7, a: r1, b: r6 } })
          CoreSet(JumpIf { target_state: s3, cond: r7 })
          CoreSet(Raise { src: r3 })
        s3:
          PureSet(Binary { kind: Add, op: BinaryOp { dst: r1, a: r1, b: r2 } })
          CoreSet(Jump { target_state: s1 })
        s4:
          CoreSet(Await { dst: r5, src: r4, resume: s5 })
        s5:
          CoreSet(Unwind { depth: 0 })
          CoreSet(Return { src: r5 })
        "#
        );
    }

    #[test]
    fn retries_sleep_their_backoff_between_attempts() {
        insta::assert_snapshot!(
            display_wrapper(0, &[retry_policy(2, Vec::new(), Some(5))]),
            @r#"
        s0:
          PureSet(LoadConst { dst: r0, value: Int(0) })
          PureSet(LoadConst { dst: r1, value: Int(1) })
          CoreSet(Jump { target_state: s1 })
        s1:
          CoreSet(PushExceptionHandlers { handlers: [ExceptionHandler { handler_state: s2, exception_types: [], exception_dst: Some(r2) }] })
          ExtCallSet(ActionCall { dst: r3, action_ref: TestActionRef("notify"), args: [], resume: s4 })
        s2:
          PureSet(LoadConst { dst: r5, value: Int(2) })
          PureSet(Binary { kind: Lt, op: BinaryOp { dst: r6, a: r0, b: r5 } })
          CoreSet(JumpIf { target_state: s3, cond: r6 })
          CoreSet(Raise { src: r2 })
        s3:
          PureSet(Binary { kind: Add, op: BinaryOp { dst: r0, a: r0, b: r1 } })
          PureSet(LoadConst { dst: r7, value: Int(5) })
          ExtCallSet(Sleep { dst: r8, duration: r7, resume: s6, unskippable: false })
        s4:
          CoreSet(Await { dst: r4, src: r3, resume: s5 })
        s5:
          CoreSet(Unwind { depth: 0 })
          CoreSet(Return { src: r4 })
        s6:
          CoreSet(Await { dst: r9, src: r8, resume: s7 })
        s7:
          CoreSet(Jump { target_state: s1 })
        "#
        );
    }

    #[test]
    fn orders_brackets_by_descending_budget_with_first_match_dispatch() {
        insta::assert_snapshot!(
            display_wrapper(
                0,
                &[
                    retry_policy(1, vec!["ValueError"], None),
                    retry_policy(3, Vec::new(), None),
                ],
            ),
            @r#"
        s0:
          PureSet(LoadConst { dst: r0, value: Int(0) })
          PureSet(LoadConst { dst: r1, value: Int(1) })
          CoreSet(Jump { target_state: s1 })
        s1:
          CoreSet(PushExceptionHandlers { handlers: [ExceptionHandler { handler_state: s2, exception_types: [], exception_dst: Some(r2) }, ExceptionHandler { handler_state: s3, exception_types: ["ValueError"], exception_dst: Some(r2) }] })
          ExtCallSet(ActionCall { dst: r3, action_ref: TestActionRef("notify"), args: [], resume: s6 })
        s2:
          PureSet(LoadConst { dst: r5, value: Int(3) })
          PureSet(Binary { kind: Lt, op: BinaryOp { dst: r6, a: r0, b: r5 } })
          CoreSet(JumpIf { target_state: s4, cond: r6 })
          CoreSet(Raise { src: r2 })
        s3:
          PureSet(LoadConst { dst: r5, value: Int(1) })
          PureSet(Binary { kind: Lt, op: BinaryOp { dst: r6, a: r0, b: r5 } })
          CoreSet(JumpIf { target_state: s5, cond: r6 })
          CoreSet(Raise { src: r2 })
        s4:
          PureSet(Binary { kind: Add, op: BinaryOp { dst: r0, a: r0, b: r1 } })
          CoreSet(Jump { target_state: s1 })
        s5:
          PureSet(Binary { kind: Add, op: BinaryOp { dst: r0, a: r0, b: r1 } })
          CoreSet(Jump { target_state: s1 })
        s6:
          CoreSet(Await { dst: r4, src: r3, resume: s7 })
        s7:
          CoreSet(Unwind { depth: 0 })
          CoreSet(Return { src: r4 })
        "#
        );
    }

    #[test]
    fn normalizes_the_exception_catch_all_like_try_except() {
        insta::assert_snapshot!(
            display_wrapper(0, &[retry_policy(1, vec!["Exception"], None)]),
            @r#"
        s0:
          PureSet(LoadConst { dst: r0, value: Int(0) })
          PureSet(LoadConst { dst: r1, value: Int(1) })
          CoreSet(Jump { target_state: s1 })
        s1:
          CoreSet(PushExceptionHandlers { handlers: [ExceptionHandler { handler_state: s2, exception_types: [], exception_dst: Some(r2) }] })
          ExtCallSet(ActionCall { dst: r3, action_ref: TestActionRef("notify"), args: [], resume: s4 })
        s2:
          PureSet(LoadConst { dst: r5, value: Int(1) })
          PureSet(Binary { kind: Lt, op: BinaryOp { dst: r6, a: r0, b: r5 } })
          CoreSet(JumpIf { target_state: s3, cond: r6 })
          CoreSet(Raise { src: r2 })
        s3:
          PureSet(Binary { kind: Add, op: BinaryOp { dst: r0, a: r0, b: r1 } })
          CoreSet(Jump { target_state: s1 })
        s4:
          CoreSet(Await { dst: r4, src: r3, resume: s5 })
        s5:
          CoreSet(Unwind { depth: 0 })
          CoreSet(Return { src: r4 })
        "#
        );
    }

    #[test]
    fn does_not_route_timeouts_into_catch_all_retries() {
        insta::assert_snapshot!(
            display_wrapper(0, &[retry_policy(2, Vec::new(), None), timeout_policy(30)]),
            @r#"
        s0:
          PureSet(LoadConst { dst: r0, value: Int(0) })
          PureSet(LoadConst { dst: r1, value: Int(1) })
          CoreSet(Jump { target_state: s1 })
        s1:
          CoreSet(PushExceptionHandlers { handlers: [ExceptionHandler { handler_state: s2, exception_types: [], exception_dst: Some(r2) }] })
          ExtCallSet(ActionCall { dst: r3, action_ref: TestActionRef("notify"), args: [], resume: s4 })
        s2:
          PureSet(LoadConst { dst: r8, value: Int(2) })
          PureSet(Binary { kind: Lt, op: BinaryOp { dst: r9, a: r0, b: r8 } })
          CoreSet(JumpIf { target_state: s3, cond: r9 })
          CoreSet(Raise { src: r2 })
        s3:
          PureSet(Binary { kind: Add, op: BinaryOp { dst: r0, a: r0, b: r1 } })
          CoreSet(Jump { target_state: s1 })
        s4:
          PureSet(LoadConst { dst: r4, value: Int(30) })
          ExtCallSet(Sleep { dst: r5, duration: r4, resume: s6, unskippable: true })
        s5:
          CoreSet(Unwind { depth: 0 })
          CoreSet(Return { src: r6 })
        s6:
          CoreSet(Select { arms: [SelectArm { src: r3, dst: r6, resume: s5 }, SelectArm { src: r5, dst: r7, resume: s7 }] })
        s7:
          CoreSet(Unwind { depth: 0 })
          PureSet(LoadConst { dst: r10, value: String("ActionTimeout") })
          PureSet(LoadConst { dst: r11, value: None })
          PureSet(MakeException { dst: r12, type_id: r10, details: r11 })
          CoreSet(Raise { src: r12 })
        "#
        );
    }

    #[test]
    fn routes_timeouts_into_retries_on_explicit_opt_in() {
        insta::assert_snapshot!(
            display_wrapper(
                0,
                &[
                    retry_policy(2, vec!["ActionTimeout"], None),
                    timeout_policy(30),
                ],
            ),
            @r#"
        s0:
          PureSet(LoadConst { dst: r0, value: Int(0) })
          PureSet(LoadConst { dst: r1, value: Int(1) })
          CoreSet(Jump { target_state: s1 })
        s1:
          CoreSet(PushExceptionHandlers { handlers: [ExceptionHandler { handler_state: s2, exception_types: ["ActionTimeout"], exception_dst: Some(r2) }] })
          ExtCallSet(ActionCall { dst: r3, action_ref: TestActionRef("notify"), args: [], resume: s4 })
        s2:
          PureSet(LoadConst { dst: r8, value: Int(2) })
          PureSet(Binary { kind: Lt, op: BinaryOp { dst: r9, a: r0, b: r8 } })
          CoreSet(JumpIf { target_state: s3, cond: r9 })
          CoreSet(Raise { src: r2 })
        s3:
          PureSet(Binary { kind: Add, op: BinaryOp { dst: r0, a: r0, b: r1 } })
          CoreSet(Jump { target_state: s1 })
        s4:
          PureSet(LoadConst { dst: r4, value: Int(30) })
          ExtCallSet(Sleep { dst: r5, duration: r4, resume: s6, unskippable: true })
        s5:
          CoreSet(Unwind { depth: 0 })
          CoreSet(Return { src: r6 })
        s6:
          CoreSet(Select { arms: [SelectArm { src: r3, dst: r6, resume: s5 }, SelectArm { src: r5, dst: r7, resume: s7 }] })
        s7:
          CoreSet(Unwind { depth: 0 })
          PureSet(LoadConst { dst: r8, value: Int(2) })
          PureSet(Binary { kind: Lt, op: BinaryOp { dst: r9, a: r0, b: r8 } })
          CoreSet(JumpIf { target_state: s3, cond: r9 })
          PureSet(LoadConst { dst: r10, value: String("ActionTimeout") })
          PureSet(LoadConst { dst: r11, value: None })
          PureSet(MakeException { dst: r12, type_id: r10, details: r11 })
          CoreSet(Raise { src: r12 })
        "#
        );
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
    fn rejects_out_of_range_backoff_durations() {
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(0);

        let error = create::<TestSpec, TestLowering>(
            &mut extra_fns,
            "notify",
            TestActionRef("notify".to_owned()),
            0,
            &[retry_policy(1, Vec::new(), Some(u64::MAX))],
        )
        .expect_err("an unrepresentable backoff should fail");
        assert!(matches!(
            error,
            Error::BackoffDurationOutOfRange { action_name, seconds }
                if action_name == "notify" && seconds == u64::MAX
        ));
    }
}

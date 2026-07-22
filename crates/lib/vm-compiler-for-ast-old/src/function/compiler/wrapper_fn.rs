//! Ephemeral wrapper-function generation for policy-annotated action calls.
//!
//! Policy brackets (retry/timeout) lower inside a dedicated per-call-site
//! function that wraps the plain action call. Every call site - sequential
//! or fan-out - then invokes the wrapper through an ordinary function call,
//! so the policy machinery has exactly one lowering path and the wrapper
//! frame owns the argument registers for the whole policy lifetime.

use waymark_vm_bytecode_core::FunctionId;

use crate::Marked;
use crate::function::extras::ExtraFunctions;

use super::Error;
use super::bytecode::emitter::FunctionEmitter;
use super::env::LocalFrame;
use super::plan::Unsupported;

/// Generates the wrapper function for one policy-annotated action call site
/// and returns its id.
///
/// The wrapper takes the action's keyword arguments as positional inputs in
/// call-site order and forwards them to the wrapped action.
pub fn create<Spec, LiteralLoweringError, ActionLoweringError>(
    extra_fns: &mut ExtraFunctions<Spec>,
    action_name: &str,
    action_ref: <Spec as waymark_vm_instructions_extcallset::Spec>::ActionRef,
    kwarg_count: usize,
    policies: &[waymark_vm_ast_old::PolicyBracket],
) -> Result<FunctionId, Error<LiteralLoweringError, ActionLoweringError>>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    if let Some(policy) = policies.first() {
        return Err(match policy {
            waymark_vm_ast_old::PolicyBracket::Retry(_) => Unsupported::RetryPolicy {
                action_name: action_name.to_owned(),
            }
            .into(),
            waymark_vm_ast_old::PolicyBracket::Timeout(_) => Unsupported::TimeoutPolicy {
                action_name: action_name.to_owned(),
            }
            .into(),
        });
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

    let result_register = local_frame.allocate_register();
    let resume_after_await = emitter.reserve_state();
    emitter.emit_await(result_register, promise_register, resume_after_await);
    emitter.switch_to(resume_after_await);
    emitter.emit_return(result_register);

    let function = waymark_vm_bytecode::Function {
        states: emitter.finish(),
        num_regs: local_frame.num_registers(),
    };

    Ok(extra_fns.insert(function))
}

#[cfg(test)]
mod tests {
    use waymark_vm_ast_old::{DurationLiteral, PolicyBracket, RetryPolicy, TimeoutPolicy};
    use waymark_vm_bytecode_core::{FunctionId, StateId};
    use waymark_vm_compiler_for_ast_old_test_support::{TestActionRef, TestSpec};
    use waymark_vm_instructions_coreset::CoreSet;
    use waymark_vm_instructions_extcallset::ExtCallSet;
    use waymark_vm_instructions_fullset::FullSet as InstructionSet;
    use waymark_vm_runtime_core::RegisterId;

    use super::super::{Error, Unsupported};
    use super::create;
    use crate::function::extras::ExtraFunctions;

    #[test]
    fn generates_the_plain_action_wrapper_body() {
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(3);

        let function_id = create::<_, (), ()>(
            &mut extra_fns,
            "notify",
            TestActionRef("notify".to_owned()),
            2,
            &[],
        )
        .expect("a wrapper without brackets should compile");
        assert_eq!(function_id, FunctionId(3));

        let functions = extra_fns.finish();
        assert_eq!(functions.len(), 1);
        let function = &functions[0];
        assert_eq!(function.num_regs, 4);

        let mut start_instructions = function.states[StateId(0)].instructions.iter();
        assert!(matches!(
            start_instructions.next(),
            Some(InstructionSet::ExtCallSet(ExtCallSet::ActionCall {
                dst,
                action_ref: TestActionRef(action_ref),
                args,
                resume,
            })) if *dst == RegisterId(2)
                && action_ref == "notify"
                && *args == vec![RegisterId(0), RegisterId(1)]
                && *resume == StateId(1)
        ));
        assert!(start_instructions.next().is_none());

        let mut await_instructions = function.states[StateId(1)].instructions.iter();
        assert!(matches!(
            await_instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Await { dst, src, resume }))
                if *dst == RegisterId(3) && *src == RegisterId(2) && *resume == StateId(2)
        ));
        assert!(await_instructions.next().is_none());

        let mut return_instructions = function.states[StateId(2)].instructions.iter();
        assert!(matches!(
            return_instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Return { src })) if *src == RegisterId(3)
        ));
        assert!(return_instructions.next().is_none());
    }

    #[test]
    fn assigns_sequential_ids_after_the_source_functions() {
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(2);

        let first = create::<_, (), ()>(
            &mut extra_fns,
            "first",
            TestActionRef("first".to_owned()),
            0,
            &[],
        )
        .expect("first wrapper should compile");
        let second = create::<_, (), ()>(
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
    fn rejects_policy_brackets_pending_their_lowering() {
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(0);

        let retry_error = create::<_, (), ()>(
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

        let timeout_error = create::<_, (), ()>(
            &mut extra_fns,
            "notify",
            TestActionRef("notify".to_owned()),
            0,
            &[PolicyBracket::Timeout(TimeoutPolicy {
                timeout: DurationLiteral { seconds: 30 },
            })],
        )
        .expect_err("timeout policies should not lower yet");
        assert!(matches!(
            timeout_error,
            Error::Unsupported(Unsupported::TimeoutPolicy { action_name })
                if action_name == "notify"
        ));

        assert!(extra_fns.finish().is_empty());
    }
}

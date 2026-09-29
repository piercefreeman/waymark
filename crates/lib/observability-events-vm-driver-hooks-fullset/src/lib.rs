//! The full instruction set's effects, summarized for the VM driver's
//! observability events.

#![warn(missing_docs)]

use std::marker::PhantomData;

/// Summarizes the full instruction set's effects, over any value.
///
/// The value is only pinned, never held; the summary never carries one.
pub struct FullSetEffectSummarizer<Value>(PhantomData<fn() -> Value>);

/// The full instruction set's effect over `Value`.
type Effect<Value> = waymark_vm_interpreter_fullset::Effect<
    waymark_vm_interpreter_coreset::Effect<Value>,
    waymark_vm_interpreter_extcallset::Effect<waymark_action_core::ActionRef, Value>,
    core::convert::Infallible,
>;

impl<Value> waymark_observability_events_vm_driver_hooks_core::SummarizeEffect
    for FullSetEffectSummarizer<Value>
{
    type Effect = Effect<Value>;
    type Summary = waymark_observability_events_payload::vm_driver::EffectSummary;

    fn summarize_effect(effect: &Self::Effect) -> Self::Summary {
        match effect {
            waymark_vm_interpreter_fullset::Effect::CoreSet(
                waymark_vm_interpreter_coreset::Effect::Complete(_),
            ) => waymark_observability_events_payload::vm_driver::EffectSummary::Complete,
            waymark_vm_interpreter_fullset::Effect::CoreSet(
                waymark_vm_interpreter_coreset::Effect::UnhandledException(exception),
            ) => {
                waymark_observability_events_payload::vm_driver::EffectSummary::UnhandledException {
                    exception_type: exception.type_id.clone(),
                }
            }
            waymark_vm_interpreter_fullset::Effect::ExtCallSet(
                waymark_vm_interpreter_extcallset::Effect::ActionCall {
                    promise_state_id,
                    action_ref,
                    args: _,
                },
            ) => waymark_observability_events_payload::vm_driver::EffectSummary::ActionCall {
                promise_state_id: *promise_state_id,
                action_name: action_ref.action_name.clone(),
                module_name: action_ref.module_name.clone(),
            },
            waymark_vm_interpreter_fullset::Effect::ExtCallSet(
                waymark_vm_interpreter_extcallset::Effect::Sleep {
                    promise_state_id,
                    duration,
                    skip_allowed,
                },
            ) => waymark_observability_events_payload::vm_driver::EffectSummary::Sleep {
                promise_state_id: *promise_state_id,
                duration: duration.get(),
                skip_allowed: *skip_allowed,
            },
            waymark_vm_interpreter_fullset::Effect::PureSet(effect) => match *effect {},
        }
    }
}

#[cfg(test)]
mod tests;

use waymark_vm_runtime_promise_core::PromiseStateId;

use super::FullSetEffectSummarizer;

/// The raised exception of the tests: a class name, captured as-is.
#[derive(Debug)]
struct TestException(&'static str);

impl waymark_observability_vm_value_display::CaptureRaisedException for TestException {
    fn capture_raised_exception(
        &self,
    ) -> waymark_observability_vm_value_display::RaisedExceptionSummary {
        waymark_observability_vm_value_display::RaisedExceptionSummary {
            exception_type: self.0.to_owned(),
        }
    }
}

type Effect = waymark_vm_interpreter_fullset::Effect<
    waymark_vm_interpreter_coreset::Effect<u8>,
    waymark_vm_interpreter_extcallset::Effect<waymark_action_core::ActionRef, u8>,
    core::convert::Infallible,
    waymark_vm_interpreter_excset::Effect<TestException>,
>;

fn summarize(effect: &Effect) -> serde_json::Value {
    let summary = <FullSetEffectSummarizer<u8, TestException> as waymark_observability_events_vm_driver_hooks_core::SummarizeEffect>::summarize_effect(effect);
    serde_json::to_value(summary).expect("serialize")
}

/// Every effect summarizes by variant; the values never reach the summary,
/// and of a raised exception only its captured type does.
#[test]
fn effects_summarize_by_variant() {
    assert_eq!(
        summarize(&waymark_vm_interpreter_fullset::Effect::CoreSet(
            waymark_vm_interpreter_coreset::Effect::Complete(7),
        )),
        serde_json::json!({ "kind": "complete" })
    );
    assert_eq!(
        summarize(&waymark_vm_interpreter_fullset::Effect::ExcSet(
            waymark_vm_interpreter_excset::Effect::UnhandledException(TestException("ValueError")),
        )),
        serde_json::json!({ "kind": "unhandled_exception", "exception_type": "ValueError" })
    );
    assert_eq!(
        summarize(&waymark_vm_interpreter_fullset::Effect::ExtCallSet(
            waymark_vm_interpreter_extcallset::Effect::ActionCall {
                promise_state_id: PromiseStateId(3),
                action_ref: waymark_action_core::ActionRef {
                    action_name: "fetch".to_owned(),
                    module_name: Some("app.tasks".to_owned()),
                    call_args: vec!["url".to_owned()],
                },
                args: vec![7],
            },
        )),
        serde_json::json!({
            "kind": "action_call", "promise_state_id": 3, "action_name": "fetch", "module_name": "app.tasks"
        })
    );
    assert_eq!(
        summarize(&waymark_vm_interpreter_fullset::Effect::ExtCallSet(
            waymark_vm_interpreter_extcallset::Effect::Sleep {
                promise_state_id: PromiseStateId(4),
                duration: waymark_nonzero_duration::NonZeroDuration::from_millis(1500)
                    .expect("non-zero"),
                skip_allowed: false,
            },
        )),
        serde_json::json!({
            "kind": "sleep", "promise_state_id": 4, "duration": { "secs": 1, "nanos": 500_000_000 }, "skip_allowed": false
        })
    );
}

//! A Python value reports every promise state it holds a handle to.

use waymark_vm_runtime_promise_core::{PromiseStateId, TracePromiseStateIds};
use waymark_vm_value_python::{Exception, MaterialValue, Value};

fn traced(value: &impl TracePromiseStateIds) -> Vec<PromiseStateId> {
    let mut ids = Vec::new();
    value.trace_promise_state_ids(&mut |id| ids.push(id));
    ids
}

#[test]
fn nested_pending_values_are_traced_once_each() {
    let mut states =
        waymark_vm_runtime_core::PromiseStates::<(), (), (), core::convert::Infallible>::new();
    let in_list = states.prepare();
    let in_dict = states.prepare();
    let in_exception = states.prepare();
    let expected = vec![in_list.id(), in_dict.id(), in_exception.id()];

    let exception = Exception {
        type_id: "ValueError".to_owned(),
        mro_type_ids: vec!["Exception".to_owned()],
        details: Value::Pending(in_exception),
    };
    let value = Value::Ready(MaterialValue::List(vec![
        Value::Pending(in_list),
        Value::Ready(MaterialValue::Dict(
            [("key".to_owned(), Value::Pending(in_dict))]
                .into_iter()
                .collect(),
        )),
        Value::Ready(MaterialValue::Exception(Box::new(exception))),
        Value::Ready(MaterialValue::Int(1)),
    ]));

    assert_eq!(traced(&value), expected);
}

#[test]
fn ready_values_hold_no_promise_states() {
    let value = Value::Ready(MaterialValue::List(vec![
        Value::Ready(MaterialValue::Int(1)),
        Value::Ready(MaterialValue::String("text".to_owned())),
    ]));

    assert!(traced(&value).is_empty());
}

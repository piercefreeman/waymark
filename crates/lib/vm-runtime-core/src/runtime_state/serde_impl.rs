//! Writing a runtime state and reading it back.
//!
//! Both directions come from one declaration, [`RuntimeStateShape`]. The
//! runtime state is read inside a promise state handle registry scope of
//! its own, so every handle and promise states table entry of one promise
//! state shares one owners counter, and every entry is built once.

use std::collections::VecDeque;

use waymark_vm_runtime_effect::EffectNumber;

use super::RuntimeState;
use crate::{Frame, PromiseStates, SelectStates};

/// The serde shape of a [`RuntimeState`]: its fields, in the order both
/// directions use.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(
    remote = "RuntimeState",
    rename = "RuntimeState",
    bound(
        serialize = "
            FunctionId: serde::Serialize,
            StateId: serde::Serialize,
            Value: serde::Serialize,
            RaisedException: serde::Serialize,
            waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: serde::Serialize,
        ",
        deserialize = "
            FunctionId: serde::Deserialize<'de>,
            StateId: serde::Deserialize<'de>,
            Value: serde::Deserialize<'de>,
            RaisedException: serde::Deserialize<'de>,
            waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: serde::Deserialize<'de>,
        ",
    )
)]
struct RuntimeStateShape<FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    /// [`RuntimeState::ready`].
    ready: VecDeque<Frame<FunctionId, StateId, Value, RaisedException>>,

    /// [`RuntimeState::promise_states`], read with the registry's counters.
    #[serde(deserialize_with = "crate::promise_states::deserialize_promise_states")]
    promise_states: PromiseStates<FunctionId, StateId, Value, RaisedException>,

    /// [`RuntimeState::select_states`].
    select_states: SelectStates<FunctionId, StateId, Value, RaisedException>,

    /// [`RuntimeState::effect_counter`].
    effect_counter: EffectNumber,
}

impl<FunctionId, StateId, Value, RaisedException> serde::Serialize
    for RuntimeState<FunctionId, StateId, Value, RaisedException>
where
    FunctionId: serde::Serialize,
    StateId: serde::Serialize,
    Value: serde::Serialize,
    RaisedException: serde::Serialize,
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
    waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: serde::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        RuntimeStateShape::serialize(self, serializer)
    }
}

impl<'de, FunctionId, StateId, Value, RaisedException> serde::Deserialize<'de>
    for RuntimeState<FunctionId, StateId, Value, RaisedException>
where
    FunctionId: serde::Deserialize<'de>,
    StateId: serde::Deserialize<'de>,
    Value: serde::Deserialize<'de>,
    RaisedException: serde::Deserialize<'de>,
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
    waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: serde::Deserialize<'de>,
{
    /// The snapshot stores promise state handles by id alone; reading it
    /// back gives every handle and promise states table entry of one
    /// promise state a share of the same owners counter again.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _registry_scope =
            crate::promise_state_handle_registry::PromiseStateHandleRegistryScope::enter()
                .map_err(<D::Error as serde::de::Error>::custom)?;

        RuntimeStateShape::deserialize(deserializer)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use waymark_vm_runtime_effect::EffectNumber;
    use waymark_vm_runtime_promise_core::PromiseStateId;

    use crate::test_helpers::RestoreTestException;
    use crate::{
        ExceptionHandlers, Frame, FrameKind, PromiseStateHandle, PromiseStates, RegisterId,
        Registers, RuntimeState, SelectStates,
    };

    /// A value that can hold a promise state handle.
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    enum RestoreTestValue {
        Int(i32),
        Pending(PromiseStateHandle),
    }

    impl waymark_vm_runtime_promise_core::TracePromiseStateIds for RestoreTestValue {
        fn trace_promise_state_ids(&self, visit: &mut dyn FnMut(PromiseStateId)) {
            match self {
                Self::Int(_) => {}
                Self::Pending(handle) => visit(handle.id()),
            }
        }
    }

    type RestoreTestState = RuntimeState<u32, u32, RestoreTestValue, RestoreTestException>;

    /// A runtime state whose one frame references a settled promise state,
    /// next to a settled promise state nothing references; with the ids of
    /// the referenced and the not referenced state.
    fn state_with_referenced_and_not_referenced()
    -> (RestoreTestState, PromiseStateId, PromiseStateId) {
        let mut promise_states = PromiseStates::new();
        let referenced = promise_states.prepare();
        let not_referenced = promise_states.prepare();
        let referenced_id = referenced.id();
        let not_referenced_id = not_referenced.id();
        drop(not_referenced);
        promise_states
            .resolve(referenced_id, RestoreTestValue::Int(1))
            .expect("waiting promise resolves");
        promise_states
            .resolve(not_referenced_id, RestoreTestValue::Int(2))
            .expect("waiting promise resolves");

        let mut regs = Registers::new(1);
        regs.set(RegisterId(0), RestoreTestValue::Pending(referenced));
        let state: RestoreTestState = RuntimeState {
            ready: VecDeque::from([Frame {
                func: 0,
                state: 0,
                regs,
                exception: None,
                exception_handler_blocks: ExceptionHandlers::new(),
                kind: FrameKind::TopLevel,
            }]),
            promise_states,
            select_states: SelectStates::new(),
            effect_counter: EffectNumber(0),
        };

        (state, referenced_id, not_referenced_id)
    }

    /// Reads `bytes` back and checks that the frame's handle and the promise
    /// states table share the referenced state's count.
    fn assert_restored_shares_counts(
        bytes: &[u8],
        referenced_id: PromiseStateId,
        not_referenced_id: PromiseStateId,
    ) {
        let mut restored: RestoreTestState =
            serde::Deserialize::deserialize(&mut rmp_serde::Deserializer::new(bytes))
                .expect("the state decodes");

        restored.promise_states.sweep();
        assert!(restored.promise_states.get(not_referenced_id).is_err());
        let referenced_by_frame = restored.ready[0]
            .regs
            .get(RegisterId(0))
            .and_then(|value| match value {
                RestoreTestValue::Pending(handle) => Some(handle),
                RestoreTestValue::Int(_) => None,
            })
            .expect("the frame keeps its handle");
        assert!(
            restored
                .promise_states
                .by_handle(referenced_by_frame)
                .is_ok()
        );

        restored.ready.clear();
        restored.promise_states.sweep();
        assert!(restored.promise_states.get(referenced_id).is_err());
    }

    #[test]
    fn a_runtime_state_read_as_a_map_shares_its_handle_counts() {
        let (state, referenced_id, not_referenced_id) = state_with_referenced_and_not_referenced();

        let mut bytes = Vec::new();
        let mut serializer = rmp_serde::Serializer::new(&mut bytes).with_struct_map();
        serde::Serialize::serialize(&state, &mut serializer).expect("the state encodes");
        drop(state);
        assert_eq!(bytes[0] & 0xf0, 0x80, "the state is written as a map");

        assert_restored_shares_counts(&bytes, referenced_id, not_referenced_id);
    }

    #[test]
    fn a_runtime_state_read_as_a_sequence_shares_its_handle_counts() {
        let (state, referenced_id, not_referenced_id) = state_with_referenced_and_not_referenced();

        let bytes = rmp_serde::to_vec(&state).expect("the state encodes");
        drop(state);
        assert_eq!(bytes[0] & 0xf0, 0x90, "the state is written as a sequence");

        assert_restored_shares_counts(&bytes, referenced_id, not_referenced_id);
    }
}

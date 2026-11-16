//! Reading a promise states table back.
//!
//! The table and its entries have no `Deserialize` of their own: only
//! [`deserialize_promise_states`] reads them.

use waymark_vm_runtime_promise_core::PromiseStateId;

use super::{PromiseStates, PromiseStatesEntriesMap, PromiseStatesEntry};
use crate::PromiseState;

/// The promise states table's type parameters, held by the entry reader.
type StateTypes<FunctionId, StateId, Value, RaisedException> =
    core::marker::PhantomData<fn() -> (FunctionId, StateId, Value, RaisedException)>;

/// Reads a promise states table [`PromiseStates`]'s `Serialize` wrote, each
/// entry built once with a share of the owners counter the restored handles
/// of its state share.
///
/// Takes the counters from the promise state handle registry, so it fails
/// with `NotInScope` outside a registry scope.
pub(crate) fn deserialize_promise_states<'de, D, FunctionId, StateId, Value, RaisedException>(
    deserializer: D,
) -> Result<PromiseStates<FunctionId, StateId, Value, RaisedException>, D::Error>
where
    D: serde::Deserializer<'de>,
    PromiseState<FunctionId, StateId, Value, RaisedException>: serde::Deserialize<'de>,
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    PromiseStatesShape::deserialize(deserializer)
}

/// The serde shape of a [`PromiseStates`] read back.
#[derive(serde::Deserialize)]
#[serde(
    remote = "PromiseStates",
    rename = "PromiseStates",
    bound(deserialize = "
        PromiseState<FunctionId, StateId, Value, RaisedException>: serde::Deserialize<'de>,
    ")
)]
struct PromiseStatesShape<FunctionId, StateId, Value, RaisedException>(
    #[serde(deserialize_with = "deserialize_entries")]
    PromiseStatesEntriesMap<FunctionId, StateId, Value, RaisedException>,
)
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern;

/// Reads the entries map of a promise states table, each entry through the
/// seed for its id.
fn deserialize_entries<'de, D, FunctionId, StateId, Value, RaisedException>(
    deserializer: D,
) -> Result<PromiseStatesEntriesMap<FunctionId, StateId, Value, RaisedException>, D::Error>
where
    D: serde::Deserializer<'de>,
    PromiseState<FunctionId, StateId, Value, RaisedException>: serde::Deserialize<'de>,
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    waymark_id_map::IdMap::deserialize_with(deserializer, |id| PromiseStatesEntrySeed {
        id,
        phantom: core::marker::PhantomData,
    })
}

/// Reads one promise states table entry: its state, and a share of the owners
/// counter of the state under `id`.
struct PromiseStatesEntrySeed<FunctionId, StateId, Value, RaisedException> {
    /// The id the entry is stored under.
    id: PromiseStateId,

    /// The entry's types.
    phantom: StateTypes<FunctionId, StateId, Value, RaisedException>,
}

impl<'de, FunctionId, StateId, Value, RaisedException> serde::de::DeserializeSeed<'de>
    for PromiseStatesEntrySeed<FunctionId, StateId, Value, RaisedException>
where
    PromiseState<FunctionId, StateId, Value, RaisedException>: serde::Deserialize<'de>,
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    type Value = PromiseStatesEntry<FunctionId, StateId, Value, RaisedException>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let state: PromiseState<FunctionId, StateId, Value, RaisedException> =
            serde::Deserialize::deserialize(deserializer)?;

        let owners_counter = crate::promise_state_handle_registry::owners_counter(self.id)
            .map_err(<D::Error as serde::de::Error>::custom)?;

        Ok(PromiseStatesEntry {
            state,
            owners_counter,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::deserialize_promise_states;
    use crate::promise_state_handle_registry::PromiseStateHandleRegistryScope;
    use crate::test_helpers::{RestoreTestException, TestNumber};
    use crate::{PromiseStateHandle, PromiseStates};

    type RestoreTestStates = PromiseStates<u32, u32, TestNumber, RestoreTestException>;

    #[test]
    fn restored_handles_share_their_state_count() {
        let mut states = RestoreTestStates::new();
        let referenced = states.prepare();
        let not_referenced = states.prepare();
        let referenced_id = referenced.id();
        let not_referenced_id = not_referenced.id();
        drop(not_referenced);
        states
            .resolve(referenced_id, TestNumber(1))
            .expect("waiting promise resolves");
        states
            .resolve(not_referenced_id, TestNumber(2))
            .expect("waiting promise resolves");

        let states_bytes = rmp_serde::to_vec(&states).expect("the table encodes");
        let handles_bytes = rmp_serde::to_vec(&vec![referenced]).expect("the handles encode");
        drop(states);

        let (mut restored, handles): (RestoreTestStates, Vec<PromiseStateHandle>) = {
            let _registry_scope =
                PromiseStateHandleRegistryScope::enter().expect("registry enters scope");
            let restored =
                deserialize_promise_states(&mut rmp_serde::Deserializer::new(&states_bytes[..]))
                    .expect("the table decodes");
            let handles = rmp_serde::from_slice(&handles_bytes).expect("the handles decode");
            (restored, handles)
        };

        restored.sweep();
        assert!(restored.get(not_referenced_id).is_err());
        assert!(restored.by_handle(&handles[0]).is_ok());

        drop(handles);
        restored.sweep();
        assert!(restored.get(referenced_id).is_err());
    }

    /// Hands a newtype struct its inner value as a one-element sequence, the
    /// way some formats write a newtype; reads nothing else.
    struct NewtypeAsSeq<D>(D);

    impl<'de, D> serde::Deserializer<'de> for NewtypeAsSeq<D>
    where
        D: serde::Deserializer<'de>,
    {
        type Error = D::Error;

        fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            Err(serde::de::Error::custom("only a newtype struct reads here"))
        }

        fn deserialize_newtype_struct<V>(
            self,
            _name: &'static str,
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_seq(OneElement(Some(self.0)))
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
            bytes byte_buf option unit unit_struct seq tuple tuple_struct map
            struct enum identifier ignored_any
        }
    }

    /// A sequence of the one element its deserializer reads.
    struct OneElement<D>(Option<D>);

    impl<'de, D> serde::de::SeqAccess<'de> for OneElement<D>
    where
        D: serde::Deserializer<'de>,
    {
        type Error = D::Error;

        fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
        where
            T: serde::de::DeserializeSeed<'de>,
        {
            self.0
                .take()
                .map(|deserializer| seed.deserialize(deserializer))
                .transpose()
        }
    }

    #[test]
    fn a_table_read_as_a_one_element_sequence_shares_its_handle_counts() {
        let mut states = RestoreTestStates::new();
        let referenced = states.prepare();
        let referenced_id = referenced.id();
        states
            .resolve(referenced_id, TestNumber(1))
            .expect("waiting promise resolves");

        let entries_bytes = rmp_serde::to_vec(&states.0).expect("the entries encode");
        let handles_bytes = rmp_serde::to_vec(&vec![referenced]).expect("the handles encode");
        drop(states);

        let (mut restored, handles): (RestoreTestStates, Vec<PromiseStateHandle>) = {
            let _registry_scope =
                PromiseStateHandleRegistryScope::enter().expect("registry enters scope");
            let restored = deserialize_promise_states(NewtypeAsSeq(
                &mut rmp_serde::Deserializer::new(&entries_bytes[..]),
            ))
            .expect("the table decodes");
            let handles = rmp_serde::from_slice(&handles_bytes).expect("the handles decode");
            (restored, handles)
        };

        restored.sweep();
        assert!(restored.by_handle(&handles[0]).is_ok());

        drop(handles);
        restored.sweep();
        assert!(restored.get(referenced_id).is_err());
    }
}

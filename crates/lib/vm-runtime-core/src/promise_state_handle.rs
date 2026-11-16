use waymark_atomic_owners_counter::AtomicOwnersCounter;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// A value's hold on a promise state: the state's id, and one owner of the
/// count the promise states table keeps for that state.
#[derive(Debug)]
pub struct PromiseStateHandle {
    /// The promise state this handle holds.
    id: PromiseStateId,

    /// This handle's owner of the state's count.
    owners_counter: AtomicOwnersCounter,
}

impl PromiseStateHandle {
    /// A handle for the state `id`, owning a share of `owners_counter`.
    pub(crate) fn new(id: PromiseStateId, owners_counter: AtomicOwnersCounter) -> Self {
        Self { id, owners_counter }
    }

    /// The id of the promise state this handle holds.
    pub fn id(&self) -> PromiseStateId {
        self.id
    }
}

impl Clone for PromiseStateHandle {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            owners_counter: self.owners_counter.clone(),
        }
    }
}

impl PartialEq for PromiseStateHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for PromiseStateHandle {}

impl core::hash::Hash for PromiseStateHandle {
    fn hash<H>(&self, state: &mut H)
    where
        H: core::hash::Hasher,
    {
        self.id.hash(state);
    }
}

/// A handle is stored as its promise state id alone.
#[cfg(feature = "serde")]
impl serde::Serialize for PromiseStateHandle {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.id, serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for PromiseStateHandle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let id = <PromiseStateId as serde::Deserialize>::deserialize(deserializer)?;
        Ok(Self::new(id, AtomicOwnersCounter::new()))
    }
}

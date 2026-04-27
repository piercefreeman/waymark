use const_sub_array::SubArray as _;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct DeterminismChain {
    next_counter_value: usize,
}

#[must_use]
#[repr(transparent)]
#[derive(Debug)]
pub struct DeterminismToken {
    counter_value: usize,
}

impl DeterminismChain {
    pub fn step(&mut self) -> DeterminismToken {
        let counter_value = self.next_counter_value;

        // It's not clear how we'd handle an overflow at the counter, so
        // we use `strict_add` and panic in case we do overflow.
        self.next_counter_value = self.next_counter_value.strict_add(1);

        DeterminismToken { counter_value }
    }
}

impl DeterminismToken {
    pub fn into_id(self, instance_id: waymark_ids::InstanceId) -> waymark_ids::ExecutionId {
        // We hand-roll the UUIDv5 hashing here to avoid mem allocations
        // that are forced by the `Uuid::new_v5` API.

        // We use the same debendency that [`uuid`] crate uses (at the time
        // of writing this code).
        let mut hasher = sha1_smol::Sha1::new();

        // Namespace.
        hasher.update(instance_id.as_ref().get().as_bytes());

        // Counter.
        hasher.update(&self.counter_value.to_le_bytes());

        let all_bytes = hasher.digest().bytes();
        let used_bytes = *all_bytes.sub_array_ref::<0, 16>();

        // SAFETY: we are constructing a UUID from SHA1-bytes - it is guaranteed
        // to have at least the variant bit set to non-zero; thus the whole
        // UUID will also be non-nil.
        let id = unsafe {
            let id = uuid::Builder::from_sha1_bytes(used_bytes).into_uuid();
            uuid::NonNilUuid::new_unchecked(id)
        };

        id.into()
    }
}

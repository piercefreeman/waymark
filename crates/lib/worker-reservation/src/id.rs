slotmap::new_key_type! {
    /// Opaque identifier for a pending worker reservation.
    ///
    /// This is typically created by [`crate::Registry::reserve`]. Converting an
    /// arbitrary integer into an `Id` is supported for deserialization and tests,
    /// but not every raw integer corresponds to a previously issued reservation.
    pub struct Id;
}

impl core::fmt::Display for Id {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0.as_ffi())
    }
}

impl core::str::FromStr for Id {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ffi: u64 = s.parse()?;
        Ok(Self::from(ffi))
    }
}

impl From<u64> for Id {
    fn from(value: u64) -> Self {
        let key_data = slotmap::KeyData::from_ffi(value);
        Self::from(key_data)
    }
}

impl From<Id> for u64 {
    fn from(value: Id) -> Self {
        value.0.as_ffi()
    }
}

#[cfg(test)]
mod tests {
    use super::Id;

    #[test]
    fn id_roundtrips_through_u64() {
        let sample: Id = Id::default();

        let encoded: u64 = sample.into();
        let decoded: Id = encoded.into();

        assert_eq!(sample, decoded);
    }

    #[test]
    fn id_roundtrips_through_string() {
        let sample: Id = Id::default();

        let encoded = sample.to_string();
        let decoded: Id = encoded.parse().unwrap();

        assert_eq!(sample, decoded);
    }

    #[test]
    fn invalid_id_string_fails_to_parse() {
        assert!("not-a-number".parse::<Id>().is_err());
    }

    #[test]
    fn note_that_constructing_id_manually_does_not_hold_usual_structural_invariant() {
        // 42 is a value we just came up with, it's not a valid ID.
        let id = Id::from(42);

        // The actual representation of the `id` is not 42.
        assert_ne!(u64::from(id), 42);
    }
}

pub use rmp_serde::decode::Error as DecodeError;
pub use rmp_serde::encode::Error as EncodeError;

pub(crate) fn serialize<T>(value: &T) -> Result<Vec<u8>, EncodeError>
where
    T: serde::Serialize,
{
    rmp_serde::to_vec_named(value)
}

pub(crate) fn deserialize<T>(payload: &[u8]) -> Result<T, DecodeError>
where
    T: serde::de::DeserializeOwned,
{
    rmp_serde::from_slice(payload)
}

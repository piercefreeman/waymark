/// The zero-based sequence number of an effect emitted by the runtime.
///
/// Each time the VM runtime produces an effect, the counter is
/// incremented. This number is embedded in [`super::EmittedEffect`] so
/// consumers can track ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct EffectNumber(pub usize);

impl std::fmt::Display for EffectNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

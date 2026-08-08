/// Capture a copy of a value.
///
/// Typically implemented via [`Clone`].
pub trait CaptureCopy<Value> {
    /// Capture a copy of a value.
    fn capture_copy(value: &Value) -> Value;
}

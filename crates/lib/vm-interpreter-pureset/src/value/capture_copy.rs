/// Capture a copy of a value.
///
/// Typically implemented via [`Clone`].
pub trait CaptureCopy: Sized {
    /// Capture a copy of a value.
    fn capture_copy(&self) -> Self;
}

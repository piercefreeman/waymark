//! Runtime view capture.

#![warn(missing_docs)]

/// Capture a runtime view from an exclusive borrow of a source view.
///
/// Implemented by a runtime view for every source view it can be captured
/// from. The runtime or an enclosing interpreter uses this to produce
/// the (typically reduced) view an interpreter actually needs, from
/// whatever view the caller holds.
///
/// Capturing borrows the source exclusively for as long as the captured
/// view lives. Keep the borrow lifetime independent of the source view's
/// own lifetime so one source can serve repeated captures.
pub trait CaptureRuntimeView<'source, SourceView>: Sized {
    /// Capture this runtime view from the source view.
    fn capture_runtime_view(source: &'source mut SourceView) -> Self;
}

/// The unit runtime view captures from any source: an interpreter that
/// needs no view at all can be driven from whatever view the caller holds.
impl<'source, SourceView> CaptureRuntimeView<'source, SourceView> for () {
    fn capture_runtime_view(_source: &'source mut SourceView) -> Self {}
}

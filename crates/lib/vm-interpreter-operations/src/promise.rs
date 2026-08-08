//! Promise-level machinery shared by the per-set promise-leveled
//! implementations.

/// An error from an operation invoked at the promise level of a value.
///
/// The provided promise-level implementations of the vocabulary traits
/// require the value to be ready and delegate to the ready-level
/// implementation. This error captures the two ways that can fail,
/// keeping the ready-level error spaces free of promise concerns.
#[derive(Debug, thiserror::Error)]
pub enum MaybeUnresolvedError<InnerError> {
    /// The value is an unresolved promise.
    #[error("the value is an unresolved promise")]
    Unresolved(#[source] waymark_vm_runtime_promise_core::UnresolvedPromiseError),

    /// The value was ready and the ready-level operation failed.
    #[error(transparent)]
    Ready(InnerError),
}

#[cfg(test)]
mod tests {
    use super::MaybeUnresolvedError;

    #[derive(Debug, PartialEq, Eq)]
    struct TestError;

    impl core::fmt::Display for TestError {
        fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            write!(formatter, "ready-level failure")
        }
    }

    impl core::error::Error for TestError {}

    #[test]
    fn unresolved_arm_reports_and_sources_the_promise_error() {
        let error: MaybeUnresolvedError<TestError> = MaybeUnresolvedError::Unresolved(
            waymark_vm_runtime_promise_core::UnresolvedPromiseError {
                promise_state_id: waymark_vm_runtime_promise_core::PromiseStateId(7),
            },
        );

        assert_eq!(error.to_string(), "the value is an unresolved promise");

        let source = core::error::Error::source(&error).expect("the promise error is the source");
        let source = source
            .downcast_ref::<waymark_vm_runtime_promise_core::UnresolvedPromiseError>()
            .expect("the source is the promise error");
        assert_eq!(
            source.promise_state_id,
            waymark_vm_runtime_promise_core::PromiseStateId(7)
        );
    }

    #[test]
    fn ready_arm_is_transparent() {
        let error: MaybeUnresolvedError<TestError> = MaybeUnresolvedError::Ready(TestError);

        assert_eq!(error.to_string(), "ready-level failure");
        assert!(core::error::Error::source(&error).is_none());
    }
}

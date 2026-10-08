//! Shared helpers for this crate's tests.

/// A raised exception for the tests: a class name, matched by equality.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TestException(pub(crate) &'static str);

impl waymark_vm_runtime_exception::HasMatchPattern for TestException {
    type Pattern = &'static str;
}

impl waymark_vm_runtime_exception::Match for TestException {
    fn matches(&self, pattern: &Self::Pattern) -> bool {
        self.0 == *pattern
    }
}

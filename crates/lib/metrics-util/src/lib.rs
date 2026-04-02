//! Utilities for [`metrics`] crate.

pub struct Val<T>(pub T);

impl metrics::IntoF64 for Val<usize> {
    fn into_f64(self) -> f64 {
        self.0 as _
    }
}

//! Shared construction helpers for the batcher test modules.

use std::num::NonZeroUsize;
use std::sync::Arc;

use nonempty_collections::NEVec;
use waymark_nonzero_duration::NonZeroDuration;

pub fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("non-zero")
}

pub fn secs(value: u64) -> NonZeroDuration {
    NonZeroDuration::from_secs(value).expect("non-zero")
}

pub fn millis(value: u64) -> NonZeroDuration {
    NonZeroDuration::from_millis(value).expect("non-zero")
}

/// A flush that records each batch's size and maps every input through `map`.
pub fn recording_flush<In, Out>(
    seen: Arc<std::sync::Mutex<Vec<usize>>>,
    map: impl Fn(In) -> Out + Clone,
) -> impl FnMut(NEVec<In>) -> std::future::Ready<NEVec<Out>> + Clone {
    move |batch: NEVec<In>| {
        seen.lock().expect("lock").push(batch.len().get());
        let outputs: Vec<Out> = batch.into_iter().map(map.clone()).collect();
        std::future::ready(NEVec::try_from_vec(outputs).expect("batch was non-empty"))
    }
}

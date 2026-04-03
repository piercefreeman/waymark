use std::future::Future;
use std::pin::Pin;

pub trait TimedFutureExt {
    type Future: Future;

    fn timed(self, histogram: metrics::Histogram) -> Timed<Self::Future>;
}

pub struct Timed<T> {
    inner: T,
    start: Option<std::time::Instant>,
    histogram: metrics::Histogram,
}

impl<T: Future> Future for Timed<T> {
    type Output = T::Output;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // Safe because we never move `inner` after it has been pinned; we only
        // mutate non-pinned fields (`start`) and create a pinned projection to
        // `inner` before polling.
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        if this.start.is_none() {
            this.start = Some(std::time::Instant::now());
        }

        // Project `Pin<&mut Timed<T>>` to `Pin<&mut T>` without moving `inner`.
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll(cx)
    }
}

impl<T> Drop for Timed<T> {
    fn drop(&mut self) {
        let Some(start) = self.start else {
            return;
        };
        self.histogram.record(start.elapsed());
    }
}

impl<T> Timed<T> {
    pub fn histogram(this: &Timed<T>) -> &metrics::Histogram {
        &this.histogram
    }

    pub fn start(this: &Timed<T>) -> Option<std::time::Instant> {
        this.start
    }

    pub fn new(future: T, histogram: metrics::Histogram) -> Self {
        Self {
            inner: future,
            start: None,
            histogram,
        }
    }
}

impl<T: IntoFuture> TimedFutureExt for T {
    type Future = T::IntoFuture;

    fn timed(self, histogram: metrics::Histogram) -> Timed<Self::Future> {
        Timed::new(self.into_future(), histogram)
    }
}

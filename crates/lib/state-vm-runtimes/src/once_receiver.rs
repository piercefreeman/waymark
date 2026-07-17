//! A shared, take-once wrapper around a oneshot receiver.

/// A shared wrapper around a [`tokio::sync::oneshot::Receiver`] that
/// hands the received value out exactly once.
pub struct OnceReceiver<T> {
    receiver: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<T>>>,
}

impl<T> OnceReceiver<T> {
    /// Create a new [`OnceReceiver`] wrapping the given `receiver`.
    pub fn new(receiver: tokio::sync::oneshot::Receiver<T>) -> Self {
        Self {
            receiver: tokio::sync::Mutex::new(Some(receiver)),
        }
    }

    /// Receive the value.
    ///
    /// The completion outcome is handed out exactly once, to the first
    /// call to complete: `Some(Ok(_))` is the value, `Some(Err(_))`
    /// means the sender was dropped without sending. Every call after
    /// that returns `None`.
    ///
    /// # Cancellation safety
    ///
    /// This method is cancellation-safe: if the future is dropped
    /// before completing, the value is retained and remains available
    /// to the next caller.
    pub async fn recv(&self) -> Option<Result<T, tokio::sync::oneshot::error::RecvError>> {
        let mut guard = self.receiver.lock().await;
        // Awaiting the receiver by `&mut` (never taking it out of the
        // slot) is what makes cancellation safe: a dropped call leaves
        // the receiver — and the pending value — in place. The slot is
        // cleared only by the poll that observes the completion, which
        // also shields later calls from the receiver's poll-after-
        // complete panic.
        let receiver = guard.as_mut()?;
        let result = receiver.await;
        *guard = None;
        Some(result)
    }
}

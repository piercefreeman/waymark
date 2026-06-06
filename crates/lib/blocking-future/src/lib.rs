//! Facilities for properly executing "blocking futures" -
//! futures that emerge blocking behaviour and therefore unfit for
//! scheduling on the regular reactor thread-pool.

#![warn(missing_docs)]

use std::panic::AssertUnwindSafe;

/// Spawn a future onto a dedicated OS thread.
///
/// The future runs on the current [`tokio::runtime`], but from within
/// a separate thread, so blocking operations won't starve the async worker
/// pool.
///
/// Panics in the future are captured and propagated when the returned
/// [`JoinHandle`] is awaited.
///
/// The executing future can spawn other tasks through normal [`tokio::spawn`]
/// and similar APIs - but those will *not* run on this blocking-safe thread.
///
/// To compose other blocking futures, either do structured concurrency if you
/// need to share a single thread (i.e. combine the futures before spawning
/// them) or make another [`spawn_thread`] call for no thread sharing.
pub fn spawn_thread<F, R>(future: F) -> JoinHandle<R>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    let handle = tokio::runtime::Handle::current();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = std::thread::spawn(move || {
        let result = std::panic::catch_unwind(AssertUnwindSafe(move || handle.block_on(future)));
        let _ = tx.send(result);
    });
    JoinHandle { rx }
}

/// Handle for a blocking future spawned on a dedicated OS thread.
///
/// Awaiting this handle returns the output of the future, or resumes
/// the panic if the future panicked.
pub struct JoinHandle<T> {
    /// Receives the future's output from the spawned thread.
    rx: tokio::sync::oneshot::Receiver<std::thread::Result<T>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = std::thread::Result<T>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let rx = std::pin::pin!(&mut self.rx);
        let val = std::task::ready!(rx.poll(cx));
        let result = match val {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(payload)) => Err(payload),
            Err(_oneshot_gone) => {
                // This should be an impossibility since the thread either panics
                // with an unwind (un which case we'd get the panic payload), or
                // the whole process aborts.
                panic!("JoinHandle polled after the spawned thread sender was dropped")
            }
        };
        std::task::Poll::Ready(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runs_on_dedicated_thread() {
        let test_tid = std::thread::current().id();
        let (tx, rx) = tokio::sync::oneshot::channel();

        let handle = spawn_thread(async move {
            let spawned_tid = std::thread::current().id();
            tx.send(spawned_tid).unwrap();
            42
        });

        let spawned_tid = rx.await.unwrap();
        assert_ne!(
            spawned_tid, test_tid,
            "spawned future should run on a different OS thread"
        );
        assert_eq!(handle.await.unwrap(), 42);
    }
}

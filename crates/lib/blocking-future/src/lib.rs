//! Facilities for properly executing "blocking futures" -
//! futures that emerge blocking behaviour and therefore unfit for
//! scheduling on the regular reactor thread-pool.

#![warn(missing_docs)]

/// Spawn a future onto a dedicated OS thread.
///
/// The future runs on the current [`tokio::runtime`], but from within
/// a separate thread, so blocking operations won't starve the async worker
/// pool.
///
/// The executing future can spawn other tasks through normal [`tokio::spawn`]
/// and similar APIs - but those will *not* run on this blocking-safe thread.
///
/// To compose other blocking futures, either do structured concurrency if you
/// need to share a single thread (i.e. combine the futures before spawning
/// them) or make another [`spawn_thread`] call for no thread sharing.
pub fn spawn_thread<F, R>(future: F) -> tokio::task::JoinHandle<R>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || handle.block_on(future))
}

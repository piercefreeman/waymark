//! The OS's shutdown requests as held listeners, portable across
//! platforms.
//!
//! Installing a kind of request is what takes over the platform's default
//! disposition for it, so a process opts in per kind, and only a receiver
//! delivers what its own `install` registered.
//!
//! A subscription is held for the receiver's lifetime, so a request that
//! lands between two `recv` calls is latched and delivered by the next one,
//! however long the consumer was busy in between. That is the guarantee
//! `tokio::signal::ctrl_c()` lacks: it subscribes anew on every call, and a
//! request broadcast before the new subscription is never seen.
//!
//! Only installing can fail: `install` reports the outcome of registering
//! the listener. Once installed, `recv` is infallible; the listeners live
//! in process statics and never close.
//!
//! # Coalescing
//!
//! Requests are latched, not counted. Requests of one kind that land
//! before the consumer observes one of them produce at least one `recv`
//! completion and never more than the requests; how many of them merge
//! is the runtime's scheduling.
//!
//! # Disposition
//!
//! A handler stays for the process lifetime, as with every tokio signal
//! listener. Dropping a receiver stops observing its requests. On unix the
//! handler keeps swallowing them; on Windows, once the kind's last receiver
//! is gone, the handler declines them and the OS falls through to the
//! default disposition. Requests before `install` keep the platform's
//! default disposition.

#![warn(missing_docs)]

pub mod ctrl_c;
pub mod termination;

/// Error from an `install`.
///
/// The registration of the listener was refused.
#[derive(Debug, thiserror::Error)]
#[error("failed to register the shutdown request listener: {source}")]
pub struct InstallError {
    /// The refusal.
    #[source]
    pub source: std::io::Error,
}

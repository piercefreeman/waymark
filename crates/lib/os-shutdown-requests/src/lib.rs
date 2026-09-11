//! The OS's shutdown requests as held listeners, portable across
//! platforms.
//!
//! Each kind of request is its own module with its own [`install`] and
//! [`Receiver`]: [`ctrl_c`] for the interrupt a person sends from the
//! keyboard, [`termination`] for the stop an orchestrator or the system
//! sends. Installing a kind is what takes over the platform's default
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
//! with the signal driver. Once installed, `recv` is infallible; the
//! listeners live in process statics and never close.
//!
//! # Coalescing
//!
//! Requests are latched, not counted. Any number of requests of one kind
//! that land before the consumer observes the first of them produce one
//! `recv` completion. A consumer that wants a second request must see the
//! second one land after it reacted to the first, which is the natural
//! rhythm of a person pressing again once the first press visibly took
//! effect.
//!
//! # Disposition
//!
//! A handler stays for the process lifetime, as with every tokio signal
//! listener. Dropping a receiver stops observing its requests; it does not
//! restore the default disposition. Requests before `install` keep the
//! platform's default disposition.
//!
//! [`install`]: ctrl_c::install
//! [`Receiver`]: ctrl_c::Receiver

#![warn(missing_docs)]

pub mod ctrl_c;
pub mod termination;

/// Error from an `install`.
///
/// The signal driver refused the registration, for instance because the
/// runtime was built without I/O support.
#[derive(Debug, thiserror::Error)]
#[error("failed to register the shutdown request listener: {source}")]
pub struct InstallError {
    /// The driver's refusal.
    #[source]
    pub source: std::io::Error,
}

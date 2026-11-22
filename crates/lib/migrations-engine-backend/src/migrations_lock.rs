//! What a migrations lock is taken with.

use waymark_nonzero_duration::NonZeroDuration;

/// The id of a migrations lock's holder. The caller of the backend supplies
/// it; a holder keeps its id for as long as it keeps its lock.
#[derive(Debug)]
pub struct HolderId(uuid::Uuid);

impl HolderId {
    /// The holder id with the value `raw`.
    pub const fn from_raw(raw: uuid::Uuid) -> Self {
        Self(raw)
    }

    /// The holder id's value, for storing it.
    pub const fn as_raw(&self) -> &uuid::Uuid {
        &self.0
    }
}

/// When a migrations lock expires, as its holder counts it:
/// `time_to_live` after `now`.
///
/// A backend keeps the expiry on the database's clock alone: it stores the
/// database's own now plus what is left of `time_to_live` since `now`, so
/// the lock does not outlast its holder's count.
#[derive(Debug)]
pub struct Expiry {
    /// The instant the holder counts `time_to_live` from.
    pub now: std::time::Instant,

    /// How long after `now` the lock expires.
    pub time_to_live: NonZeroDuration,
}

#[cfg(test)]
mod tests;

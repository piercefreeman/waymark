use chrono::{DateTime, Utc};

/// Metadata about a running VM instance used to make lifecycle decisions.
///
/// Fields are intentionally lightweight — the driver thread updates them
/// after each effect and persist.
#[derive(Debug, Clone, Default)]
pub struct VmState {
    /// When this VM last executed an instruction and emitted an effect.
    ///
    /// `None` if the VM has never executed.
    pub last_active_at: Option<DateTime<Utc>>,

    /// When this VM was last persisted (snapshotted).
    ///
    /// `None` if the VM has never been persisted.
    pub last_persisted_at: Option<DateTime<Utc>>,

    /// Whether the VM has state changes that have not yet been persisted.
    ///
    /// Set to `true` whenever the VM executes instructions that mutate
    /// its state, and cleared to `false` upon a successful snapshot.
    pub has_unpersisted_changes: bool,
}

impl VmState {
    /// Returns the duration since the VM was last active.
    ///
    /// Returns `None` if the VM has never been active.
    pub fn idle_duration(&self) -> Option<chrono::Duration> {
        self.last_active_at.map(|active_at| Utc::now() - active_at)
    }

    /// Returns the duration since the VM was last persisted.
    ///
    /// Returns `None` if the VM has never been persisted.
    pub fn duration_since_persist(&self) -> Option<chrono::Duration> {
        self.last_persisted_at
            .map(|persisted_at| Utc::now() - persisted_at)
    }
}

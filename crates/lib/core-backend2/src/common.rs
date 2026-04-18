use waymark_ids::{InstanceId, LockId};

use crate::Timestamp;

/// Lock claim settings for owned instances.
#[derive(Clone, Debug)]
pub struct LockClaim {
    /// Lock owner identifier.
    pub lock_id: LockId,

    /// Timestamp at which this claim stops being valid unless refreshed.
    pub expires_at: Timestamp,
}

/// Current lock status for an instance.
#[derive(Clone, Debug)]
pub struct InstanceLockStatus {
    /// Instance whose queue lock status was checked or updated.
    pub instance_id: InstanceId,

    /// Current claim after the operation, or `None` if the instance is unlocked.
    pub claim: Option<LockClaim>,
}

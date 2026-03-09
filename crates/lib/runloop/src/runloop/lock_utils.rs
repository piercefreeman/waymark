use std::collections::HashSet;

use uuid::Uuid;
use waymark_core_backend::InstanceLockStatus;

/// Returns a list of Instance IDs.
pub fn lock_mismatches_for(locks: &[InstanceLockStatus], lock_uuid: Uuid) -> HashSet<Uuid> {
    locks
        .iter()
        .filter(|status| status.lock_uuid != Some(lock_uuid))
        .map(|status| status.instance_id)
        .collect()
}

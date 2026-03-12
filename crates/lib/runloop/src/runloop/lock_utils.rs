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

#[cfg(test)]
mod tests {
    use super::lock_mismatches_for;
    use std::collections::HashSet;

    use chrono::Utc;
    use uuid::Uuid;
    use waymark_core_backend::InstanceLockStatus;

    #[test]
    fn ignores_expired_lock_with_matching_owner() {
        let lock_uuid = Uuid::new_v4();
        let instance_id = Uuid::new_v4();

        let statuses = vec![InstanceLockStatus {
            instance_id,
            lock_uuid: Some(lock_uuid),
            lock_expires_at: Some(Utc::now() - chrono::Duration::seconds(60)),
        }];
        assert!(
            lock_mismatches_for(&statuses, lock_uuid).is_empty(),
            "matching lock UUID should not evict solely due to stale expiry"
        );

        let mismatched = vec![InstanceLockStatus {
            instance_id,
            lock_uuid: Some(Uuid::new_v4()),
            lock_expires_at: Some(Utc::now() + chrono::Duration::seconds(60)),
        }];
        let evict_ids = lock_mismatches_for(&mismatched, lock_uuid);
        assert_eq!(evict_ids, HashSet::from([instance_id]));
    }
}

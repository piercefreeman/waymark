use std::collections::HashSet;

use waymark_core_backend::InstanceLockStatus;
use waymark_ids::{InstanceId, LockId};

/// Returns a list of Instance IDs.
pub fn lock_mismatches_for(locks: &[InstanceLockStatus], lock_uuid: LockId) -> HashSet<InstanceId> {
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
    use waymark_core_backend::InstanceLockStatus;
    use waymark_ids::{InstanceId, LockId};

    #[test]
    fn ignores_expired_lock_with_matching_owner() {
        let lock_uuid = LockId::new_uuid_v4();
        let instance_id = InstanceId::new_uuid_v4();

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
            lock_uuid: Some(LockId::new_uuid_v4()),
            lock_expires_at: Some(Utc::now() + chrono::Duration::seconds(60)),
        }];
        let evict_ids = lock_mismatches_for(&mismatched, lock_uuid);
        assert_eq!(evict_ids, HashSet::from([instance_id]));
    }

    #[test]
    fn all_mismatched_locks_return_all() {
        let lock_uuid = LockId::new_uuid_v4();
        let id1 = InstanceId::new_uuid_v4();
        let id2 = InstanceId::new_uuid_v4();
        let id3 = InstanceId::new_uuid_v4();

        let statuses = vec![
            InstanceLockStatus {
                instance_id: id1,
                lock_uuid: Some(LockId::new_uuid_v4()),
                lock_expires_at: Some(Utc::now() + chrono::Duration::seconds(60)),
            },
            InstanceLockStatus {
                instance_id: id2,
                lock_uuid: Some(LockId::new_uuid_v4()),
                lock_expires_at: Some(Utc::now() + chrono::Duration::seconds(60)),
            },
            InstanceLockStatus {
                instance_id: id3,
                lock_uuid: None,
                lock_expires_at: None,
            },
        ];
        let result = lock_mismatches_for(&statuses, lock_uuid);
        assert_eq!(result, HashSet::from([id1, id2, id3]));
    }

    #[test]
    fn mixed_matching_and_mismatched_locks() {
        let lock_uuid = LockId::new_uuid_v4();
        let matching_id = InstanceId::new_uuid_v4();
        let mismatched_id = InstanceId::new_uuid_v4();
        let none_lock_id = InstanceId::new_uuid_v4();

        let statuses = vec![
            InstanceLockStatus {
                instance_id: matching_id,
                lock_uuid: Some(lock_uuid),
                lock_expires_at: Some(Utc::now() + chrono::Duration::seconds(60)),
            },
            InstanceLockStatus {
                instance_id: mismatched_id,
                lock_uuid: Some(LockId::new_uuid_v4()),
                lock_expires_at: Some(Utc::now() + chrono::Duration::seconds(60)),
            },
            InstanceLockStatus {
                instance_id: none_lock_id,
                lock_uuid: None,
                lock_expires_at: None,
            },
        ];
        let result = lock_mismatches_for(&statuses, lock_uuid);
        // Only the matching one should be excluded
        assert_eq!(result, HashSet::from([mismatched_id, none_lock_id]));
    }
}

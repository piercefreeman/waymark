//! The migrations locks.

use waymark_migrations_engine_backend::migrations_lock;

/// The SQLSTATE code of a row an exclusion constraint refused.
const EXCLUSION_VIOLATION: &str = "23P01";

/// The mode column's value of a shared lock.
const SHARED: &str = "shared";

/// The mode column's value of an exclusive lock.
const EXCLUSIVE: &str = "exclusive";

/// How many microseconds are left of `expiry`'s time to live since its
/// `now`; negative once it has passed. Out of range, the count is the
/// nearest one in range.
pub fn remaining_microseconds(expiry: &migrations_lock::Expiry) -> i64 {
    let time_to_live = i64::try_from(expiry.time_to_live.as_micros()).unwrap_or(i64::MAX);
    let elapsed = i64::try_from(expiry.now.elapsed().as_micros()).unwrap_or(i64::MAX);

    time_to_live.saturating_sub(elapsed)
}

/// Whether a lock was taken.
enum Taking {
    /// The lock's row is there.
    Taken,

    /// The exclusion constraint refused the lock's row.
    Refused,
}

impl<Tables> crate::Backend<Tables>
where
    Tables: crate::Tables,
{
    /// Take a lock of `mode` for `holder_id`, once the locks that have
    /// expired are out of its way.
    async fn take_lock(
        &mut self,
        holder_id: &migrations_lock::HolderId,
        expiry: migrations_lock::Expiry,
        mode: &'static str,
    ) -> Result<Taking, sqlx::Error> {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
DELETE FROM "{locks}"
WHERE expires_at <= clock_timestamp()
            "#,
            locks = Tables::LOCKS,
        )))
        .execute(&mut self.connection)
        .await?;

        // `clock_timestamp()`, not `now()`: the latter is the instant the
        // transaction began, before any wait on a conflicting row.
        let inserted = sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
INSERT INTO "{locks}" (holder_id, mode, expires_at)
VALUES ($1, $2, clock_timestamp() + $3 * interval '1 microsecond')
            "#,
            locks = Tables::LOCKS,
        )))
        .bind(holder_id.as_raw())
        .bind(mode)
        .bind(remaining_microseconds(&expiry))
        .execute(&mut self.connection)
        .await;

        match inserted {
            Ok(_) => Ok(Taking::Taken),
            Err(error) => {
                if crate::sqlstate(&error).as_deref() == Some(EXCLUSION_VIOLATION) {
                    return Ok(Taking::Refused);
                }

                Err(error)
            }
        }
    }
}

impl<Tables> waymark_migrations_engine_backend::TakeSharedMigrationsLock for crate::Backend<Tables>
where
    Tables: crate::Tables,
{
    type Error = sqlx::Error;

    async fn take_shared_migrations_lock(
        &mut self,
        holder_id: &migrations_lock::HolderId,
        expiry: migrations_lock::Expiry,
    ) -> Result<waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome, Self::Error>
    {
        let taking = self.take_lock(holder_id, expiry, SHARED).await?;

        Ok(match taking {
            Taking::Taken => {
                waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome::Taken
            }
            Taking::Refused => {
                waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome::Refused
            }
        })
    }
}

impl<Tables> waymark_migrations_engine_backend::TakeExclusiveMigrationsLock
    for crate::Backend<Tables>
where
    Tables: crate::Tables,
{
    type Error = sqlx::Error;

    async fn take_exclusive_migrations_lock(
        &mut self,
        holder_id: &migrations_lock::HolderId,
        expiry: migrations_lock::Expiry,
    ) -> Result<
        waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome,
        Self::Error,
    > {
        let taking = self.take_lock(holder_id, expiry, EXCLUSIVE).await?;

        Ok(match taking {
            Taking::Taken => {
                waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Taken
            }
            Taking::Refused => {
                waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Refused
            }
        })
    }
}

impl<Tables> waymark_migrations_engine_backend::ConvertMigrationsLockToShared
    for crate::Backend<Tables>
where
    Tables: crate::Tables,
{
    type Error = sqlx::Error;

    async fn convert_migrations_lock_to_shared(
        &mut self,
        holder_id: &migrations_lock::HolderId,
        expiry: migrations_lock::Expiry,
    ) -> Result<
        waymark_migrations_engine_backend::convert_migrations_lock_to_shared::Outcome,
        Self::Error,
    > {
        let converted = sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
UPDATE "{locks}"
SET mode = $2,
    expires_at = clock_timestamp() + $3 * interval '1 microsecond'
WHERE holder_id = $1
    AND mode = $4
            "#,
            locks = Tables::LOCKS,
        )))
        .bind(holder_id.as_raw())
        .bind(SHARED)
        .bind(remaining_microseconds(&expiry))
        .bind(EXCLUSIVE)
        .execute(&mut self.connection)
        .await?;

        if converted.rows_affected() == 0 {
            return Ok(
                waymark_migrations_engine_backend::convert_migrations_lock_to_shared::Outcome::Lost,
            );
        }

        Ok(waymark_migrations_engine_backend::convert_migrations_lock_to_shared::Outcome::Converted)
    }
}

impl<Tables> waymark_migrations_engine_backend::RenewMigrationsLock for crate::Backend<Tables>
where
    Tables: crate::Tables,
{
    type Error = sqlx::Error;

    async fn renew_migrations_lock(
        &mut self,
        holder_id: &migrations_lock::HolderId,
        expiry: migrations_lock::Expiry,
    ) -> Result<waymark_migrations_engine_backend::renew_migrations_lock::Outcome, Self::Error>
    {
        let renewed = sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
UPDATE "{locks}"
SET expires_at = clock_timestamp() + $2 * interval '1 microsecond'
WHERE holder_id = $1
            "#,
            locks = Tables::LOCKS,
        )))
        .bind(holder_id.as_raw())
        .bind(remaining_microseconds(&expiry))
        .execute(&mut self.connection)
        .await?;

        if renewed.rows_affected() == 0 {
            return Ok(waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Lost);
        }

        Ok(waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Renewed)
    }
}

impl<Tables> waymark_migrations_engine_backend::ReleaseMigrationsLock for crate::Backend<Tables>
where
    Tables: crate::Tables,
{
    type Error = sqlx::Error;

    async fn release_migrations_lock(
        &mut self,
        holder_id: &migrations_lock::HolderId,
    ) -> Result<(), Self::Error> {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
DELETE FROM "{locks}"
WHERE holder_id = $1
            "#,
            locks = Tables::LOCKS,
        )))
        .bind(holder_id.as_raw())
        .execute(&mut self.connection)
        .await?;

        Ok(())
    }
}

impl<Tables> waymark_migrations_engine_backend::FindExclusiveMigrationsLock
    for crate::Backend<Tables>
where
    Tables: crate::Tables,
{
    type Error = sqlx::Error;

    async fn find_exclusive_migrations_lock(
        &mut self,
    ) -> Result<
        waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome,
        Self::Error,
    > {
        let found: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            r#"
SELECT EXISTS (
    SELECT 1
    FROM "{locks}"
    WHERE mode = $1
        AND expires_at > clock_timestamp()
)
            "#,
            locks = Tables::LOCKS,
        )))
        .bind(EXCLUSIVE)
        .fetch_one(&mut self.connection)
        .await?;

        if found {
            return Ok(
                waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome::Found,
            );
        }

        Ok(waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome::NotFound)
    }
}

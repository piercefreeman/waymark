//! The comparison of a migrations ledger with a migrations array.

use std::collections::{BTreeMap, BTreeSet};

use waymark_migrations_engine_backend::{MigrationsLedger, migrations_ledger};
use waymark_migrations_engine_migration::MigrationVersion;

/// A migrations array in the order of its versions: each of its
/// migrations has a higher version than the one before it.
///
/// [`validate_migrations_array`] makes one, and the flows take no other
/// form of a migrations array.
#[derive(Debug)]
pub struct ValidMigrationsArray<Migrations> {
    /// The validated array.
    migrations: Migrations,
}

impl<Migrations> ValidMigrationsArray<Migrations> {
    /// The migrations, in the order of their versions.
    pub fn as_slice<Migration>(&self) -> &[Migration]
    where
        Migrations: AsRef<[Migration]>,
    {
        self.migrations.as_ref()
    }
}

/// How a migrations ledger stands against a migrations array it does not
/// conflict with.
#[derive(Debug)]
pub enum MigrationsComparison {
    /// The migrations engine is not set up in the database: none of the
    /// array's migrations is applied.
    NotSetUp,

    /// The first `applied` migrations of the array are applied and the
    /// rest of them are not.
    InOrder {
        /// How many of the array's migrations are applied.
        applied: usize,
    },
}

/// Error returned by [`validate_migrations_array`]: the array is not in
/// the order of its versions.
#[derive(Debug, thiserror::Error)]
#[error(
    "migration {} does not come after migration {} in the migrations array",
    .version.to_raw(),
    .previous.to_raw()
)]
pub struct ArrayOrderError {
    /// The version that is not higher than the one before it.
    pub version: MigrationVersion,

    /// The version before it in the array.
    pub previous: MigrationVersion,
}

/// Error returned by [`compare_migrations`]: the ledger conflicts with the
/// array.
#[derive(Debug, thiserror::Error)]
pub enum ConflictError {
    /// A migration was marked as started and never as finished.
    #[error("migration {} was started and never finished", .version.to_raw())]
    Dirty {
        /// The version of the migration.
        version: MigrationVersion,
    },

    /// The ledger has a migration the array does not.
    #[error(
        "the database has migration {} applied, which this build does not have",
        .version.to_raw()
    )]
    UnknownApplied {
        /// The version of the migration.
        version: MigrationVersion,
    },

    /// A migration was applied at another revision than the array's
    /// migration of that version has.
    #[error(
        "migration {} was applied at revision {applied} and this build has revision {expected}",
        .version.to_raw()
    )]
    RevisionMismatch {
        /// The version of the migration.
        version: MigrationVersion,

        /// The revision the ledger has.
        applied: String,

        /// The revision the array's migration has.
        expected: String,
    },

    /// A migration is applied while one before it in the array is not.
    #[error(
        "migration {} is applied while the earlier migration {} is not",
        .applied.to_raw(),
        .pending.to_raw()
    )]
    AppliedOutOfOrder {
        /// The version of the migration that is not applied.
        pending: MigrationVersion,

        /// The version of the applied migration after it.
        applied: MigrationVersion,
    },
}

/// Check that each of `migrations` has a higher version than the one
/// before it, and give the array back as a valid one.
pub fn validate_migrations_array<Migrations, Migration>(
    migrations: Migrations,
) -> Result<ValidMigrationsArray<Migrations>, ArrayOrderError>
where
    Migrations: AsRef<[Migration]>,
    Migration: waymark_migrations_engine_migration::HasMigrationMetadata,
{
    let mut previous: Option<MigrationVersion> = None;

    for migration in migrations.as_ref() {
        let version = migration.migration_metadata().version;

        if let Some(previous) = previous
            && version <= previous
        {
            return Err(ArrayOrderError { version, previous });
        }

        previous = Some(version);
    }

    Ok(ValidMigrationsArray { migrations })
}

/// Compare `ledger` with `migrations`.
///
/// Of several conflicts the one reported is, in this order: the lowest
/// started migration, the lowest migration the array does not have, then
/// the first conflict met going through the array.
pub fn compare_migrations<Migrations, Migration>(
    ledger: &MigrationsLedger,
    migrations: &ValidMigrationsArray<Migrations>,
) -> Result<MigrationsComparison, ConflictError>
where
    Migrations: AsRef<[Migration]>,
    Migration: waymark_migrations_engine_migration::HasMigrationMetadata,
{
    let migrations = migrations.as_slice();

    let entries = match ledger {
        MigrationsLedger::NotSetUp => return Ok(MigrationsComparison::NotSetUp),
        MigrationsLedger::SetUp(entries) => entries,
    };

    let entries_by_version: BTreeMap<MigrationVersion, &migrations_ledger::Entry> =
        entries.iter().map(|entry| (entry.version, entry)).collect();

    for (version, entry) in &entries_by_version {
        match entry.state {
            migrations_ledger::EntryState::Started => {
                return Err(ConflictError::Dirty { version: *version });
            }
            migrations_ledger::EntryState::Finished => {}
        }
    }

    let array_versions: BTreeSet<MigrationVersion> = migrations
        .iter()
        .map(|migration| migration.migration_metadata().version)
        .collect();

    for version in entries_by_version.keys() {
        if !array_versions.contains(version) {
            return Err(ConflictError::UnknownApplied { version: *version });
        }
    }

    let mut applied = 0;
    let mut first_pending: Option<MigrationVersion> = None;

    for migration in migrations {
        let metadata = migration.migration_metadata();

        let Some(entry) = entries_by_version.get(&metadata.version) else {
            first_pending.get_or_insert(metadata.version);

            continue;
        };

        if &entry.revision != metadata.revision {
            return Err(ConflictError::RevisionMismatch {
                version: metadata.version,
                applied: entry.revision.as_raw().to_owned(),
                expected: metadata.revision.as_raw().to_owned(),
            });
        }

        if let Some(pending) = first_pending {
            return Err(ConflictError::AppliedOutOfOrder {
                pending,
                applied: metadata.version,
            });
        }

        applied += 1;
    }

    Ok(MigrationsComparison::InOrder { applied })
}

#[cfg(test)]
mod tests;

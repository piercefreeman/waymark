//! Types and traits shared by the migration interface.

/// The version of a migration: its place in the order the migrations of
/// one database are applied in.
///
/// Versions compare and order by their numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MigrationVersion(u32);

impl MigrationVersion {
    /// The version with the number `raw`.
    pub const fn from_raw(raw: u32) -> Self {
        Self(raw)
    }

    /// The version's number, for storing it.
    pub const fn to_raw(self) -> u32 {
        self.0
    }
}

/// The revision of a migration: a label its author gives it, and changes
/// when the migration no longer does to a database what it did before.
///
/// Reformatting or rewording the migration's source leaves the revision
/// as it is. It is text for people to read.
///
/// Two revisions are equal when their texts are.
#[derive(Debug, PartialEq, Eq)]
pub struct MigrationRevision(std::borrow::Cow<'static, str>);

impl MigrationRevision {
    /// The revision with the text `raw`.
    pub const fn from_static(raw: &'static str) -> Self {
        Self(std::borrow::Cow::Borrowed(raw))
    }

    /// The revision with the text `raw`, as read from where it is stored.
    pub fn from_raw(raw: String) -> Self {
        Self(std::borrow::Cow::Owned(raw))
    }

    /// The revision's text, for storing it and for showing it.
    pub fn as_raw(&self) -> &str {
        &self.0
    }
}

/// What a migration states about itself.
#[derive(Debug)]
pub struct MigrationMetadata<'a> {
    /// The migration's version.
    pub version: MigrationVersion,

    /// A short description of what the migration does, for people.
    pub description: &'a str,

    /// The migration's revision.
    pub revision: &'a MigrationRevision,

    /// A digest of the migration's source, for people who look at where
    /// an applied migration is recorded. Nothing is decided by it.
    pub source_digest: &'a str,
}

/// Whether a migration can run inside a transaction.
#[derive(Debug)]
pub enum MigrationTransactionSupport {
    /// The migration can run inside a transaction.
    Supported,

    /// The migration cannot run inside a transaction.
    Unsupported,
}

/// Common base: a migration is associated with the connection type it
/// runs through.
pub trait HasConnection {
    /// The connection type the migration runs through.
    type Connection;
}

/// Common base: a migration has metadata.
pub trait HasMigrationMetadata {
    /// The migration's metadata.
    fn migration_metadata(&self) -> MigrationMetadata<'_>;
}

/// Common base: a migration states whether it can run inside a
/// transaction.
pub trait HasMigrationTransactionSupport {
    /// Whether the migration can run inside a transaction.
    fn migration_transaction_support(&self) -> MigrationTransactionSupport;
}

#[cfg(test)]
mod tests;

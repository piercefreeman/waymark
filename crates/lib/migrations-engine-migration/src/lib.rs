//! The migration interface of the migrations engine: the traits a
//! migration implements.

#![warn(missing_docs)]

mod common;

pub mod post_validate_migration;
pub mod pre_validate_migration;
pub mod run_migration;

pub use self::common::*;
pub use self::post_validate_migration::PostValidateMigration;
pub use self::pre_validate_migration::PreValidateMigration;
pub use self::run_migration::RunMigration;

/// A migration.
///
/// Blanket-implemented for any type that implements every trait of the
/// migration interface.
#[waymark_blanket_impl_macros::blanket_impl]
pub trait Migration:
    HasConnection
    + HasMigrationMetadata
    + HasMigrationTransactionSupport
    + PreValidateMigration
    + RunMigration
    + PostValidateMigration
{
}

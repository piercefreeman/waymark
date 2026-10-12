//! A Postgres observability store that is only migrated.

use waymark_secret_string::SecretString;

use waymark_postgres_config::connection;

use crate::profiles;

/// Configuration for a Postgres observability store that is only
/// migrated.
#[derive(Debug, Clone)]
pub struct MigrateOnly {
    /// The connection the migrations run through.
    pub migrations: connection::Config,
}

impl MigrateOnly {
    /// Create config from environment variables, for the store migrated
    /// at `migrations_url`.
    pub fn from_url_and_env(
        migrations_url: SecretString,
    ) -> Result<Self, connection::FromUrlAndEnvError> {
        let migrations =
            connection::Config::from_url_and_env::<profiles::Migrations>(migrations_url)?;

        Ok(Self { migrations })
    }
}

//! The processes the main database's config is defaulted for.

use waymark_postgres_config::pool;
use waymark_secret_string::SecretString;

use crate::profiles;

/// The process the main database's config is defaulted for: it picks the
/// defaults of the read-write pool's variables, which every process reads
/// under the same names.
#[derive(Debug)]
pub enum DefaultsFor {
    /// The executor, the node.
    Executor,

    /// The bridge.
    Bridge,
}

impl DefaultsFor {
    /// The read-write pool's config, for the database at `url`, with the
    /// rest from environment variables, defaulted for this process.
    pub(crate) fn read_write_pool_from_url_and_env(
        self,
        url: SecretString,
    ) -> Result<pool::Config, pool::FromUrlAndEnvError> {
        match self {
            Self::Executor => pool::Config::from_url_and_env::<profiles::ExecutorPool>(url),
            Self::Bridge => pool::Config::from_url_and_env::<profiles::BridgePool>(url),
        }
    }
}

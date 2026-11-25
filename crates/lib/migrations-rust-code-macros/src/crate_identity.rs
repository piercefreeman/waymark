//! The version and the description in the name of a Rust-code migration's
//! crate.

/// What comes before the version in the name of a migration's crate.
const MIGRATION_PART: &str = "-migration-";

/// What the name of a Rust-code migration's crate says.
#[derive(Debug)]
pub struct Parsed {
    /// The migration's version.
    pub version: u32,

    /// The migration's description.
    pub description: String,
}

/// Error returned by [`parse`].
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// The name has no `-migration-` part.
    #[error("the crate name has no -migration- part")]
    NoMigrationPart,

    /// Nothing separates a version from a description after the
    /// `-migration-` part.
    #[error("the crate name is not <prefix>-migration-<version>-<description>")]
    NoSeparator,

    /// What is after the `-migration-` part, up to the next dash, is not
    /// digits alone.
    #[error("the version {version:?} is not made of digits alone")]
    VersionNotDigits {
        /// What is between the `-migration-` part and the next dash.
        version: String,
    },

    /// The digits are a number no migration version can be.
    #[error("the version {version:?} is not a number a migration version can be: {error}")]
    VersionOutOfRange {
        /// What is between the `-migration-` part and the next dash.
        version: String,

        /// Why it is not a version.
        #[source]
        error: std::num::ParseIntError,
    },

    /// Nothing comes after the version's dash.
    #[error("the description is empty")]
    EmptyDescription,
}

/// Read `<prefix>-migration-<version>-<description>`: the version is the
/// digits after the last `-migration-`, and the description is the rest
/// of the name, with its dashes as spaces.
pub fn parse(crate_name: &str) -> Result<Parsed, ParseError> {
    let Some((_prefix, migration)) = crate_name.rsplit_once(MIGRATION_PART) else {
        return Err(ParseError::NoMigrationPart);
    };

    let Some((version, description)) = migration.split_once('-') else {
        return Err(ParseError::NoSeparator);
    };

    if version.is_empty() || !version.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ParseError::VersionNotDigits {
            version: version.to_owned(),
        });
    }

    let parsed_version: u32 = version
        .parse()
        .map_err(|error| ParseError::VersionOutOfRange {
            version: version.to_owned(),
            error,
        })?;

    if description.is_empty() {
        return Err(ParseError::EmptyDescription);
    }

    Ok(Parsed {
        version: parsed_version,
        description: description.replace('-', " "),
    })
}

#[cfg(test)]
mod tests;

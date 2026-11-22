//! The version and the description in the file name of an SQL-file
//! migration.

/// The extension of an SQL file's file name.
const EXTENSION: &str = ".sql";

/// What a file name of an SQL-file migration says.
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
    /// The file name has no `.sql` extension.
    #[error("the file name has no .sql extension")]
    NotAnSqlFile,

    /// The file name has no underscore between a version and a
    /// description.
    #[error("the file name is not <version>_<description>.sql")]
    NoSeparator,

    /// What is before the first underscore is not digits alone.
    #[error("the version {version:?} is not made of digits alone")]
    VersionNotDigits {
        /// What is before the first underscore.
        version: String,
    },

    /// The digits before the first underscore are a number no migration
    /// version can be.
    #[error("the version {version:?} is not a number a migration version can be: {error}")]
    VersionOutOfRange {
        /// What is before the first underscore.
        version: String,

        /// Why it is not a version.
        #[source]
        error: std::num::ParseIntError,
    },

    /// Nothing is between the first underscore and the extension.
    #[error("the description is empty")]
    EmptyDescription,
}

/// Read `<version>_<description>.sql`: the version is the digits before
/// the first underscore, and the description is the rest before the
/// extension, with its underscores as spaces.
pub fn parse(file_name: &str) -> Result<Parsed, ParseError> {
    let Some(stem) = file_name.strip_suffix(EXTENSION) else {
        return Err(ParseError::NotAnSqlFile);
    };

    let Some((version, description)) = stem.split_once('_') else {
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
        description: description.replace('_', " "),
    })
}

#[cfg(test)]
mod tests;

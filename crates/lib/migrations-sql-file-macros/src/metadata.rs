//! The metadata header of an SQL file: its lines from the top down to the
//! first empty one, each an annotation.

/// What every line of the header begins with.
const ANNOTATION_PREFIX: &str = "-- ";

/// What is between an annotation's key and its value.
const VALUE_SEPARATOR: char = ':';

/// The key of the annotation with the migration's revision.
const REVISION: &str = "revision";

/// The key of the annotation of a migration that cannot run inside a
/// transaction.
const NO_TRANSACTION: &str = "no-transaction";

/// What the metadata header of an SQL file says.
#[derive(Debug)]
pub struct Metadata {
    /// The migration's revision.
    pub revision: String,

    /// Whether the migration can run inside a transaction.
    pub runs_inside_a_transaction: bool,
}

/// Error returned by [`parse`].
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// A line of the header is not an annotation.
    #[error(
        "line {line} is in the metadata header and is not an annotation: the header is every \
         line down to the first empty one, each `-- revision: <revision>` or `-- no-transaction`"
    )]
    NotAnAnnotation {
        /// The line's number, from 1.
        line: usize,
    },

    /// A line of the header is an annotation the header cannot have.
    #[error(
        "line {line}: the metadata header has no `{key}` annotation: it is every line down to \
         the first empty one, each `-- revision: <revision>` or `-- no-transaction`"
    )]
    UnknownAnnotation {
        /// The line's number, from 1.
        line: usize,

        /// The annotation's key.
        key: String,
    },

    /// The header has an annotation more than once.
    #[error("line {line}: the metadata header has the `{key}` annotation more than once")]
    RepeatedAnnotation {
        /// The number, from 1, of the line that has it again.
        line: usize,

        /// The annotation's key.
        key: &'static str,
    },

    /// An annotation that has a value is written without one.
    #[error("line {line}: the `{key}` annotation has no value")]
    NoValue {
        /// The line's number, from 1.
        line: usize,

        /// The annotation's key.
        key: &'static str,
    },

    /// An annotation that has no value is written with one.
    #[error("line {line}: the `{key}` annotation takes no value")]
    UnexpectedValue {
        /// The line's number, from 1.
        line: usize,

        /// The annotation's key.
        key: &'static str,
    },

    /// The header has no revision.
    #[error("the metadata header has no `-- revision: <revision>` line")]
    NoRevision,
}

/// An annotation as its line has it: the key, and the value when the line
/// has the separator. Both are without the whitespace around them.
fn annotation(line: &str) -> Option<(&str, Option<&str>)> {
    let annotation = line.strip_prefix(ANNOTATION_PREFIX)?;

    match annotation.split_once(VALUE_SEPARATOR) {
        Some((key, value)) => Some((key.trim(), Some(value.trim()))),
        None => Some((annotation.trim(), None)),
    }
}

/// Read the metadata header of the SQL file with the content `sql`.
///
/// The header is every line from the top down to the first empty one, or
/// to the last line of a file that has no empty one. Each is one of:
///
/// - `-- revision: <revision>`, once: the migration's revision, which is
///   not empty;
/// - `-- no-transaction`, at most once: the migration cannot run inside a
///   transaction.
pub fn parse(sql: &str) -> Result<Metadata, ParseError> {
    let mut revision: Option<String> = None;
    let mut no_transaction = false;

    let header = sql
        .lines()
        .map(str::trim_end)
        .take_while(|line| !line.is_empty());

    for (index, text) in header.enumerate() {
        let line = index + 1;

        let Some((key, value)) = annotation(text) else {
            return Err(ParseError::NotAnAnnotation { line });
        };

        if key == REVISION {
            if revision.is_some() {
                return Err(ParseError::RepeatedAnnotation {
                    line,
                    key: REVISION,
                });
            }

            let Some(value) = value.filter(|value| !value.is_empty()) else {
                return Err(ParseError::NoValue {
                    line,
                    key: REVISION,
                });
            };

            revision = Some(value.to_owned());
        } else if key == NO_TRANSACTION {
            if no_transaction {
                return Err(ParseError::RepeatedAnnotation {
                    line,
                    key: NO_TRANSACTION,
                });
            }

            if value.is_some() {
                return Err(ParseError::UnexpectedValue {
                    line,
                    key: NO_TRANSACTION,
                });
            }

            no_transaction = true;
        } else {
            return Err(ParseError::UnknownAnnotation {
                line,
                key: key.to_owned(),
            });
        }
    }

    let Some(revision) = revision else {
        return Err(ParseError::NoRevision);
    };

    Ok(Metadata {
        revision,
        runs_inside_a_transaction: !no_transaction,
    })
}

#[cfg(test)]
mod tests;

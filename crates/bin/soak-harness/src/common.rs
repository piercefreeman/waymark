use std::{
    collections::VecDeque,
    fs::File,
    io::{BufRead as _, BufReader},
    path::Path,
};

use color_eyre::eyre::WrapErr as _;

pub fn read_tail_lines(
    path: &Path,
    max_lines: usize,
) -> Result<Vec<String>, color_eyre::eyre::Report> {
    let file = File::open(path).wrap_err_with(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut lines = VecDeque::with_capacity(max_lines.max(1));

    for line in reader.lines() {
        let line = line.wrap_err_with(|| format!("read line from {}", path.display()))?;
        if lines.len() == max_lines {
            let _ = lines.pop_front();
        }
        lines.push_back(line);
    }

    Ok(lines.into_iter().collect())
}

const DB_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);

/// One failed attempt to connect a database.
#[derive(Debug)]
pub enum WaitForDatabaseAttemptError<Retry, Stop> {
    /// The database is not there yet: tried again after a delay, until
    /// the deadline.
    Retry(Retry),

    /// Not a readiness problem: the wait ends with this error.
    Stop(Stop),
}

/// Error waiting for a database.
#[derive(Debug, thiserror::Error)]
pub enum WaitForDatabaseError<Retry, Stop> {
    /// The deadline passed; the last attempt's error, when there was one.
    #[error("timed out waiting for {what}")]
    TimedOut {
        /// The database waited for.
        what: String,

        /// What the last attempt failed with.
        #[source]
        last_error: Option<Retry>,
    },

    /// An attempt failed in a way that is not worth retrying.
    #[error(transparent)]
    Stopped(Stop),
}

/// Wait for a database: `attempt_fn` is retried until it connects or
/// `timeout` elapses; `what` names the database in the timeout error.
pub async fn wait_for_database<Connection, Retry, Stop, AttemptFn, AttemptFut>(
    what: &str,
    timeout: std::time::Duration,
    mut attempt_fn: AttemptFn,
) -> Result<Connection, WaitForDatabaseError<Retry, Stop>>
where
    AttemptFn: FnMut() -> AttemptFut,
    AttemptFut: Future<Output = Result<Connection, WaitForDatabaseAttemptError<Retry, Stop>>>,
{
    let deadline = std::time::Instant::now() + timeout;
    let mut last_error: Option<Retry> = None;
    loop {
        if std::time::Instant::now() >= deadline {
            return Err(WaitForDatabaseError::TimedOut {
                what: what.to_owned(),
                last_error,
            });
        }
        match attempt_fn().await {
            Ok(connection) => return Ok(connection),
            Err(WaitForDatabaseAttemptError::Retry(error)) => {
                last_error = Some(error);
                tokio::time::sleep(DB_RETRY_DELAY).await;
            }
            Err(WaitForDatabaseAttemptError::Stop(error)) => {
                return Err(WaitForDatabaseError::Stopped(error));
            }
        }
    }
}

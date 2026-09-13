use std::{
    collections::VecDeque,
    fs::File,
    io::{BufRead as _, BufReader},
    path::Path,
};

use color_eyre::eyre::{WrapErr as _, eyre};

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

/// Runs `future` unless `cancellation_token` is cancelled first. A
/// cancellation, whether it landed before the call or mid-run, drops
/// the future and reports the interrupted `step` as the error.
pub async fn run_unless_cancelled<Output, Future>(
    cancellation_token: &tokio_util::sync::CancellationToken,
    step: &str,
    future: Future,
) -> Result<Output, color_eyre::eyre::Report>
where
    Future: std::future::Future<Output = Result<Output, color_eyre::eyre::Report>>,
{
    match cancellation_token.run_until_cancelled(future).await {
        Some(result) => result,
        None => Err(eyre!("interrupted while {step}")),
    }
}

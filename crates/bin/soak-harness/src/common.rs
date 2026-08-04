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

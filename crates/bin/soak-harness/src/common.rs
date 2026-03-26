use std::{
    collections::VecDeque,
    fs::File,
    io::{BufRead as _, BufReader},
    path::Path,
};

use anyhow::{Context, Result};

pub fn read_tail_lines(path: &Path, max_lines: usize) -> Result<Vec<String>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut lines = VecDeque::with_capacity(max_lines.max(1));

    for line in reader.lines() {
        let line = line.with_context(|| format!("read line from {}", path.display()))?;
        if lines.len() == max_lines {
            let _ = lines.pop_front();
        }
        lines.push_back(line);
    }

    Ok(lines.into_iter().collect())
}

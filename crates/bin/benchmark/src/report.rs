//! Benchmark result reporting.

use std::collections::HashMap;
use std::time::Duration;

pub struct BenchmarkStats {
    pub elapsed: Duration,
    pub query_counts: HashMap<String, usize>,
    pub batch_counts: HashMap<String, HashMap<usize, usize>>,
}

pub fn format_query_counts(counts: HashMap<String, usize>) -> String {
    let mut keys: Vec<_> = counts.keys().cloned().collect();
    keys.sort();
    let mut lines = vec!["Postgres query counts:".to_string()];
    for key in keys {
        let value = counts.get(&key).copied().unwrap_or(0);
        lines.push(format!("  {key}: {value}"));
    }
    lines.join("\n")
}

fn median_from_counts(counts: &HashMap<usize, usize>) -> usize {
    let total: usize = counts.values().sum();
    if total == 0 {
        return 0;
    }
    let threshold = total.div_ceil(2);
    let mut running = 0;
    let mut sizes: Vec<_> = counts.keys().cloned().collect();
    sizes.sort();
    for size in sizes {
        running += counts.get(&size).copied().unwrap_or(0);
        if running >= threshold {
            return size;
        }
    }
    0
}

pub fn format_batch_size_counts(batch_counts: HashMap<String, HashMap<usize, usize>>) -> String {
    let mut keys: Vec<_> = batch_counts.keys().cloned().collect();
    keys.sort();
    let mut lines = vec!["Postgres batch size p50:".to_string()];
    for key in keys {
        if let Some(counts) = batch_counts.get(&key) {
            if counts.is_empty() {
                continue;
            }
            let median = median_from_counts(counts);
            let total: usize = counts.values().sum();
            lines.push(format!("  {key}: p50={median} batches={total}"));
        }
    }
    lines.join("\n")
}

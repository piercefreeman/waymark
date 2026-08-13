//! Benchmark result reporting.

use std::collections::HashMap;
use std::time::Duration;

pub struct BenchmarkStats {
    pub total: usize,
    pub elapsed: Duration,
    pub query_counts: HashMap<String, usize>,
    pub batch_counts: HashMap<String, HashMap<usize, usize>>,
}

pub fn format_query_counts(counts: &HashMap<String, usize>) -> String {
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

pub fn format_batch_size_counts(batch_counts: &HashMap<String, HashMap<usize, usize>>) -> String {
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

pub fn format_json(
    stats: &BenchmarkStats,
    count_per_case: std::num::NonZeroUsize,
    base: i64,
) -> String {
    let elapsed_s = stats.elapsed.as_secs_f64();
    let batch_p50: HashMap<&str, serde_json::Value> = stats
        .batch_counts
        .iter()
        .filter(|(_, counts)| !counts.is_empty())
        .map(|(key, counts)| {
            let total: usize = counts.values().sum();
            (
                key.as_str(),
                serde_json::json!({
                    "p50": median_from_counts(counts),
                    "batches": total,
                }),
            )
        })
        .collect();
    serde_json::json!({
        "count_per_case": count_per_case.get(),
        "base": base,
        "total": stats.total,
        "elapsed_s": elapsed_s,
        "throughput": stats.total as f64 / elapsed_s,
        "query_counts": stats.query_counts,
        "batch_p50": batch_p50,
    })
    .to_string()
}

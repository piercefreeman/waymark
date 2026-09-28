//! In-memory ring buffer for pool-level time-series metrics.
//!
//! Stores up to 1440 entries at the status report interval (~2 hours at the
//! default 5-second tick). ~42 KB per runner.
//! Binary format: `[u32 count][N x 30-byte entries]`

use std::collections::VecDeque;

/// Maximum entries: 24h * 60 min = 1440
const MAX_ENTRIES: usize = 1440;

/// Size of one serialized entry in bytes.
const ENTRY_SIZE: usize = 30;

/// A single time-series data point (30 bytes serialized).
#[derive(Debug, Clone, Copy)]
pub struct TimeSeriesEntry {
    /// Unix timestamp in seconds
    pub timestamp_secs: i64,
    /// Actions per second at this point
    pub actions_per_sec: f32,
    /// Number of active Python worker processes
    pub active_workers: u16,
    /// Average instance duration in seconds
    pub median_instance_duration_secs: f32,
    /// Number of workflow instances currently owned
    pub active_instances: u32,
    /// Dispatch queue depth (actions waiting to be sent to workers)
    pub queue_depth: u32,
    /// Actions currently being executed by workers
    pub in_flight_actions: u32,
}

/// Ring buffer of time-series entries, max 1440 (24 hours at 1-minute resolution).
#[derive(Debug, Clone)]
pub struct PoolTimeSeries {
    entries: VecDeque<TimeSeriesEntry>,
}

impl PoolTimeSeries {
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
        }
    }

    /// Append an entry, dropping the oldest if over capacity.
    pub fn push(&mut self, entry: TimeSeriesEntry) {
        if self.entries.len() >= MAX_ENTRIES {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Encode to binary: `[u32 count][N x 30-byte entries]`
    ///
    /// Each entry is:
    /// - i64 timestamp_secs (8 bytes, little-endian)
    /// - f32 actions_per_sec (4 bytes)
    /// - u16 active_workers (2 bytes)
    /// - f32 median_instance_duration_secs (4 bytes)
    /// - u32 active_instances (4 bytes)
    /// - u32 queue_depth (4 bytes)
    /// - u32 in_flight_actions (4 bytes)
    pub fn encode(&self) -> Vec<u8> {
        let count = self.entries.len() as u32;
        let mut buf = Vec::with_capacity(4 + self.entries.len() * ENTRY_SIZE);
        buf.extend_from_slice(&count.to_le_bytes());
        for entry in &self.entries {
            buf.extend_from_slice(&entry.timestamp_secs.to_le_bytes());
            buf.extend_from_slice(&entry.actions_per_sec.to_le_bytes());
            buf.extend_from_slice(&entry.active_workers.to_le_bytes());
            buf.extend_from_slice(&entry.median_instance_duration_secs.to_le_bytes());
            buf.extend_from_slice(&entry.active_instances.to_le_bytes());
            buf.extend_from_slice(&entry.queue_depth.to_le_bytes());
            buf.extend_from_slice(&entry.in_flight_actions.to_le_bytes());
        }
        buf
    }

    /// Decode from binary. Returns `None` if the bytes are malformed.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 4 {
            return None;
        }
        let count = u32::from_le_bytes(bytes[0..4].try_into().ok()?) as usize;
        if count > MAX_ENTRIES {
            return None;
        }
        let expected_len = 4 + count * ENTRY_SIZE;
        if bytes.len() < expected_len {
            return None;
        }
        let mut entries = VecDeque::with_capacity(count);
        for i in 0..count {
            let offset = 4 + i * ENTRY_SIZE;
            let timestamp_secs = i64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
            let actions_per_sec =
                f32::from_le_bytes(bytes[offset + 8..offset + 12].try_into().ok()?);
            let active_workers =
                u16::from_le_bytes(bytes[offset + 12..offset + 14].try_into().ok()?);
            let median_instance_duration_secs =
                f32::from_le_bytes(bytes[offset + 14..offset + 18].try_into().ok()?);
            let active_instances =
                u32::from_le_bytes(bytes[offset + 18..offset + 22].try_into().ok()?);
            let queue_depth = u32::from_le_bytes(bytes[offset + 22..offset + 26].try_into().ok()?);
            let in_flight_actions =
                u32::from_le_bytes(bytes[offset + 26..offset + 30].try_into().ok()?);
            entries.push_back(TimeSeriesEntry {
                timestamp_secs,
                actions_per_sec,
                active_workers,
                median_instance_duration_secs,
                active_instances,
                queue_depth,
                in_flight_actions,
            });
        }
        Some(Self { entries })
    }
}

impl Default for PoolTimeSeries {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(ts: i64) -> TimeSeriesEntry {
        TimeSeriesEntry {
            timestamp_secs: ts,
            actions_per_sec: 1.5,
            active_workers: 4,
            median_instance_duration_secs: 10.0,
            active_instances: 20,
            queue_depth: 5,
            in_flight_actions: 8,
        }
    }

    #[test]
    fn test_push_and_len() {
        let mut ts = PoolTimeSeries::new();
        assert_eq!(ts.len(), 0);

        ts.push(sample_entry(1000));
        assert_eq!(ts.len(), 1);
    }

    #[test]
    fn test_capacity_limit() {
        let mut ts = PoolTimeSeries::new();
        for i in 0..1500 {
            ts.push(sample_entry(i as i64));
        }
        assert_eq!(ts.len(), MAX_ENTRIES);
        // Oldest entries should have been dropped; first entry should be 60
        assert_eq!(ts.entries[0].timestamp_secs, 60);
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut ts = PoolTimeSeries::new();
        ts.push(TimeSeriesEntry {
            timestamp_secs: 1700000000,
            actions_per_sec: 3.25,
            active_workers: 8,
            median_instance_duration_secs: 42.5,
            active_instances: 100,
            queue_depth: 15,
            in_flight_actions: 32,
        });
        ts.push(TimeSeriesEntry {
            timestamp_secs: 1700000060,
            actions_per_sec: 2.71,
            active_workers: 6,
            median_instance_duration_secs: 38.0,
            active_instances: 90,
            queue_depth: 10,
            in_flight_actions: 24,
        });

        let bytes = ts.encode();
        let decoded = PoolTimeSeries::decode(&bytes).expect("decode should succeed");
        assert_eq!(decoded.len(), 2);

        let entries = &decoded.entries;
        assert_eq!(entries[0].timestamp_secs, 1700000000);
        assert!((entries[0].actions_per_sec - 3.25).abs() < 0.001);
        assert_eq!(entries[0].active_workers, 8);
        assert!((entries[0].median_instance_duration_secs - 42.5).abs() < 0.01);
        assert_eq!(entries[0].active_instances, 100);
        assert_eq!(entries[0].queue_depth, 15);
        assert_eq!(entries[0].in_flight_actions, 32);

        assert_eq!(entries[1].timestamp_secs, 1700000060);
        assert_eq!(entries[1].active_workers, 6);
        assert_eq!(entries[1].active_instances, 90);
        assert_eq!(entries[1].queue_depth, 10);
        assert_eq!(entries[1].in_flight_actions, 24);
    }

    #[test]
    fn test_decode_empty() {
        let ts = PoolTimeSeries::new();
        let bytes = ts.encode();
        let decoded = PoolTimeSeries::decode(&bytes).expect("decode should succeed");
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn test_decode_invalid_short() {
        assert!(PoolTimeSeries::decode(&[0, 1]).is_none());
    }

    #[test]
    fn test_decode_invalid_truncated() {
        // count says 1 entry but no entry data
        let bytes = 1u32.to_le_bytes();
        assert!(PoolTimeSeries::decode(&bytes).is_none());
    }
}

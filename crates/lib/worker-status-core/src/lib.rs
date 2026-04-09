//! Background status reporting helpers for worker pools.

use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use waymark_ids::InstanceId;

#[derive(Debug, Clone)]
pub struct WorkerPoolStatsSnapshot {
    pub active_workers: u16,
    pub throughput_per_min: f64,
    pub total_completed: i64,
    pub last_action_at: Option<DateTime<Utc>>,
    pub dispatch_queue_size: usize,
    pub total_in_flight: usize,
    pub median_dequeue_ms: Option<i64>,
    pub median_handling_ms: Option<i64>,
}

pub trait WorkerPoolStats {
    fn stats_snapshot(&self) -> WorkerPoolStatsSnapshot;
}

#[derive(Debug, Clone, Copy)]
pub struct InstanceMetricsSnapshot {
    pub median_instance_duration_secs: Option<f64>,
    pub total_instances_completed: i64,
    pub instances_per_sec: f64,
    pub instances_per_min: f64,
}

#[derive(Debug)]
pub struct InstanceMetricsTracker {
    completion_window: Duration,
    duration_samples: usize,
    state: Mutex<InstanceMetricsState>,
}

#[derive(Debug)]
struct InstanceMetricsState {
    active_started_at: HashMap<InstanceId, Instant>,
    recent_completed_at: VecDeque<Instant>,
    recent_duration_secs: VecDeque<f64>,
    total_instances_completed: i64,
}

impl InstanceMetricsTracker {
    const DEFAULT_COMPLETION_WINDOW: Duration = Duration::from_secs(60);
    const DEFAULT_DURATION_SAMPLES: usize = 256;

    pub fn new(completion_window: Duration, duration_samples: usize) -> Self {
        Self {
            completion_window,
            duration_samples: duration_samples.max(1),
            state: Mutex::new(InstanceMetricsState {
                active_started_at: HashMap::new(),
                recent_completed_at: VecDeque::new(),
                recent_duration_secs: VecDeque::new(),
                total_instances_completed: 0,
            }),
        }
    }

    pub fn record_started(&self, instance_id: InstanceId) {
        let mut state = self.state.lock().expect("instance metrics poisoned");
        state.active_started_at.insert(instance_id, Instant::now());
    }

    pub fn record_finished(&self, instance_id: InstanceId, success: bool) {
        let mut state = self.state.lock().expect("instance metrics poisoned");
        let now = Instant::now();
        let started_at = state.active_started_at.remove(&instance_id);

        if !success {
            return;
        }

        state.prune_before(now.checked_sub(self.completion_window).unwrap_or(now));
        state.recent_completed_at.push_back(now);
        state.total_instances_completed = state.total_instances_completed.saturating_add(1);

        if let Some(started_at) = started_at {
            state
                .recent_duration_secs
                .push_back(now.duration_since(started_at).as_secs_f64());
            while state.recent_duration_secs.len() > self.duration_samples {
                state.recent_duration_secs.pop_front();
            }
        }
    }

    pub fn forget_instance(&self, instance_id: InstanceId) {
        let mut state = self.state.lock().expect("instance metrics poisoned");
        state.active_started_at.remove(&instance_id);
    }

    pub fn snapshot(&self) -> InstanceMetricsSnapshot {
        let mut state = self.state.lock().expect("instance metrics poisoned");
        let now = Instant::now();
        state.prune_before(now.checked_sub(self.completion_window).unwrap_or(now));

        let instances_per_min = state.recent_completed_at.len() as f64;
        let instances_per_sec = instances_per_min / 60.0;
        let median_instance_duration_secs = median_f64(&state.recent_duration_secs);

        InstanceMetricsSnapshot {
            median_instance_duration_secs,
            total_instances_completed: state.total_instances_completed,
            instances_per_sec,
            instances_per_min,
        }
    }
}

impl Default for InstanceMetricsTracker {
    fn default() -> Self {
        Self::new(
            Self::DEFAULT_COMPLETION_WINDOW,
            Self::DEFAULT_DURATION_SAMPLES,
        )
    }
}

impl InstanceMetricsState {
    fn prune_before(&mut self, cutoff: Instant) {
        while self
            .recent_completed_at
            .front()
            .is_some_and(|completed_at| *completed_at < cutoff)
        {
            self.recent_completed_at.pop_front();
        }
    }
}

fn median_f64(values: &VecDeque<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    let mut ordered: Vec<f64> = values.iter().copied().collect();
    ordered.sort_by(f64::total_cmp);
    Some(ordered[ordered.len() / 2])
}

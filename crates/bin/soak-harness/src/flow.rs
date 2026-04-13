mod rate;
mod tick_delta;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use sqlx::PgPool;
use tracing::{info, warn};
use waymark_backend_postgres::PostgresBackend;
use waymark_core_backend::QueuedInstance;
use waymark_ids::InstanceId;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_proto::ast as ir;
use waymark_runner_state::RunnerState;

use crate::data;

const MAX_SAMPLE_HISTORY: usize = 20_000;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum TerminationReason {
    DurationReached,
    Interrupted,
    IssueDetected(String),
    WorkerExited(String),
}

impl TerminationReason {
    pub fn is_error_exit(&self) -> bool {
        matches!(self, Self::IssueDetected(_) | Self::WorkerExited(_))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthSample {
    pub at: DateTime<Utc>,
    pub queued_total: i64,
    pub queued_ready: i64,
    pub queued_future: i64,
    pub queued_locked_live: i64,
    pub queued_locked_expired: i64,
    pub queued_this_tick: usize,
    pub actions_per_sec: Option<f64>,
    pub throughput_per_min: Option<f64>,
    pub total_completed: Option<i64>,
    pub active_workers: Option<i32>,
    pub total_in_flight: Option<i64>,
    pub active_instance_count: Option<i32>,
    pub worker_status_age_secs: Option<i64>,
    pub last_action_age_secs: Option<i64>,
    pub zero_streak: usize,
}

pub async fn run_soak_loop(
    args: &crate::cli::SoakArgs,
    backend: &PostgresBackend,
    pool: &PgPool,
    workflow: &crate::setup_workflows::RegisteredWorkflow,
    worker: &mut Option<crate::setup_workers::WorkerProcess>,
) -> Result<(TerminationReason, VecDeque<HealthSample>)> {
    let seed = args.seed.unwrap_or_else(rand::random);
    info!(seed, "soak workload random seed");
    let mut rng = StdRng::seed_from_u64(seed);

    let mut samples: VecDeque<HealthSample> = VecDeque::new();
    let mut zero_streak = 0usize;
    let start = Instant::now();
    let mut previous_total_completed: Option<i64> = None;
    let tick_duration = NonZeroDuration::from_nonzero_secs(args.tick_seconds);

    let queue_rate = rate::Rate::per_minute(args.queue_rate_per_minute);
    let mut tick_delta = tick_delta::TickDelta::new(start.into());
    let mut ticker = tokio::time::interval(tick_duration.get());
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let _ = ticker.tick().await;

    loop {
        let elapsed = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                return Ok((TerminationReason::Interrupted, samples));
            }
            instant = ticker.tick() =>tick_delta.tick(instant),
        };

        if let Some(worker_process) = worker.as_mut()
            && let Some(status) = worker_process
                .child
                .try_wait()
                .context("poll worker process")?
        {
            return Ok((
                TerminationReason::WorkerExited(format!("worker process exited: {status}")),
                samples,
            ));
        }

        let queue_snapshot = data::fetch_queue_snapshot(pool).await?;
        let worker_status = data::fetch_latest_worker_status(pool).await?;

        let mut requested = queue_rate.for_delta(elapsed);

        if queue_snapshot.ready < args.target_ready_queue {
            let deficit = (args.target_ready_queue - queue_snapshot.ready) as u128;
            requested = requested.saturating_add(deficit.min(args.max_top_up_per_tick.get()));
        }

        requested = requested.min(args.max_queue_per_tick.get());

        let requested: usize = requested.try_into().unwrap_or(usize::MAX);

        let queued_this_tick = if requested > 0 {
            queue_instances(backend, workflow, args, requested, &mut rng).await?
        } else {
            0
        };

        let now = Utc::now();
        let status_age_secs = worker_status
            .as_ref()
            .map(|status| (now - status.updated_at).num_seconds());
        let last_action_age_secs = worker_status.as_ref().and_then(|status| {
            status
                .last_action_at
                .map(|last_action| (now - last_action).num_seconds())
        });

        let stalled = should_count_stall(
            args,
            &queue_snapshot,
            worker_status.as_ref(),
            status_age_secs,
            last_action_age_secs,
        );

        if stalled {
            zero_streak = zero_streak.saturating_add(1);
        } else {
            zero_streak = 0;
        }

        let sample = HealthSample {
            at: now,
            queued_total: queue_snapshot.total,
            queued_ready: queue_snapshot.ready,
            queued_future: queue_snapshot.future,
            queued_locked_live: queue_snapshot.locked_live,
            queued_locked_expired: queue_snapshot.locked_expired,
            queued_this_tick,
            actions_per_sec: worker_status.as_ref().map(|value| value.actions_per_sec),
            throughput_per_min: worker_status.as_ref().map(|value| value.throughput_per_min),
            total_completed: worker_status.as_ref().map(|value| value.total_completed),
            active_workers: worker_status.as_ref().map(|value| value.active_workers),
            total_in_flight: worker_status.as_ref().map(|value| value.total_in_flight),
            active_instance_count: worker_status
                .as_ref()
                .map(|value| value.active_instance_count),
            worker_status_age_secs: status_age_secs,
            last_action_age_secs,
            zero_streak,
        };
        samples.push_back(sample);
        while samples.len() > MAX_SAMPLE_HISTORY {
            let _ = samples.pop_front();
        }

        match worker_status.as_ref() {
            Some(status) => {
                let completed_delta = previous_total_completed
                    .map(|previous| status.total_completed.saturating_sub(previous))
                    .unwrap_or(0);
                previous_total_completed = Some(status.total_completed);
                info!(
                    elapsed_secs = start.elapsed().as_secs_f64(),
                    queued_total = queue_snapshot.total,
                    ready_queue = queue_snapshot.ready,
                    queued_this_tick,
                    actions_per_sec = status.actions_per_sec,
                    total_completed = status.total_completed,
                    completed_delta,
                    in_flight = status.total_in_flight,
                    active_instances = status.active_instance_count,
                    status_age_secs = status_age_secs.unwrap_or(-1),
                    last_action_age_secs = last_action_age_secs.unwrap_or(-1),
                    zero_streak,
                    "soak tick"
                );
            }
            None => {
                warn!(
                    ready_queue = queue_snapshot.ready,
                    queued_this_tick, zero_streak, "soak tick without worker_status row"
                );
            }
        }

        if zero_streak >= args.issue_consecutive_samples.get() {
            let detail = format!(
                "actions/sec <= {:.4} for {} consecutive samples while ready queue={} (threshold={})",
                args.issue_actions_per_sec_threshold,
                zero_streak,
                queue_snapshot.ready,
                args.issue_min_ready_queue
            );
            return Ok((TerminationReason::IssueDetected(detail), samples));
        }

        if let Some(hours) = args.duration_hours
            && start.elapsed() >= Duration::from_secs_f64(hours * 60.0 * 60.0)
        {
            return Ok((TerminationReason::DurationReached, samples));
        }
    }
}

fn should_count_stall(
    args: &crate::cli::SoakArgs,
    queue_snapshot: &data::QueueSnapshot,
    worker_status: Option<&data::WorkerStatusSnapshot>,
    status_age_secs: Option<i64>,
    last_action_age_secs: Option<i64>,
) -> bool {
    if queue_snapshot.ready < args.issue_min_ready_queue {
        return false;
    }

    let Some(status) = worker_status else {
        return true;
    };

    let status_age = status_age_secs.unwrap_or(i64::MAX);
    if status_age > args.issue_status_stale_secs {
        return true;
    }

    if status.actions_per_sec > args.issue_actions_per_sec_threshold {
        return false;
    }

    let last_action_age = last_action_age_secs.unwrap_or(i64::MAX);
    status.total_in_flight == 0 || last_action_age > args.issue_last_action_stale_secs
}

async fn queue_instances(
    backend: &PostgresBackend,
    workflow: &crate::setup_workflows::RegisteredWorkflow,
    args: &crate::cli::SoakArgs,
    count: usize,
    rng: &mut StdRng,
) -> Result<usize> {
    let mut queued_total = 0usize;
    let mut remaining = count;

    while remaining > 0 {
        let take = remaining.min(args.queue_batch_size.get());
        let mut instances = Vec::with_capacity(take);

        for _ in 0..take {
            let item = sample_work_item(args, rng);
            let instance = build_instance(workflow, item)?;
            instances.push(instance);
        }

        backend
            .queue_instances(&instances)
            .await
            .context("queue soak instances")?;

        queued_total += take;
        remaining -= take;
    }

    Ok(queued_total)
}

#[derive(Debug, Clone)]
struct WorkItem {
    pub step_delays_ms: Vec<i64>,
    pub step_should_fail: Vec<bool>,
    pub step_payload_bytes: Vec<i64>,
    pub step_include_payload: Vec<bool>,
}

fn sample_work_item(args: &crate::cli::SoakArgs, rng: &mut StdRng) -> WorkItem {
    let len = args.actions_per_workflow.get();

    let mut step_delays_ms = Vec::with_capacity(len);
    let mut step_should_fail = Vec::with_capacity(len);
    let mut step_payload_bytes = Vec::with_capacity(len);
    let mut step_include_payload = Vec::with_capacity(len);

    for _ in 0..len {
        let (delay_ms, should_fail) = sample_step_behavior(args, rng);
        step_delays_ms.push(delay_ms);
        step_should_fail.push(should_fail);
        step_payload_bytes.push(jitter_payload(args.payload_bytes, rng));
        step_include_payload.push(args.include_payload_in_result);
    }

    WorkItem {
        step_delays_ms,
        step_should_fail,
        step_payload_bytes,
        step_include_payload,
    }
}

fn sample_step_behavior(args: &crate::cli::SoakArgs, rng: &mut StdRng) -> (i64, bool) {
    let timeout_threshold = args.timeout_percent;
    let failure_threshold = timeout_threshold + args.failure_percent;
    let slow_threshold = failure_threshold + args.slow_percent;

    let class = rng.random_range(0.0..100.0);
    let timeout_base_ms = i64::from(args.timeout_seconds) * 1000;

    if class < timeout_threshold {
        let delay_ms = rng.random_range(
            (timeout_base_ms + 1500)..=(timeout_base_ms * 3).max(timeout_base_ms + 1500),
        );
        return (delay_ms, false);
    }

    if class < failure_threshold {
        return (rng.random_range(50..=400), true);
    }

    if class < slow_threshold {
        return (rng.random_range(1_000..=8_000), false);
    }

    (rng.random_range(25..=400), false)
}

fn jitter_payload(base_payload: i64, rng: &mut StdRng) -> i64 {
    if base_payload <= 0 {
        return 0;
    }

    let lower = (base_payload / 2).max(1);
    let upper = (base_payload * 3 / 2).max(lower);
    rng.random_range(lower..=upper)
}

fn build_instance(
    workflow: &crate::setup_workflows::RegisteredWorkflow,
    item: WorkItem,
) -> Result<QueuedInstance> {
    let mut state = RunnerState::new(Some(Arc::clone(&workflow.dag)), None, None, false);
    if item.step_delays_ms.len() != item.step_should_fail.len()
        || item.step_delays_ms.len() != item.step_payload_bytes.len()
        || item.step_delays_ms.len() != item.step_include_payload.len()
    {
        bail!("step input vectors are not aligned");
    }

    for (step, (((delay_ms, should_fail), payload_bytes), include_payload)) in item
        .step_delays_ms
        .iter()
        .zip(item.step_should_fail.iter())
        .zip(item.step_payload_bytes.iter())
        .zip(item.step_include_payload.iter())
        .enumerate()
    {
        let idx = step + 1;
        state
            .record_assignment(
                vec![format!("delay_ms_{idx}")],
                &literal_int(*delay_ms),
                None,
                Some(format!("input delay_ms_{idx} = {delay_ms}")),
            )
            .map_err(|err| anyhow!(err.0))?;
        state
            .record_assignment(
                vec![format!("should_fail_{idx}")],
                &literal_bool(*should_fail),
                None,
                Some(format!("input should_fail_{idx} = {should_fail}")),
            )
            .map_err(|err| anyhow!(err.0))?;
        state
            .record_assignment(
                vec![format!("payload_bytes_{idx}")],
                &literal_int(*payload_bytes),
                None,
                Some(format!("input payload_bytes_{idx} = {payload_bytes}")),
            )
            .map_err(|err| anyhow!(err.0))?;
        state
            .record_assignment(
                vec![format!("include_payload_{idx}")],
                &literal_bool(*include_payload),
                None,
                Some(format!("input include_payload_{idx} = {include_payload}")),
            )
            .map_err(|err| anyhow!(err.0))?;
    }

    let entry_node = state
        .queue_template_node(&workflow.entry_template_id, None)
        .map_err(|err| anyhow!(err.0))?;

    Ok(QueuedInstance {
        workflow_version_id: workflow.workflow_version_id,
        schedule_id: None,
        entry_node: entry_node.node_id,
        state,
        action_results: HashMap::new(),
        instance_id: InstanceId::new_uuid_v4(),
        scheduled_at: None,
    })
}

fn literal_int(value: i64) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::Literal(ir::Literal {
            value: Some(ir::literal::Value::IntValue(value)),
        })),
        span: None,
    }
}

fn literal_bool(value: bool) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::Literal(ir::Literal {
            value: Some(ir::literal::Value::BoolValue(value)),
        })),
        span: None,
    }
}

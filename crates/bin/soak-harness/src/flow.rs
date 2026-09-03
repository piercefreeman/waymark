mod rate;
mod tick_delta;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use color_eyre::eyre::{WrapErr as _, bail, eyre};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use sqlx::PgPool;
use tracing::{info, warn};
use waymark_ids::InstanceId;
use waymark_nonzero_duration::NonZeroDuration;

use crate::data;
use crate::setup_workflows::{RegisteredWorkflow, SoakServices};

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
    pub runnable_total: i64,
    pub runnable_ready: i64,
    pub pinned_live: i64,
    pub pinned_expired: i64,
    pub workflows_completed: i64,
    pub queued_this_tick: usize,
    pub actions_per_sec: Option<f64>,
    pub actions_completed_total: Option<u64>,
    pub worker_pool_size: Option<u64>,
    pub in_flight_actions: Option<u64>,
    pub driven_vm_runtimes: Option<u64>,
    pub node_sample_age_secs: Option<i64>,
    pub last_action_age_secs: Option<i64>,
    pub zero_streak: usize,
}

pub async fn run_soak_loop(
    args: &crate::cli::SoakArgs,
    services: &SoakServices,
    pool: &PgPool,
    store: &waymark_observability_store_postgres::Store,
    workflow: &RegisteredWorkflow,
    worker: &mut Option<crate::setup_workers::WorkerProcess>,
) -> Result<(TerminationReason, VecDeque<HealthSample>), color_eyre::eyre::Report> {
    let seed = args.seed.unwrap_or_else(rand::random);
    info!(seed, "soak workload random seed");
    let mut rng = StdRng::seed_from_u64(seed);

    let mut samples: VecDeque<HealthSample> = VecDeque::new();
    let mut zero_streak = 0usize;
    let start = Instant::now();
    let mut action_completion_rate = waymark_counter_rate::CounterRate::new();
    let mut action_completion_window: Option<waymark_counter_rate::Window<u64>> = None;
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
                .wrap_err("poll worker process")?
        {
            return Ok((
                TerminationReason::WorkerExited(format!("worker process exited: {status}")),
                samples,
            ));
        }

        let workload_snapshot = data::fetch_workload_snapshot(pool).await?;
        let node_sample = data::fetch_latest_node_sample(store).await?;

        let mut requested = queue_rate.for_delta(elapsed);

        if workload_snapshot.ready < args.target_ready_queue {
            let deficit = (args.target_ready_queue - workload_snapshot.ready) as u128;
            requested = requested.saturating_add(deficit.min(args.max_top_up_per_tick.get()));
        }

        requested = requested.min(args.max_queue_per_tick.get());

        let requested: usize = requested.try_into().unwrap_or(usize::MAX);

        let queued_this_tick = if requested > 0 {
            register_instances(services, workflow, args, requested, &mut rng).await?
        } else {
            0
        };

        let now = Utc::now();
        let sample_age_secs = node_sample
            .as_ref()
            .map(|sample| (now - sample.sampled_at).num_seconds());
        let last_action_age_secs = node_sample.as_ref().and_then(|sample| {
            sample
                .last_action_completed_at
                .map(|last_action| (now - last_action).num_seconds())
        });

        // The store keeps no rates; the derivation from the cumulative
        // completions counter is the reader's. The last derived window
        // carries across the ticks that re-read the same sample.
        if let Some(sample) = node_sample.as_ref() {
            let observation = action_completion_rate
                .observe(sample.sampled_at, sample.actions_completed_total)
                .wrap_err("observe the completions counter")?;

            if let waymark_counter_rate::Observation::Advanced(window) = observation {
                action_completion_window = Some(window);
            }
        }
        let actions_per_sec = action_completion_window
            .as_ref()
            .map(|window| window.per_second);
        let actions_completed_delta = action_completion_window.as_ref().map(|window| window.delta);

        let stalled = should_count_stall(
            args,
            &workload_snapshot,
            node_sample.as_ref(),
            actions_per_sec,
            sample_age_secs,
            last_action_age_secs,
        );

        if stalled {
            zero_streak = zero_streak.saturating_add(1);
        } else {
            zero_streak = 0;
        }

        let sample = HealthSample {
            at: now,
            runnable_total: workload_snapshot.total,
            runnable_ready: workload_snapshot.ready,
            pinned_live: workload_snapshot.pinned_live,
            pinned_expired: workload_snapshot.pinned_expired,
            workflows_completed: workload_snapshot.workflows_completed,
            queued_this_tick,
            actions_per_sec,
            actions_completed_total: node_sample
                .as_ref()
                .map(|value| value.actions_completed_total),
            worker_pool_size: node_sample.as_ref().map(|value| value.worker_pool_size),
            in_flight_actions: node_sample.as_ref().map(|value| value.in_flight_actions),
            driven_vm_runtimes: node_sample.as_ref().map(|value| value.driven_vm_runtimes),
            node_sample_age_secs: sample_age_secs,
            last_action_age_secs,
            zero_streak,
        };
        samples.push_back(sample);
        while samples.len() > MAX_SAMPLE_HISTORY {
            let _ = samples.pop_front();
        }

        match node_sample.as_ref() {
            Some(sample) => {
                info!(
                    elapsed_secs = start.elapsed().as_secs_f64(),
                    runnable_total = workload_snapshot.total,
                    ready_queue = workload_snapshot.ready,
                    pinned_live = workload_snapshot.pinned_live,
                    pinned_expired = workload_snapshot.pinned_expired,
                    workflows_completed = workload_snapshot.workflows_completed,
                    queued_this_tick,
                    actions_per_sec = actions_per_sec.unwrap_or(-1.0),
                    actions_completed_total = sample.actions_completed_total,
                    actions_completed_delta = actions_completed_delta.unwrap_or(0),
                    in_flight_actions = sample.in_flight_actions,
                    driven_vm_runtimes = sample.driven_vm_runtimes,
                    sample_age_secs = sample_age_secs.unwrap_or(-1),
                    last_action_age_secs = last_action_age_secs.unwrap_or(-1),
                    zero_streak,
                    "soak tick"
                );
            }
            None => {
                warn!(
                    ready_queue = workload_snapshot.ready,
                    queued_this_tick, zero_streak, "soak tick without a node sample"
                );
            }
        }

        if zero_streak >= args.issue_consecutive_samples.get() {
            let detail = format!(
                "actions/sec <= {:.4} for {} consecutive samples while ready queue={} (threshold={})",
                args.issue_actions_per_sec_threshold,
                zero_streak,
                workload_snapshot.ready,
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
    workload_snapshot: &data::WorkloadSnapshot,
    node_sample: Option<&data::NodeSampleReport>,
    actions_per_sec: Option<f64>,
    sample_age_secs: Option<i64>,
    last_action_age_secs: Option<i64>,
) -> bool {
    if workload_snapshot.ready < args.issue_min_ready_queue {
        return false;
    }

    let Some(sample) = node_sample else {
        return true;
    };

    let sample_age = sample_age_secs.unwrap_or(i64::MAX);
    if sample_age > args.issue_status_stale_secs {
        return true;
    }

    if actions_per_sec.unwrap_or(0.0) > args.issue_actions_per_sec_threshold {
        return false;
    }

    let last_action_age = last_action_age_secs.unwrap_or(i64::MAX);
    sample.in_flight_actions == 0 || last_action_age > args.issue_last_action_stale_secs
}

async fn register_instances(
    services: &SoakServices,
    workflow: &RegisteredWorkflow,
    args: &crate::cli::SoakArgs,
    count: usize,
    rng: &mut StdRng,
) -> Result<usize, color_eyre::eyre::Report> {
    let mut registered = 0usize;

    while registered < count {
        let take = (count - registered).min(args.queue_batch_size.get());
        let mut vms = Vec::with_capacity(take);

        for _ in 0..take {
            let item = sample_work_item(args, rng);
            let inputs = build_instance_inputs(&item)?;

            let call_spec = waymark_vm_runtime_builder::builder(&workflow.metadata)
                .first_fn()
                .map_err(|err| eyre!("select soak entry function: {err}"))?
                .args(inputs)
                .map_err(|err| eyre!("match soak entry function arguments: {err}"))?;
            let runtime = waymark_system_vm::Runtime::with_custom_entrypoint(
                waymark_system_vm::Interpreter::default(),
                Arc::clone(&workflow.executable),
                call_spec,
            )
            .map_err(|err| eyre!("create soak VM runtime: {err}"))?;

            vms.push((
                InstanceId::new_uuid_v4(),
                workflow.workflow_version_id,
                runtime,
            ));
        }

        let success = services
            .registration
            .register_vms(
                nonempty_collections::NEVec::try_from_vec(vms)
                    .expect("the batch takes at least one instance"),
                |runtime, serializer| runtime.snapshot(serializer),
            )
            .await
            .map_err(|err| eyre!("register soak VMs: {err}"))?;
        assert!(
            matches!(
                success,
                waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegistrationSuccess::AllRegistered,
            ),
            "freshly minted instance ids can never be already registered",
        );

        registered += take;
    }

    Ok(registered)
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

fn build_instance_inputs(
    item: &WorkItem,
) -> Result<HashMap<String, waymark_system_vm::Value>, color_eyre::eyre::Report> {
    if item.step_delays_ms.len() != item.step_should_fail.len()
        || item.step_delays_ms.len() != item.step_payload_bytes.len()
        || item.step_delays_ms.len() != item.step_include_payload.len()
    {
        bail!("step input vectors are not aligned");
    }

    let mut inputs = HashMap::with_capacity(item.step_delays_ms.len() * 4);
    for (step, (((delay_ms, should_fail), payload_bytes), include_payload)) in item
        .step_delays_ms
        .iter()
        .zip(item.step_should_fail.iter())
        .zip(item.step_payload_bytes.iter())
        .zip(item.step_include_payload.iter())
        .enumerate()
    {
        let idx = step + 1;
        inputs.insert(
            format!("delay_ms_{idx}"),
            waymark_system_vm::Value::Ready(waymark_system_vm::ReadyValue::Int(*delay_ms)),
        );
        inputs.insert(
            format!("should_fail_{idx}"),
            waymark_system_vm::Value::Ready(waymark_system_vm::ReadyValue::Bool(*should_fail)),
        );
        inputs.insert(
            format!("payload_bytes_{idx}"),
            waymark_system_vm::Value::Ready(waymark_system_vm::ReadyValue::Int(*payload_bytes)),
        );
        inputs.insert(
            format!("include_payload_{idx}"),
            waymark_system_vm::Value::Ready(waymark_system_vm::ReadyValue::Bool(*include_payload)),
        );
    }

    Ok(inputs)
}

//! Execution-subsystem configuration and supervision for the benchmark.

use core::str::FromStr;
use std::num::NonZeroUsize;
use std::time::Duration;

use waymark_nonzero_duration::NonZeroDuration;

/// Parses a [`NonZeroDuration`] from a whole-milliseconds string.
struct FromMillis(NonZeroDuration);

impl FromStr for FromMillis {
    type Err = core::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(NonZeroDuration::from_nonzero_millis(s.parse()?)))
    }
}

/// Reads a batch-size knob from the environment.
fn batch_max(name: &'static str) -> Result<NonZeroUsize, color_eyre::eyre::Report> {
    Ok(envfury::or_parse(name, "256")?)
}

/// Reads a batch flush-window knob from the environment.
fn batch_delay(name: &'static str) -> Result<NonZeroDuration, color_eyre::eyre::Report> {
    let FromMillis(delay) = envfury::or_parse(name, "5")?;
    Ok(delay)
}

pub fn durable_execution_config(
    max_pinned: NonZeroUsize,
) -> Result<waymark_execution_bringup::Config<uuid::Uuid>, color_eyre::eyre::Report> {
    Ok(waymark_execution_bringup::Config {
        node_id: uuid::Uuid::new_v4(),
        action_effect_reconciler_lock_ttl: Duration::from_secs(15).try_into().unwrap(),
        action_effect_reconciler_lock_heartbeat: Duration::from_secs(5).try_into().unwrap(),
        max_pinned,
        pinning_ttl: Duration::from_secs(15).try_into().unwrap(),
        pinning_heartbeat: Duration::from_secs(5).try_into().unwrap(),
        pinning_fencing_margin: Duration::from_secs(1).try_into().unwrap(),
        workload_poll_interval: Duration::from_millis(1).try_into().unwrap(),
        snapshot_batch_max: batch_max("WAYMARK_SNAPSHOT_BATCH_MAX")?,
        snapshot_batch_delay: batch_delay("WAYMARK_SNAPSHOT_BATCH_DELAY_MS")?,
        action_effect_reconciler_request_batch_max: batch_max(
            "WAYMARK_ACTION_EFFECT_RECONCILER_REQUEST_BATCH_MAX",
        )?,
        action_effect_reconciler_request_batch_delay: batch_delay(
            "WAYMARK_ACTION_EFFECT_RECONCILER_REQUEST_BATCH_DELAY_MS",
        )?,
        workflow_completion_batch_max: batch_max("WAYMARK_WORKFLOW_COMPLETION_BATCH_MAX")?,
        workflow_completion_batch_delay: batch_delay("WAYMARK_WORKFLOW_COMPLETION_BATCH_DELAY_MS")?,
        action_effect_reconciler_lock_batch_max: batch_max(
            "WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_BATCH_MAX",
        )?,
        action_effect_reconciler_lock_batch_delay: batch_delay(
            "WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_BATCH_DELAY_MS",
        )?,
        sleep_poll_interval: Duration::from_millis(250).try_into().unwrap(),
        vm_retention: Duration::from_secs(60).try_into().unwrap(),
        vm_sweep_interval: Duration::from_secs(10).try_into().unwrap(),
        executable_retention: Duration::from_secs(300).try_into().unwrap(),
        executable_sweep_interval: Duration::from_secs(60).try_into().unwrap(),
    })
}

/// Supervise the execution subsystem's tasks.
pub fn track(supervisor: &mut crate::Supervisor, handles: waymark_execution_bringup::Handles) {
    let waymark_execution_bringup::Handles {
        pinning_manager,
        execution_driver,
        executable_sweeper,
        vm_sweeper,
        durable_action_completions_writer,
        durable_action_completions_poller,
        durable_action_completions_acker,
        durable_sleeps_poller,
        durable_sleeps_acker,
        action_effect_reconciler_lock_renewal,
        snapshot_batcher,
        action_effect_reconciler_request_batcher,
        workflow_completion_batcher,
        action_effect_reconciler_lock_batcher,
    } = handles;

    supervisor.track("workload pinning manager", pinning_manager);
    supervisor.track("execution driver", execution_driver);
    supervisor.track("executable sweeper", executable_sweeper);
    supervisor.track("vm runtimes sweeper", vm_sweeper);
    supervisor.track(
        "durable action completions writer",
        durable_action_completions_writer,
    );
    supervisor.track(
        "durable action completions poller",
        durable_action_completions_poller,
    );
    supervisor.track(
        "durable action completions acker",
        durable_action_completions_acker,
    );
    supervisor.track("durable sleeps poller", durable_sleeps_poller);
    supervisor.track("durable sleeps acker", durable_sleeps_acker);
    supervisor.track(
        "action effect reconciler lock renewal",
        action_effect_reconciler_lock_renewal,
    );
    supervisor.track("snapshot batcher", snapshot_batcher);
    supervisor.track(
        "action effect reconciler request batcher",
        action_effect_reconciler_request_batcher,
    );
    supervisor.track("workflow completion batcher", workflow_completion_batcher);
    supervisor.track(
        "action effect reconciler lock batcher",
        action_effect_reconciler_lock_batcher,
    );
}

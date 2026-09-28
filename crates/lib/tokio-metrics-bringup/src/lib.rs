//! Bringup for the tokio runtime and task metrics reporters.

#![warn(missing_docs)]

fn make_metric_name_transformer(
    executable_name: &'static str,
) -> impl Fn(&'static str) -> metrics::Key + Send + Sync + Copy + 'static {
    move |name| {
        metrics::Key::from_parts(
            metrics::KeyName::from_const_str(name),
            &[("application", executable_name)],
        )
    }
}

/// Spawn the runtime metrics reporter, and the task metrics reporter when
/// `task_monitor` is given, as tasks on `spawner`; each stops when
/// `shutdown_token` is cancelled.
pub fn start<Spawner>(
    mut spawner: Spawner,
    executable_name: &'static str,
    task_monitor: Option<tokio_metrics::TaskMonitor>,
    shutdown_token: tokio_util::sync::CancellationToken,
) where
    Spawner: waymark_managed_spawner::Spawner,
{
    let metric_name_transformer = make_metric_name_transformer(executable_name);

    spawner.spawn("tokio runtime metrics reporter", {
        let shutdown_token = shutdown_token.clone();
        async move {
            shutdown_token
                .run_until_cancelled(
                    tokio_metrics::RuntimeMetricsReporterBuilder::default()
                        .with_metrics_transformer(metric_name_transformer)
                        .describe_and_run(),
                )
                .await;
        }
    });

    // The task metrics are those of the tasks instrumented with the
    // monitor; without one there is nothing to report on.
    if let Some(task_monitor) = task_monitor {
        spawner.spawn("tokio task metrics reporter", async move {
            shutdown_token
                .run_until_cancelled(
                    tokio_metrics::TaskMetricsReporterBuilder::new(metric_name_transformer)
                        .describe_and_run(task_monitor),
                )
                .await;
        });
    }
}

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

pub fn start<Spawner>(
    mut spawner: Spawner,
    executable_name: &'static str,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> tokio_metrics::TaskMonitor
where
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

    let task_monitor = tokio_metrics::TaskMonitor::new();

    spawner.spawn("tokio task metrics reporter", {
        let task_monitor = task_monitor.clone();
        async move {
            shutdown_token
                .run_until_cancelled(
                    tokio_metrics::TaskMetricsReporterBuilder::new(metric_name_transformer)
                        .describe_and_run(task_monitor),
                )
                .await;
        }
    });

    task_monitor
}

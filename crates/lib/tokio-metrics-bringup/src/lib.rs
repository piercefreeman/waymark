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

// TODO: update spawns when we have a proper
// task management / registration system.
pub fn bringup(executable_name: &'static str) -> tokio_metrics::TaskMonitor {
    let metric_name_transformer = make_metric_name_transformer(executable_name);

    tokio::spawn(
        tokio_metrics::RuntimeMetricsReporterBuilder::default()
            .with_metrics_transformer(metric_name_transformer)
            .describe_and_run(),
    );

    let task_monitor = tokio_metrics::TaskMonitor::new();

    tokio::spawn(
        tokio_metrics::TaskMetricsReporterBuilder::new(metric_name_transformer)
            .describe_and_run(task_monitor.clone()),
    );

    task_monitor
}

use monad_metrics::{executor_metrics, MetricsPolicy};

executor_metrics! {
    StateSync {
        namespace: statesync,
        gauges: [
            syncing,
            progress_estimate,
            last_target,
        ]
    }
}

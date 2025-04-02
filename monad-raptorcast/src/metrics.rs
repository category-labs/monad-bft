use monad_metrics::{executor_metrics, MetricsPolicy};

executor_metrics! {
    RaptorCast {
        namespace: raptorcast,
        counters: [
            a
        ],
    }
}

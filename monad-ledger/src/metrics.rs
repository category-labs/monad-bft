use monad_metrics::{executor_metrics, MetricsPolicy};

executor_metrics! {
    Ledger {
        namespace: ledger,
        counters: [
            num_commits,
            num_tx_commits,
        ],
        gauges: [
            block_num,
        ]
    }
}

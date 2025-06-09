use monad_metrics::metrics_bft;

metrics_bft! {
    #[statesync]
    StateSync {
        #[gauge]
        syncing,

        #[gauge]
        progress_estimate,

        #[gauge]
        last_target
    }
}

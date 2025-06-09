use monad_metrics::metrics_bft;

metrics_bft! {
    Ledger {
        #[counter]
        num_commits,

        #[counter]
        num_tx_commits,

        #[gauge]
        block_num
    }
}

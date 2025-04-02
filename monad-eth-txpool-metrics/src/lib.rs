use monad_metrics::{MetricsPolicy, executor_metrics};

executor_metrics! {
    TxPool {
        namespace: txpool,
        counters: [
            reject_forwarded_invalid_bytes,
            reject_forwarded_invalid_signer,

            create_proposal,
            create_proposal_elapsed_ns,
            create_proposal_txs,
            create_proposal_available_txs,
        ],
        components: [
            Pool {
                namespace: pool,
                counters: [
                    insert_owned_txs,
                    insert_forwarded_txs,

                    drop_not_well_formed,
                    drop_invalid_signature,
                    drop_nonce_too_low,
                    drop_fee_too_low,
                    drop_insufficient_balance,
                    drop_existing_higher_priority,
                    drop_pool_full,
                    drop_pool_not_ready,
                    drop_internal_state_backend_error,
                ],
                components: [
                    Pending {
                        namespace: pending,
                        counters: [
                            promote_addresses,
                            promote_txs,
                            drop_unknown_addresses,
                            drop_unknown_txs,
                            drop_low_nonce_addresses,
                            drop_low_nonce_txs,
                        ],
                        gauges: [
                            addresses,
                            txs,
                        ]
                    },
                    Tracked {
                        namespace: tracked,
                        counters: [
                            evict_expired_addresses,
                            evict_expired_txs,
                            remove_committed_addresses,
                            remove_committed_txs,
                        ],
                        gauges: [
                            addresses,
                            txs,
                        ]
                    }
                ]
            }
        ]
    }
}

use monad_metrics::metrics_bft;

metrics_bft! {
    #[txpool]
    TxPool {
        #[counter]
        create_proposal,

        #[counter]
        create_proposal_elapsed_ns,

        #[counter(
            label = "error",
            variants = [invalid_bytes, invalid_signer]
        )]
        reject_forwarded,

        Pool {
            #[counter]
            create_proposal,

            #[counter]
            create_proposal_txs,

            #[counter(
                label = "address_type",
                variants = [tracked, available]
            )]
            create_proposal_addresses,

            #[counter]
            create_proposal_backend_lookups,

            #[counter(
                label = "tx_type",
                variants = [owned, forwarded]
            )]
            insert,

            #[counter(
                label = "reason",
                variants = [
                    not_well_formed,
                    invalid_signature,
                    nonce_too_low,
                    fee_too_low,
                    insufficient_balance,
                    existing_higher_priority,
                    pool_full,
                    pool_not_ready,
                    internal_state_backend_error,
                ]
            )]
            drop,

            Pending {
                #[counter(
                    label = "type",
                    variants = [addresses, txs]
                )]
                promote,

                #[counter(
                    label = "type",
                    variants = [
                        unknown_addresses,
                        unknown_txs,
                        low_nonce_addresses,
                        low_nonce_txs,
                    ]
                )]
                drop,

                #[gauge]
                addresses,

                #[gauge]
                txs,
            },

            Tracked {
                #[counter(
                    label = "type",
                    variants = [expired_addresses, expired_txs]
                )]
                evict,

                #[counter(
                    label = "type",
                    variants = [committed_addresses, committed_txs]
                )]
                remove,

                #[gauge]
                addresses,

                #[gauge]
                txs,
            }
        }
    }
}

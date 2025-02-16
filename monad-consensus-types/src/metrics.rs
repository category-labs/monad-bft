use serde::{Deserialize, Serialize};

macro_rules! metrics {
    (
        $(
            (
                $class:ident,
                $class_field:ident,
                [$($name:ident),* $(,)?]
            )
        ),*
        $(,)?
    ) => {
        $(
            metrics!(
                @class
                $class,
                [$($name),*]
            );
        )*

        #[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
        pub struct Metrics {
            $(
                pub $class_field: $class
            ),*
        }

        impl Metrics {
            pub fn metrics(&self) -> Vec<(&'static str, u64)> {
                vec![
                    $(
                        $(
                            (
                                concat!("monad.state.", stringify!($class_field), ".", stringify!($name)),
                                self.$class_field.$name
                            ),
                        )*
                    )*
                ]
            }
        }
    };

    (
        @class
        $class:ident,
        [$($name:ident),*]
    ) => {
        #[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
        pub struct $class {
            $(
                pub $name: u64
            ),*
        }
    };
}

metrics!(
    (
        ValidationErrors,
        validation_errors,
        [
            invalid_author,
            not_well_formed_sig,
            invalid_signature,
            invalid_tc_round,
            insufficient_stake,
            invalid_seq_num,
            val_data_unavailable,
            invalid_vote_message,
            invalid_version,
            invalid_epoch
        ]
    ),
    (
        ConsensusEvents,
        consensus_events,
        [
            local_timeout,
            handle_proposal,
            failed_txn_validation,
            failed_ts_validation,
            invalid_proposal_round_leader,
            out_of_order_proposals,
            created_vote,
            old_vote_received,
            vote_received,
            created_qc,
            old_remote_timeout,
            remote_timeout_msg,
            remote_timeout_msg_with_tc,
            created_tc,
            process_old_qc,
            process_qc,
            // TODO(andr-dev, PR): Add metric to differentiate emitting
            // TxPoolCommand::CreateProposal vs consensus state creating + broadcasting finalized
            // proposal
            creating_proposal,
            rx_execution_lagging,
            rx_bad_state_root,
            proposal_with_tc,
            failed_verify_randao_reveal_sig,
            commit_block,
            enter_new_round_qc,
            enter_new_round_tc,
            trigger_state_sync
        ]
    ),
    (
        BlocktreeEvents,
        blocktree_events,
        [prune_success, add_success, add_dup]
    ),
    (
        BlocksyncEvents,
        blocksync_events,
        [
            self_headers_request,
            self_payload_request,
            self_payload_requests_in_flight,
            headers_response_successful,
            headers_response_failed,
            headers_response_unexpected,
            headers_validation_failed,
            self_headers_response_successful,
            self_headers_response_failed,
            num_headers_received,
            payload_response_successful,
            payload_response_failed,
            payload_response_unexpected,
            self_payload_response_successful,
            self_payload_response_failed,
            request_timeout,
            peer_headers_request,
            peer_headers_request_successful,
            peer_headers_request_failed,
            peer_payload_request,
            peer_payload_request_successful,
            peer_payload_request_failed
        ]
    )
);

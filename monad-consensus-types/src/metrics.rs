use monad_metrics::metrics_bft;

metrics_bft! {
    State {
        #[counter(
            label = "error",
            variants = [
                invalid_author,
                not_well_formed_sig,
                invalid_signature,
                author_not_sender,
                invalid_tc_round,
                insufficient_stake,
                invalid_seq_num,
                val_data_unavailable,
                invalid_vote_message,
                invalid_version,
                invalid_epoch,
            ]
        )]
        validation_errors,

        ConsensusEvents {
            // TODD (andr-dev): Move pacemaker events to separate namespace
            #[counter]
            local_timeout,

            #[counter]
            handle_proposal,

            #[counter]
            failed_txn_validation,

            #[counter]
            failed_ts_validation,

            #[counter]
            invalid_proposal_round_leader,

            #[counter]
            out_of_order_proposals,

            #[counter]
            created_vote,

            #[counter]
            old_vote_received,

            #[counter]
            vote_received,

            #[counter]
            created_qc,

            #[counter]
            old_remote_timeout,

            #[counter]
            remote_timeout_msg,

            #[counter]
            remote_timeout_msg_with_tc,

            #[counter]
            created_tc,

            #[counter]
            process_old_qc,

            #[counter]
            process_qc,

            // TODO (andr-dev): Add metric to differentiate emitting
            // TxPoolCommand::CreateProposal vs consensus state creating + broadcasting
            // finalized proposal
            #[counter]
            creating_proposal,

            #[counter]
            rx_execution_lagging,

            #[counter]
            rx_bad_state_root,

            #[counter]
            rx_no_path_to_root,

            #[counter]
            rx_proposal,

            #[counter]
            proposal_with_tc,

            #[counter]
            failed_verify_randao_reveal_sig,

            #[counter]
            commit_block,

            #[counter]
            state_root_update,

            #[counter]
            enter_new_round_qc,

            #[counter]
            enter_new_round_tc,

            #[counter]
            trigger_state_sync

        },

        #[blocksync_events]
        BlockSyncEvents {
            #[counter]
            self_headers_request,

            #[counter]
            self_payload_request,

            #[counter]
            headers_response_successful,

            #[counter]
            headers_response_failed,

            #[counter]
            headers_response_unexpected,

            #[counter]
            headers_validation_failed,

            #[counter]
            self_headers_response_successful,

            #[counter]
            self_headers_response_failed,

            #[counter]
            num_headers_received,

            #[counter]
            payload_response_successful,

            #[counter]
            payload_response_failed,

            #[counter]
            payload_response_unexpected,

            #[counter]
            self_payload_response_successful,

            #[counter]
            self_payload_response_failed,

            #[counter]
            request_timeout,

            #[counter]
            peer_headers_request,

            #[counter]
            peer_headers_request_successful,

            #[counter]
            peer_headers_request_failed,

            #[counter]
            peer_payload_request,

            #[counter]
            peer_payload_request_successful,

            #[counter]
            peer_payload_request_failed,

            #[gauge]
            self_payload_requests_in_flight
        }
    }
}

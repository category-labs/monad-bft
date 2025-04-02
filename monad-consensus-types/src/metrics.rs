use monad_metrics::{state_metrics, MetricsPolicy};

state_metrics! {
    State {
        namespace: state,
        components: [
            ValidationErrors {
                namespace: validation_errors,
                counters: [
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
                    invalid_epoch
                ]
            },
            ConsensusEvents {
                namespace: consensus_events,
                counters: [
                    // TODD (andr-dev): Move pacemaker events to separate namespace
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
                    // TODO (andr-dev): Add metric to differentiate emitting
                    // TxPoolCommand::CreateProposal vs consensus state creating + broadcasting
                    // finalized proposal
                    creating_proposal,
                    rx_execution_lagging,
                    rx_bad_state_root,
                    rx_no_path_to_root,
                    rx_proposal,
                    proposal_with_tc,
                    failed_verify_randao_reveal_sig,
                    commit_block,
                    state_root_update,
                    enter_new_round_qc,
                    enter_new_round_tc,
                    trigger_state_sync
                ]
            },
            BlockSyncEvents {
                namespace: blocksync_events,
                counters: [
                    self_headers_request,
                    self_payload_request,
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
                ],
                gauges: [
                    self_payload_requests_in_flight,
                ]
            }
        ]
    }
}

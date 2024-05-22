#[derive(Debug, Default, Clone, Copy)]
pub struct ValidationErrors {
    pub invalid_author: u64,
    pub not_well_formed_sig: u64,
    pub invalid_signature: u64,
    pub author_not_sender: u64,
    pub invalid_tc_round: u64,
    pub insufficient_stake: u64,
    pub invalid_seq_num: u64,
    pub val_data_unavailable: u64,
    pub invalid_vote_message: u64,
    pub invalid_version: u64,
    pub invalid_epoch: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ConsensusEvents {
    pub local_timeout: u64,
    pub handle_proposal: u64,
    pub failed_txn_validation: u64,
    pub invalid_proposal_round_leader: u64,
    pub out_of_order_proposals: u64,
    pub created_vote: u64,
    pub old_vote_received: u64,
    pub vote_received: u64,
    pub created_qc: u64,
    pub old_remote_timeout: u64,
    pub remote_timeout_msg: u64,
    pub remote_timeout_msg_with_tc: u64,
    pub created_tc: u64,
    pub process_old_qc: u64,
    pub process_qc: u64,
    pub creating_proposal: u64,
    pub creating_empty_block_proposal: u64,
    pub rx_empty_block: u64,
    pub rx_execution_lagging: u64,
    pub rx_bad_state_root: u64,
    pub rx_missing_state_root: u64,
    pub rx_proposal: u64,
    pub proposal_with_tc: u64,
    pub failed_verify_randao_reveal_sig: u64,
    pub commit_empty_block: u64,
    pub committed_bytes: u64,
    pub state_root_update: u64,
    pub enter_new_round_qc: u64,
    pub enter_new_round_tc: u64,
    pub trigger_state_sync: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BlocktreeEvents {
    pub prune_success: u64,
    pub add_success: u64,
    pub add_dup: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BlocksyncEvents {
    pub blocksync_response_successful: u64,
    pub blocksync_response_failed: u64,
    pub blocksync_response_unexpected: u64,
    pub blocksync_request: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Metrics {
    pub validation_errors: ValidationErrors,
    pub consensus_events: ConsensusEvents,
    pub blocktree_events: BlocktreeEvents,
    pub blocksync_events: BlocksyncEvents,
}

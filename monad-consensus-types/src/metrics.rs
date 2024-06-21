use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
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

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct ConsensusEvents {
    pub local_timeout: u64,
    pub handle_proposal: u64,
    pub failed_txn_validation: u64,
    pub failed_ts_validation: u64,
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

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct TxPoolEvents {
    pub local_inserted_txns: u64,
    pub dropped_txns: u64,
    pub external_inserted_txns: u64,
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct BlocktreeEvents {
    pub prune_success: u64,
    pub add_success: u64,
    pub add_dup: u64,
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct BlocksyncEvents {
    pub blocksync_response_successful: u64,
    pub blocksync_response_failed: u64,
    pub blocksync_response_unexpected: u64,
    pub blocksync_request: u64,
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Metrics {
    pub validation_errors: ValidationErrors,
    pub consensus_events: ConsensusEvents,
    pub txpool_events: TxPoolEvents,
    pub blocktree_events: BlocktreeEvents,
    pub blocksync_events: BlocksyncEvents,
}

macro_rules! metric {
    ($self:expr, $($field:ident).+) => {
        (concat!("monad.state.", stringify!($($field).+)), $self.$($field).+)
    };
}

impl Metrics {
    pub fn metrics(&self) -> Vec<(&'static str, u64)> {
        vec![
            // ValidationErrors
            metric!(self, validation_errors.invalid_author),
            metric!(self, validation_errors.not_well_formed_sig),
            metric!(self, validation_errors.invalid_signature),
            metric!(self, validation_errors.author_not_sender),
            metric!(self, validation_errors.invalid_tc_round),
            metric!(self, validation_errors.insufficient_stake),
            metric!(self, validation_errors.invalid_seq_num),
            metric!(self, validation_errors.val_data_unavailable),
            metric!(self, validation_errors.invalid_vote_message),
            metric!(self, validation_errors.invalid_version),
            metric!(self, validation_errors.invalid_epoch),
            // ConsensusEvents
            metric!(self, consensus_events.local_timeout),
            metric!(self, consensus_events.handle_proposal),
            metric!(self, consensus_events.failed_txn_validation),
            metric!(self, consensus_events.invalid_proposal_round_leader),
            metric!(self, consensus_events.out_of_order_proposals),
            metric!(self, consensus_events.created_vote),
            metric!(self, consensus_events.old_vote_received),
            metric!(self, consensus_events.vote_received),
            metric!(self, consensus_events.created_qc),
            metric!(self, consensus_events.old_remote_timeout),
            metric!(self, consensus_events.remote_timeout_msg),
            metric!(self, consensus_events.remote_timeout_msg_with_tc),
            metric!(self, consensus_events.created_tc),
            metric!(self, consensus_events.process_old_qc),
            metric!(self, consensus_events.process_qc),
            metric!(self, consensus_events.creating_proposal),
            metric!(self, consensus_events.creating_empty_block_proposal),
            metric!(self, consensus_events.rx_empty_block),
            metric!(self, consensus_events.rx_execution_lagging),
            metric!(self, consensus_events.rx_bad_state_root),
            metric!(self, consensus_events.rx_missing_state_root),
            metric!(self, consensus_events.rx_proposal),
            metric!(self, consensus_events.proposal_with_tc),
            metric!(self, consensus_events.failed_verify_randao_reveal_sig),
            metric!(self, consensus_events.commit_empty_block),
            metric!(self, consensus_events.committed_bytes),
            metric!(self, consensus_events.state_root_update),
            metric!(self, consensus_events.enter_new_round_qc),
            metric!(self, consensus_events.enter_new_round_tc),
            metric!(self, consensus_events.trigger_state_sync),
            // TxPoolEvents
            metric!(self, txpool_events.local_inserted_txns),
            metric!(self, txpool_events.dropped_txns),
            metric!(self, txpool_events.external_inserted_txns),
            // BlocktreeEvents
            metric!(self, blocktree_events.prune_success),
            metric!(self, blocktree_events.add_success),
            metric!(self, blocktree_events.add_dup),
            // BlocksyncEvents
            metric!(self, blocksync_events.blocksync_response_successful),
            metric!(self, blocksync_events.blocksync_response_failed),
            metric!(self, blocksync_events.blocksync_response_unexpected),
            metric!(self, blocksync_events.blocksync_request),
        ]
    }
}

// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use serde::{Deserialize, Serialize};

macro_rules! metrics {
    (
        $(
            (
                $class:ident,
                $class_field:ident,
                [$(($name:ident, $help:expr)),* $(,)?]
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
            pub fn metrics(&self) -> Vec<(&'static str, u64, &'static str)> {
                vec![
                    $(
                        $(
                            (
                                concat!("monad.state.", stringify!($class_field), ".", stringify!($name)),
                                self.$class_field.$name,
                                $help,
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
        NodeState,
        node_state,
        [(self_stake_bps, "Self stake in basis points")]
    ),
    (
        ValidationErrors,
        validation_errors,
        [
            (invalid_author, "Validation errors - invalid author"),
            (
                not_well_formed_sig,
                "Validation errors - malformed signature"
            ),
            (invalid_signature, "Validation errors - invalid signature"),
            (invalid_tc_round, "Validation errors - invalid TC round"),
            (
                duplicate_tc_tip_round,
                "Validation errors - duplicate TC tip round"
            ),
            (
                empty_signers_tc_tip_round,
                "Validation errors - empty signers TC tip"
            ),
            (
                too_many_tc_tip_round,
                "Validation errors - too many TC tip round"
            ),
            (insufficient_stake, "Validation errors - insufficient stake"),
            (
                invalid_seq_num,
                "Validation errors - invalid sequence number"
            ),
            (
                val_data_unavailable,
                "Validation errors - validator data unavailable"
            ),
            (
                signatures_duplicate_node,
                "Validation errors - duplicate node signatures"
            ),
            (
                invalid_vote_message,
                "Validation errors - invalid vote message"
            ),
            (invalid_version, "Validation errors - invalid version"),
            (invalid_epoch, "Validation errors - invalid epoch"),
        ]
    ),
    (
        ConsensusEvents,
        consensus_events,
        [
            (local_timeout, "Local timeout events"),
            (handle_proposal, "Proposal handling events"),
            (failed_txn_validation, "Failed transaction validation"),
            (failed_ts_validation, "Failed timestamp validation"),
            (
                invalid_proposal_round_leader,
                "Invalid proposal round leader"
            ),
            (out_of_order_proposals, "Out of order proposals"),
            (created_vote, "Votes created"),
            (old_vote_received, "Old votes received"),
            (future_vote_received, "Future votes received"),
            (vote_received, "Votes received"),
            (created_qc, "Quorum certificates created"),
            (old_remote_timeout, "Old remote timeout events"),
            (remote_timeout_msg, "Remote timeout messages received"),
            (
                remote_timeout_msg_with_tc,
                "Remote timeout messages with TC"
            ),
            (
                remote_timeout_msg_with_future_tc,
                "Remote timeout messages with future TC"
            ),
            (created_tc, "Timeout certificates created"),
            (process_old_qc, "Old QC processing events"),
            (process_qc, "QC processing events"),
            (process_old_tc, "Old TC processing events"),
            (process_tc, "TC processing events"),
            // TODO(andr-dev, PR): Add metric to differentiate emitting
            // TxPoolCommand::CreateProposal vs consensus state creating + broadcasting finalized
            // proposal
            (creating_proposal, "Proposal creation events"),
            (rx_execution_lagging, "Execution lagging events"),
            (rx_bad_state_root, "Bad state root events"),
            (rx_base_fee_error, "Base fee error events"),
            (proposal_with_tc, "Proposals with TC"),
            (
                failed_verify_randao_reveal_sig,
                "Failed RANDAO reveal signature verifications"
            ),
            (commit_block, "Block commit events"),
            (enter_new_round_qc, "New round entries via QC"),
            (enter_new_round_tc, "New round entries via TC"),
            (trigger_state_sync, "State sync trigger events"),
            (handle_round_recovery, "Round recovery handling events"),
            (
                invalid_round_recovery_leader,
                "Invalid round recovery leader"
            ),
            (handle_no_endorsement, "No endorsement handling events"),
            (
                old_no_endorsement_received,
                "Old no endorsement messages received"
            ),
            (
                future_no_endorsement_received,
                "Future no endorsement messages received"
            ),
            (created_nec, "No endorsement certificates created"),
            (handle_advance_round, "Advance round handling events"),
        ]
    ),
    (
        VoteDelay,
        vote_delay,
        [
            (
                ready_after_timer_start_p50_ms,
                "Vote delay ready-after-timer-start p50 in ms (5m rolling window)"
            ),
            (
                ready_after_timer_start_p90_ms,
                "Vote delay ready-after-timer-start p90 in ms (5m rolling window)"
            ),
            (
                ready_after_timer_start_p99_ms,
                "Vote delay ready-after-timer-start p99 in ms (5m rolling window)"
            )
        ]
    ),
    (
        BlocktreeEvents,
        blocktree_events,
        [
            (prune_success, "Successful blocktree prune operations"),
            (add_success, "Successful blocktree add operations"),
            (add_dup, "Duplicate blocktree add operations"),
        ]
    ),
    (
        BlocksyncEvents,
        blocksync_events,
        [
            (self_headers_request, "Self headers requests"),
            (self_payload_request, "Self payload requests"),
            (
                self_payload_requests_in_flight,
                "Self payload requests in flight"
            ),
            (headers_response_successful, "Successful headers responses"),
            (headers_response_failed, "Failed headers responses"),
            (headers_response_unexpected, "Unexpected headers responses"),
            (headers_validation_failed, "Failed headers validations"),
            (
                self_headers_response_successful,
                "Successful self headers responses"
            ),
            (
                self_headers_response_failed,
                "Failed self headers responses"
            ),
            (num_headers_received, "Headers received"),
            (payload_response_successful, "Successful payload responses"),
            (payload_response_failed, "Failed payload responses"),
            (payload_response_unexpected, "Unexpected payload responses"),
            (
                self_payload_response_successful,
                "Successful self payload responses"
            ),
            (
                self_payload_response_failed,
                "Failed self payload responses"
            ),
            (request_timeout, "Block sync request timeouts"),
            (
                request_failed_no_peers,
                "Block sync requests failed - no peers"
            ),
            (peer_headers_request, "Peer headers requests"),
            (
                peer_headers_request_successful,
                "Successful peer headers requests"
            ),
            (peer_headers_request_failed, "Failed peer headers requests"),
            (peer_payload_request, "Peer payload requests"),
            (
                peer_payload_request_successful,
                "Successful peer payload requests"
            ),
            (peer_payload_request_failed, "Failed peer payload requests"),
        ]
    ),
);

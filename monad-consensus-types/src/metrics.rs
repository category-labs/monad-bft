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
    (NodeState, node_state, [self_stake_bps,]),
    (
        ValidationErrors,
        validation_errors,
        [
            invalid_author,
            not_well_formed_sig,
            invalid_signature,
            invalid_tc_round,
            duplicate_tc_tip_round,
            insufficient_stake,
            invalid_seq_num,
            val_data_unavailable,
            signatures_duplicate_node,
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
            future_vote_received,
            vote_received,
            created_qc,
            old_remote_timeout,
            remote_timeout_msg,
            remote_timeout_msg_with_tc,
            created_tc,
            process_old_qc,
            process_qc,
            process_old_tc,
            process_tc,
            // TODO(andr-dev, PR): Add metric to differentiate emitting
            // TxPoolCommand::CreateProposal vs consensus state creating + broadcasting finalized
            // proposal
            creating_proposal,
            rx_execution_lagging,
            rx_bad_state_root,
            rx_base_fee_error,
            proposal_with_tc,
            failed_verify_randao_reveal_sig,
            commit_block,
            enter_new_round_qc,
            enter_new_round_tc,
            trigger_state_sync,
            handle_round_recovery,
            invalid_round_recovery_leader,
            handle_no_endorsement,
            old_no_endorsement_received,
            future_no_endorsement_received,
            created_nec,
            handle_advance_round
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
            request_failed_no_peers,
            peer_headers_request,
            peer_headers_request_successful,
            peer_headers_request_failed,
            peer_payload_request,
            peer_payload_request_successful,
            peer_payload_request_failed
        ]
    )
);

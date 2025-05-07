use alloy_consensus::{Transaction, TxReceipt};
use alloy_primitives::U256;
use monad_exec_events::{
    block_builder::{BlockBuilder, BlockUpdate, ExecutedBlockInfo},
    consensus_state_tracker::{ConsensusStateResult, ConsensusStateTracker},
    exec_event_stream::PollResult,
    exec_events::{ConsensusState, ExecEvent},
};
use monad_types::BlockId;
use tracing::{error, warn};

use crate::eth_json_types::{BlockCommitState, StreamEvent, StreamItem};

pub struct WebSocketServer {
    rx: flume::Receiver<PollResult>,
    block_builder: BlockBuilder,
    consensus_state_tracker: ConsensusStateTracker<Box<ExecutedBlockInfo>>,
    broadcaster: tokio::sync::broadcast::Sender<Event>,
}

enum ReferendumOutcome {
    Advanced(ConsensusState),
    Abandoned,
}

#[derive(Clone)]
pub enum Event {
    ProcessedBlock {
        header: alloy_rpc_types::eth::Header,
        block_id: BlockId,
        commit_state: BlockCommitState,
    },
    ProcessedLogs {
        logs: Vec<alloy_rpc_types::eth::Log>,
        header: alloy_rpc_types::eth::Header,
        block_id: BlockId,
        commit_state: BlockCommitState,
    },
    Event(StreamItem),
}

impl WebSocketServer {
    pub fn new(rx: flume::Receiver<PollResult>, broadcaster: tokio::sync::broadcast::Sender<Event>) -> Self {
        Self {
            rx,
            block_builder: BlockBuilder::new(),
            consensus_state_tracker: ConsensusStateTracker::new(),
            broadcaster,
        }
    }

    pub async fn run(mut self) {
        loop {
            if let Ok(poll_result) = self.rx.recv_async().await {
                match poll_result {
                    PollResult::Disconnected => {
                        error!("event stream disconnected; stopping all websocket sessions");
                        return;
                    }
                    PollResult::Gap {
                        last_read_seqno,
                        last_write_seqno,
                    } => {
                        warn!("event stream gap detected: {last_read_seqno} -> {last_write_seqno}");
                        self.process_event_stream_item(StreamEvent::StreamGap {
                            last_read_seqno,
                            next_seqno: last_write_seqno,
                        })
                        .await;
                        self.block_builder.drop_block();
                        self.consensus_state_tracker.reset_all();
                    }
                    PollResult::PayloadExpired {
                        expired_seqno,
                        last_write_seqno,
                        ..
                    } => {
                        warn!("event stream has expired payload at {expired_seqno} -> {last_write_seqno}");
                        self.process_event_stream_item(StreamEvent::StreamGap {
                            last_read_seqno: expired_seqno - 1,
                            next_seqno: last_write_seqno,
                        })
                        .await;
                        self.block_builder.drop_block();
                        self.consensus_state_tracker.reset_all();
                    }
                    PollResult::Ready { seqno, event } => {
                        if let ExecEvent::Referendum {
                            proposal_meta,
                            outcome,
                        } = &event
                        {
                            if let ConsensusStateResult::Finalization {
                                finalized_proposal: _,
                                abandoned_proposals,
                            } = self.consensus_state_tracker.update_proposal(
                                proposal_meta.block_number,
                                &proposal_meta.id,
                                *outcome,
                            ) {
                                for abandoned in abandoned_proposals {
                                    self.process_block_update(
                                        &abandoned.user_data,
                                        ReferendumOutcome::Abandoned,
                                    )
                                    .await;
                                    self.process_event_stream_item(
                                        StreamEvent::AbandonedProposal {
                                            proposal_meta: abandoned.proposal_meta,
                                        },
                                    )
                                    .await;
                                }
                            }

                            if let Some(opt_proposal_state) = self
                                .consensus_state_tracker
                                .get(proposal_meta.block_number, &proposal_meta.id)
                            {
                                self.process_block_update(
                                    &opt_proposal_state.user_data,
                                    ReferendumOutcome::Advanced(*outcome),
                                )
                                .await;
                            }
                        }

                        // TODO(ken): cloning this is a bad idea
                        self.process_event_stream_item(StreamEvent::ExecutionEvent {
                            seqno,
                            event: event.clone(),
                        })
                        .await;

                        match self.block_builder.try_append(event) {
                            Some(BlockUpdate::Failed(failed_info)) => {
                                error!("event stream received failed block, execution daemon is likely dead: {:?}",failed_info);
                                return;
                            }

                            Some(BlockUpdate::Executed(exec_info)) => {
                                let proposal_meta = exec_info.proposal_meta;
                                let consensus_state = exec_info.consensus_state;
                                assert_eq!(consensus_state, ConsensusState::Proposed);
                                if let ConsensusStateResult::Outstanding {
                                    updated_proposal, ..
                                } = self.consensus_state_tracker.add_proposal(
                                    proposal_meta,
                                    consensus_state,
                                    exec_info,
                                ) {
                                    self.process_block_update(
                                        &updated_proposal.user_data,
                                        ReferendumOutcome::Advanced(ConsensusState::Proposed),
                                    )
                                    .await
                                }
                            }

                            implicit_drop @ Some(BlockUpdate::ImplicitDrop { .. }) => {
                                // Because we explicitly call BlockBuilder::drop_block, we
                                // don't expect this to ever happen
                                panic!(
                                    "implicit_drop should never happen here, but it did: {:?}",
                                    implicit_drop.unwrap()
                                );
                            }

                            Some(BlockUpdate::OrphanedEvent(_))
                            | Some(BlockUpdate::NonBlockEvent(_))
                            | None => (),
                        }
                    }
                    PollResult::NotReady => unreachable!(),
                }
            }
        }
    }

    async fn process_block_update(
        &mut self,
        block_update: &ExecutedBlockInfo,
        referendum_outcome: ReferendumOutcome,
    ) {
        // Given a block update, convert it to a rpc block and logs.
        let difficulty = block_update.eth_header.difficulty;
        let block_hash = block_update.eth_block_hash;
        let block_number = block_update.eth_header.number;
        let block_timestamp = block_update.eth_header.timestamp;
        // TODO(ken): we don't know the block size in bytes, since
        //    we neither export that as a field in BLOCK_END, nor
        //    do we export the full RLP of the encoded block.
        //    Either could be done if anyone cares.
        let rpc_header = alloy_rpc_types::eth::Header {
            hash: block_hash,
            inner: block_update.eth_header.clone(),
            total_difficulty: Some(difficulty),
            size: Some(U256::ZERO),
        };
        let mut rpc_txns: Vec<alloy_rpc_types::eth::Transaction> = Vec::new();
        for txn_info in block_update.transactions.iter() {
            assert!(txn_info.is_some());
            if let Some(txn_info) = txn_info {
                let eff_gas_price = txn_info
                    .txn_envelope
                    .effective_gas_price(rpc_header.base_fee_per_gas.clone().take());
                rpc_txns.push(alloy_rpc_types::eth::Transaction {
                    inner: txn_info.txn_envelope.clone(),
                    block_hash: Some(block_update.eth_block_hash),
                    block_number: Some(rpc_header.number),
                    transaction_index: Some(txn_info.txn_index),
                    effective_gas_price: Some(eff_gas_price),
                    from: txn_info.sender,
                });
            }
        }

        let block = alloy_rpc_types::eth::Block {
            header: rpc_header,
            uncles: vec![], // TODO(ken): not currently available, only historical?
            transactions: alloy_rpc_types::eth::BlockTransactions::Full(rpc_txns),
            withdrawals: None,
        };

        // Create a list of logs
        let mut log_idx = 0;
        let logs: Vec<alloy_rpc_types::Log> = block_update
            .transactions
            .iter()
            .filter_map(|txn| {
                // If the transaction is None, we can skip it.
                let (receipt, tx_id, tx_index) = match txn {
                    Some(txn) => (
                        txn.receipt.clone(),
                        *txn.txn_envelope.tx_hash(),
                        txn.txn_index,
                    ),
                    None => return None,
                };

                let filtered_logs: Vec<alloy_primitives::Log> =
                    receipt.logs().to_owned().into_iter().collect();

                let rpc_logs = filtered_logs
                    .into_iter()
                    .map(|log| {
                        let rpc_log = alloy_rpc_types::Log {
                            inner: log,
                            block_hash: Some(block_hash),
                            block_number: Some(block_number),
                            block_timestamp: Some(block_timestamp),
                            transaction_hash: Some(tx_id),
                            transaction_index: Some(tx_index),
                            log_index: Some(log_idx),
                            ..Default::default()
                        };
                        log_idx += 1;
                        rpc_log
                    })
                    .collect::<Vec<alloy_rpc_types::Log>>();
                Some(rpc_logs)
            })
            .flatten()
            .collect();

        let commit_state = match referendum_outcome {
            ReferendumOutcome::Advanced(ConsensusState::Proposed) => BlockCommitState::Proposed,
            ReferendumOutcome::Advanced(ConsensusState::QC) => BlockCommitState::Voted,
            ReferendumOutcome::Advanced(ConsensusState::Finalized) => BlockCommitState::Finalized,
            ReferendumOutcome::Advanced(ConsensusState::Verified) => BlockCommitState::Verified,
            ReferendumOutcome::Abandoned => BlockCommitState::Abandoned,
        };

        if let Err(err) = self.broadcaster.send(Event::ProcessedBlock {
            header: block.header.clone(),
            block_id: BlockId(monad_types::Hash(block_update.proposal_meta.id.0 .0)),
            commit_state,
        }) {
            warn!("publish processed block send error: {err}");
        }

        if let Err(err) = self.broadcaster.send(Event::ProcessedLogs {
            logs,
            header: block.header.clone(),
            block_id: BlockId(monad_types::Hash(block_update.proposal_meta.id.0 .0)),
            commit_state,
        }) {
            warn!("publish processed log send error: {err}");
        }
    }

    async fn process_event_stream_item(&mut self, event: StreamEvent) {
        if let Err(err) = self.broadcaster.send(Event::Event(StreamItem {
            protocol_version: 1,
            event: event.clone(),
        })) {
            warn!("could not broadcast event stream item: {err}")
        }
    }
}

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

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use itertools::Itertools;
use monad_ethcall::{eth_header_layout, ChainId, EthHeaderLayout};
use monad_event_ring::{DecodedEventRing, EventNextResult};
use monad_exec_events::{
    BlockBuilderError, BlockCommitState, CommitStateBlockBuilder, CommitStateBlockUpdate,
    ExecEventRing, ExecutedBlock, ExecutedBlockBuilder,
};
use monad_types::BlockId;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use super::{EventServerClient, EventServerEvent, BROADCAST_CHANNEL_SIZE};
use crate::{
    handlers::ethcall_chain_id,
    types::{eth_json::MonadNotification, serialize::JsonSerialized},
};

pub struct EventServer<R>
where
    R: DecodedEventRing,
{
    event_ring: R,
    block_builder: CommitStateBlockBuilder,
    broadcast_tx: broadcast::Sender<EventServerEvent>,
}

impl EventServer<ExecEventRing> {
    pub fn start(event_ring: ExecEventRing) -> EventServerClient {
        let (broadcast_tx, _) = tokio::sync::broadcast::channel(BROADCAST_CHANNEL_SIZE);

        let this = Self {
            event_ring,
            block_builder: CommitStateBlockBuilder::new(ExecutedBlockBuilder::new(false)),
            broadcast_tx: broadcast_tx.clone(),
        };

        let handle = tokio::spawn(this.run());

        EventServerClient::new(broadcast_tx, handle)
    }

    async fn run(self) {
        let Self {
            event_ring,
            mut block_builder,
            broadcast_tx,
        } = self;

        let mut event_reader = event_ring.create_reader();

        loop {
            let event_descriptor = match event_reader.next_descriptor() {
                EventNextResult::Gap => {
                    warn!("EventServer event_reader gapped");

                    broadcast_event(&broadcast_tx, EventServerEvent::Gap);
                    event_reader.reset();
                    block_builder.reset();
                    continue;
                }
                EventNextResult::NotReady => {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    continue;
                }
                EventNextResult::Ready(event_descriptor) => event_descriptor,
            };

            let Some(result) = block_builder.process_event_descriptor(&event_descriptor) else {
                continue;
            };

            match result {
                Err(BlockBuilderError::Rejected) => {
                    unimplemented!();
                }
                Err(BlockBuilderError::PayloadExpired) => {
                    warn!("EventServer consensus state tracker gapped through payload expired");

                    broadcast_event(&broadcast_tx, EventServerEvent::Gap);
                    event_reader.reset();
                    block_builder.reset();
                    continue;
                }
                Err(BlockBuilderError::ImplicitDrop {
                    block,
                    reassembly_error,
                }) => {
                    unreachable!("Implicit drop: {reassembly_error:#?}\n{block:#?}");
                }
                Ok(CommitStateBlockUpdate {
                    block,
                    state,
                    abandoned,
                }) => handle_update(&broadcast_tx, block, state, abandoned),
            }
        }
    }
}

fn handle_update(
    broadcast_tx: &broadcast::Sender<EventServerEvent>,
    block: Arc<ExecutedBlock>,
    commit_state: BlockCommitState,
    abandoned: Vec<Arc<ExecutedBlock>>,
) {
    for abandoned in abandoned {
        debug!(
            "abandoned [round {}, seqnum {}]",
            abandoned.start.round, abandoned.start.block_tag.block_number
        );
    }

    broadcast_block_updates(broadcast_tx, block, commit_state);
}

fn broadcast_event(broadcast_tx: &broadcast::Sender<EventServerEvent>, event: EventServerEvent) {
    if broadcast_tx.send(event).is_err() {
        // TODO: The send method only produces an error
        // warn!("EventServer did not send event");
    }
}

fn broadcast_block_updates(
    broadcast_tx: &broadcast::Sender<EventServerEvent>,
    block: Arc<ExecutedBlock>,
    commit_state: BlockCommitState,
) {
    let block_id = BlockId(monad_types::Hash(block.start.block_tag.id.bytes));

    let mut header = block.to_alloy_rpc_header();
    match block_header_layout(&block) {
        Some(layout) => strip_fields_beyond_layout(&mut header.inner, layout),
        None => warn_header_layout_unknown(&block),
    }

    let serialized_monad_header = JsonSerialized::new_shared_with_map(
        MonadNotification {
            block_id,
            commit_state,
            data: header,
        },
        |notification| notification.map(JsonSerialized::new_shared),
    );

    let transactions = block
        .iter_alloy_rpc_txs()
        .zip_eq(block.iter_alloy_rpc_tx_receipts())
        .map(|(tx, tx_receipt)| {
            let logs = tx_receipt
                .logs()
                .iter()
                .map(|log| {
                    JsonSerialized::new_shared_with_map(
                        MonadNotification {
                            block_id,
                            commit_state,
                            data: log.clone(),
                        },
                        |notification| notification.map(JsonSerialized::new_shared),
                    )
                })
                .collect_vec();

            (
                JsonSerialized::new_shared(tx),
                JsonSerialized::new_shared(tx_receipt),
                logs.into_boxed_slice(),
            )
        })
        .collect_vec();

    broadcast_event(
        broadcast_tx,
        EventServerEvent::Block {
            commit_state,
            header: serialized_monad_header,
            transactions: Arc::new(transactions.into_boxed_slice()),
        },
    );
}

/// Least significant limb first.
fn chain_id_from_limbs(limbs: [u64; 4]) -> Option<ChainId> {
    let [low, high @ ..] = limbs;
    if high.iter().any(|limb| *limb != 0) {
        return None;
    }
    ethcall_chain_id(low)
}

fn block_header_layout(block: &ExecutedBlock) -> Option<EthHeaderLayout> {
    let chain = chain_id_from_limbs(block.start.chain_id.limbs)?;
    eth_header_layout(
        chain,
        block.start.eth_block_input.number,
        block.start.eth_block_input.timestamp,
    )
}

/// Makes the header match the block's RLP by clearing the fields its fork lacks.
fn strip_fields_beyond_layout(header: &mut alloy_consensus::Header, layout: EthHeaderLayout) {
    if layout < EthHeaderLayout::London {
        header.base_fee_per_gas = None;
    }
    if layout < EthHeaderLayout::Shanghai {
        header.withdrawals_root = None;
    }
    if layout < EthHeaderLayout::Cancun {
        header.blob_gas_used = None;
        header.excess_blob_gas = None;
        header.parent_beacon_block_root = None;
    }
    if layout < EthHeaderLayout::Prague {
        header.requests_hash = None;
    }
    if layout < EthHeaderLayout::Amsterdam {
        header.block_access_list_hash = None;
        header.slot_number = None;
    }
}

static HEADER_LAYOUT_UNKNOWN_WARNED: AtomicBool = AtomicBool::new(false);

fn warn_header_layout_unknown(block: &ExecutedBlock) {
    if HEADER_LAYOUT_UNKNOWN_WARNED.swap(true, Ordering::Relaxed) {
        return;
    }
    warn!(
        chain_id = %alloy_primitives::U256::from_limbs(block.start.chain_id.limbs),
        block_number = block.start.eth_block_input.number,
        "execution cannot report the header layout; serving headers with every optional field"
    );
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use monad_chain_config::MONAD_MAINNET_CHAIN_ID;
    use monad_event_ring::SnapshotEventRing;
    use monad_exec_events::ExecEventDecoder;
    use serde::{de::DeserializeOwned, Serialize};

    use super::*;
    use crate::{
        event::{EventServer, EventServerEvent},
        types::eth_json::MonadNotification,
    };

    impl EventServer<SnapshotEventRing<ExecEventDecoder>> {
        pub(crate) fn start_for_testing(
            snapshot_event_ring: SnapshotEventRing<ExecEventDecoder>,
        ) -> EventServerClient {
            Self::start_for_testing_with_delay(snapshot_event_ring, Duration::from_millis(1))
        }

        pub(crate) fn start_for_testing_with_delay(
            snapshot_event_ring: SnapshotEventRing<ExecEventDecoder>,
            delay: Duration,
        ) -> EventServerClient {
            let (broadcast_tx, _) = tokio::sync::broadcast::channel(1024);

            let this = Self {
                event_ring: snapshot_event_ring,
                block_builder: CommitStateBlockBuilder::new(ExecutedBlockBuilder::new(false)),
                broadcast_tx: broadcast_tx.clone(),
            };

            let handle = tokio::spawn(this.run_for_testing(delay));

            EventServerClient::new(broadcast_tx, handle)
        }

        async fn run_for_testing(self, delay: Duration) {
            tokio::time::sleep(delay).await;

            let Self {
                event_ring,
                mut block_builder,
                broadcast_tx,
            } = self;

            let mut event_reader = event_ring.create_reader();

            loop {
                let event_descriptor = match event_reader.next_descriptor() {
                    EventNextResult::Ready(event_descriptor) => event_descriptor,
                    EventNextResult::NotReady => break,
                    EventNextResult::Gap => {
                        unreachable!("SnapshotEventDescriptor cannot gap")
                    }
                };

                let Some(result) = block_builder.process_event_descriptor(&event_descriptor) else {
                    continue;
                };

                match result {
                    Err(BlockBuilderError::Rejected) => {
                        unimplemented!();
                    }
                    Err(BlockBuilderError::PayloadExpired) => {
                        unreachable!("SnapshotEventDescriptor payload cannot expire")
                    }
                    Err(BlockBuilderError::ImplicitDrop {
                        block,
                        reassembly_error,
                    }) => {
                        unreachable!("Implicit drop: {reassembly_error:#?}\n{block:#?}");
                    }
                    Ok(CommitStateBlockUpdate {
                        block,
                        state,
                        abandoned,
                    }) => handle_update(&broadcast_tx, block, state, abandoned),
                }
            }
        }
    }

    #[tokio::test]
    async fn testing_server() {
        let snapshot_event_ring = SnapshotEventRing::new_from_zstd_bytes(
            "TEST",
            include_bytes!(
                "../../../monad-execution/rust/crates/monad-exec-events/test/data/exec-events-emn-30b-15m/snapshot.zst"
            ),
            None,
        )
        .unwrap();

        let event_server_client = EventServer::start_for_testing(snapshot_event_ring);

        let mut subscription = event_server_client.subscribe().unwrap();

        let event = tokio::time::timeout(Duration::from_millis(10), subscription.recv())
            .await
            .unwrap()
            .unwrap();

        match event {
            EventServerEvent::Gap => {
                panic!("EventServer using snapshot should never produce a gap!")
            }
            EventServerEvent::Block { .. } => {}
        }
    }

    #[tokio::test]
    async fn json() {
        let snapshot_event_ring = SnapshotEventRing::new_from_zstd_bytes(
            "TEST",
            include_bytes!(
                "../../../monad-execution/rust/crates/monad-exec-events/test/data/exec-events-emn-30b-15m/snapshot.zst"
            ),
            None,
        )
        .unwrap();

        let event_server_client = EventServer::start_for_testing(snapshot_event_ring);

        let mut subscription = event_server_client.subscribe().unwrap();

        let event = tokio::time::timeout(Duration::from_millis(10), subscription.recv())
            .await
            .unwrap()
            .unwrap();

        let (commit_state, monad_header, transactions) = match event {
            EventServerEvent::Gap => {
                panic!("EventServer using snapshot should never produce a gap!")
            }
            EventServerEvent::Block {
                commit_state,
                header,
                transactions,
            } => (commit_state, header, transactions),
        };

        assert_eq!(commit_state, BlockCommitState::Proposed);

        assert_eq!(monad_header.data.inner.hash_slow(), monad_header.data.hash);

        assert_json::<_, MonadNotification<alloy_rpc_types::Header>>(
            &[&monad_header],
            include_str!(
                "../../../monad-execution/rust/crates/monad-exec-events/test/data/exec-events-emn-30b-15m/0.monad-header.json"
            ),
        );

        assert_json::<_, alloy_rpc_types::Header>(
            &[&monad_header.data],
            include_str!(
                "../../../monad-execution/rust/crates/monad-exec-events/test/data/exec-events-emn-30b-15m/0.header.json"
            ),
        );

        let (tx_first, tx_first_receipt, tx_first_monad_logs) = transactions.first().unwrap();

        assert_json::<_, alloy_rpc_types::Transaction>(
            &[&tx_first],
            include_str!(
                "../../../monad-execution/rust/crates/monad-exec-events/test/data/exec-events-emn-30b-15m/0.tx.0.json"
            ),
        );

        assert_json::<_, alloy_rpc_types::TransactionReceipt>(
            &[&tx_first_receipt],
            include_str!(
                "../../../monad-execution/rust/crates/monad-exec-events/test/data/exec-events-emn-30b-15m/0.tx-receipt.0.json"
            ),
        );

        assert!(tx_first_monad_logs.is_empty());

        let monad_log = transactions
            .iter()
            .flat_map(|(_, _, logs)| logs.iter())
            .next()
            .unwrap();

        assert_json::<_, MonadNotification<alloy_rpc_types::Log>>(
            &[&monad_log],
            include_str!(
                "../../../monad-execution/rust/crates/monad-exec-events/test/data/exec-events-emn-30b-15m/0.monad-log.0.json"
            ),
        );

        assert_json::<_, alloy_rpc_types::Log>(
            &[&monad_log.data],
            include_str!(
                "../../../monad-execution/rust/crates/monad-exec-events/test/data/exec-events-emn-30b-15m/0.log.0.json"
            ),
        );
    }

    fn header_with_every_optional_field() -> alloy_consensus::Header {
        alloy_consensus::Header {
            base_fee_per_gas: Some(1),
            withdrawals_root: Some(alloy_primitives::B256::repeat_byte(2)),
            blob_gas_used: Some(3),
            excess_blob_gas: Some(4),
            parent_beacon_block_root: Some(alloy_primitives::B256::repeat_byte(5)),
            requests_hash: Some(alloy_primitives::B256::repeat_byte(6)),
            block_access_list_hash: Some(alloy_primitives::B256::repeat_byte(7)),
            slot_number: Some(8),
            ..Default::default()
        }
    }

    #[test]
    fn strip_fields_beyond_layout_amsterdam_keeps_everything() {
        let mut header = header_with_every_optional_field();
        strip_fields_beyond_layout(&mut header, EthHeaderLayout::Amsterdam);
        assert_eq!(header, header_with_every_optional_field());
    }

    #[test]
    fn strip_fields_beyond_layout_prague_clears_the_amsterdam_pair() {
        let mut header = header_with_every_optional_field();
        strip_fields_beyond_layout(&mut header, EthHeaderLayout::Prague);
        assert!(header.base_fee_per_gas.is_some());
        assert!(header.withdrawals_root.is_some());
        assert!(header.blob_gas_used.is_some());
        assert!(header.excess_blob_gas.is_some());
        assert!(header.parent_beacon_block_root.is_some());
        assert!(header.requests_hash.is_some());
        assert!(header.block_access_list_hash.is_none());
        assert!(header.slot_number.is_none());
    }

    #[test]
    fn strip_fields_beyond_layout_cancun_clears_requests_hash() {
        let mut header = header_with_every_optional_field();
        strip_fields_beyond_layout(&mut header, EthHeaderLayout::Cancun);
        assert!(header.base_fee_per_gas.is_some());
        assert!(header.withdrawals_root.is_some());
        assert!(header.blob_gas_used.is_some());
        assert!(header.excess_blob_gas.is_some());
        assert!(header.parent_beacon_block_root.is_some());
        assert!(header.requests_hash.is_none());
        assert!(header.block_access_list_hash.is_none());
        assert!(header.slot_number.is_none());
    }

    #[test]
    fn strip_fields_beyond_layout_shanghai_clears_the_cancun_trio_together() {
        let mut header = header_with_every_optional_field();
        strip_fields_beyond_layout(&mut header, EthHeaderLayout::Shanghai);
        assert!(header.base_fee_per_gas.is_some());
        assert!(header.withdrawals_root.is_some());
        assert!(header.blob_gas_used.is_none());
        assert!(header.excess_blob_gas.is_none());
        assert!(header.parent_beacon_block_root.is_none());
        assert!(header.requests_hash.is_none());
        assert!(header.block_access_list_hash.is_none());
        assert!(header.slot_number.is_none());
    }

    #[test]
    fn strip_fields_beyond_layout_london_clears_withdrawals_root() {
        let mut header = header_with_every_optional_field();
        strip_fields_beyond_layout(&mut header, EthHeaderLayout::London);
        assert!(header.base_fee_per_gas.is_some());
        assert!(header.withdrawals_root.is_none());
        assert!(header.blob_gas_used.is_none());
        assert!(header.excess_blob_gas.is_none());
        assert!(header.parent_beacon_block_root.is_none());
        assert!(header.requests_hash.is_none());
        assert!(header.block_access_list_hash.is_none());
        assert!(header.slot_number.is_none());
    }

    #[test]
    fn strip_fields_beyond_layout_legacy_clears_base_fee() {
        let mut header = header_with_every_optional_field();
        strip_fields_beyond_layout(&mut header, EthHeaderLayout::Legacy);
        assert!(header.base_fee_per_gas.is_none());
        assert!(header.withdrawals_root.is_none());
        assert!(header.blob_gas_used.is_none());
        assert!(header.excess_blob_gas.is_none());
        assert!(header.parent_beacon_block_root.is_none());
        assert!(header.requests_hash.is_none());
        assert!(header.block_access_list_hash.is_none());
        assert!(header.slot_number.is_none());
    }

    #[test]
    fn strip_fields_beyond_layout_never_sets_a_field() {
        // Legacy runs every clearing branch.
        let mut header = alloy_consensus::Header::default();
        strip_fields_beyond_layout(&mut header, EthHeaderLayout::Legacy);
        assert_eq!(header, alloy_consensus::Header::default());
    }

    #[test]
    fn chain_id_from_limbs_accepts_known_chain_in_low_limb() {
        assert_eq!(
            chain_id_from_limbs([MONAD_MAINNET_CHAIN_ID, 0, 0, 0]),
            Some(ChainId::MonadMainnet)
        );
    }

    #[test]
    fn chain_id_from_limbs_rejects_nonzero_high_limbs() {
        assert_eq!(chain_id_from_limbs([MONAD_MAINNET_CHAIN_ID, 1, 0, 0]), None);
        assert_eq!(chain_id_from_limbs([MONAD_MAINNET_CHAIN_ID, 0, 1, 0]), None);
        assert_eq!(chain_id_from_limbs([MONAD_MAINNET_CHAIN_ID, 0, 0, 1]), None);
    }

    #[test]
    fn chain_id_from_limbs_rejects_unknown_chain() {
        assert_eq!(chain_id_from_limbs([42, 0, 0, 0]), None);
    }

    #[test]
    fn block_header_layout_monad_mainnet_prague_activation() {
        // All-zero bytes are a valid value for these plain C structs.
        let mut block = ExecutedBlock {
            start: unsafe { std::mem::zeroed() },
            end: unsafe { std::mem::zeroed() },
            txns: Box::default(),
        };
        block.start.chain_id.limbs = [MONAD_MAINNET_CHAIN_ID, 0, 0, 0];
        block.start.eth_block_input.number = 40_000_000;

        block.start.eth_block_input.timestamp = 1762266599;
        assert_eq!(block_header_layout(&block), Some(EthHeaderLayout::Cancun));

        block.start.eth_block_input.timestamp = 1762266600;
        assert_eq!(block_header_layout(&block), Some(EthHeaderLayout::Prague));
    }

    fn assert_json<T, E>(values: &[T], json: &'static str)
    where
        T: Serialize,
        E: DeserializeOwned,
    {
        for value in values {
            let str = serde_json::to_string(value).unwrap();

            assert!(!str.contains("serde"));
            assert!(!str.contains("RawValue"));
            assert!(!str.contains("$serde_json::private::RawValue"));

            assert_eq!(str, json);

            let _: E = serde_json::from_str(&str).unwrap();
        }
    }
}

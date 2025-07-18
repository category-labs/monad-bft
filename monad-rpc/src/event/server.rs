use std::{sync::Arc, time::Duration};

use itertools::Itertools;
use monad_event_ring::{DecodedEventRing, EventNextResult, SnapshotEventRing};
use monad_exec_events::{
    BlockBuilderError, BlockCommitState, CommitStateBlockBuilder, CommitStateBlockUpdate,
    ExecEventDecoder, ExecEventRing, ExecutedBlock,
};
use monad_types::BlockId;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use super::{EventServerClient, EventServerEvent, BROADCAST_CHANNEL_SIZE};
use crate::{eth_json_types::MonadNotification, serialize::JsonSerialized};

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
            block_builder: CommitStateBlockBuilder::default(),
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
                    block_builder.reset();
                    event_reader = event_ring.create_reader();
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
                    block_builder.reset();
                    event_reader = event_ring.create_reader();
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
        let (broadcast_tx, _) = tokio::sync::broadcast::channel(BROADCAST_CHANNEL_SIZE);

        let this = Self {
            event_ring: snapshot_event_ring,
            block_builder: CommitStateBlockBuilder::default(),
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

    let (serialized_header, serialized_block) = {
        let alloy_rpc_types::Block {
            header,
            uncles,
            transactions,
            withdrawals,
        } = block.to_alloy_rpc();

        let serialized_header = JsonSerialized::new_shared(header);

        (
            serialized_header.clone(),
            JsonSerialized::new_shared(alloy_rpc_types::Block {
                header: serialized_header,
                uncles,
                transactions,
                withdrawals,
            }),
        )
    };

    let logs = Arc::new(
        block
            .get_alloy_rpc_logs()
            .into_iter()
            .map(|log| {
                JsonSerialized::new_shared(MonadNotification {
                    block_id,
                    commit_state,
                    data: JsonSerialized::new_shared(log),
                })
            })
            .collect_vec(),
    );

    broadcast_event(
        broadcast_tx,
        EventServerEvent::Block {
            header: JsonSerialized::new_shared(MonadNotification {
                block_id,
                commit_state,
                data: serialized_header,
            }),
            block: JsonSerialized::new_shared(MonadNotification {
                block_id,
                commit_state,
                data: serialized_block,
            }),
            logs,
        },
    );
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use monad_event_ring::SnapshotEventRing;

    use crate::event::{EventServer, EventServerEvent};

    #[tokio::test]
    async fn testing_server() {
        let snapshot_event_ring = SnapshotEventRing::new_from_zstd_bytes(
            include_bytes!("../../../monad-exec-events/test/data/exec-events-emn-30b-15m.zst"),
            "TEST",
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
}

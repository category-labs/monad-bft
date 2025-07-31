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

use std::{
    collections::{BTreeMap, HashMap},
    io::ErrorKind,
    num::NonZero,
    path::{Path, PathBuf},
};

use futures_util::{Stream, StreamExt};
use inotify::{Inotify, WatchMask};
use lru::LruCache;
use monad_block_persist::{BlockPersist, FileBlockPersist, BLOCKDB_HEADERS_PATH};
use monad_consensus_types::{
    block::{ConsensusBlockHeader, ConsensusFullBlock},
    validator_data::ValidatorsConfig,
};
use monad_node_config::{
    ExecutionProtocolType, MonadNodeConfig, SignatureCollectionType, SignatureType,
};
use monad_types::{BlockId, Hash, Round, GENESIS_ROUND};
use monad_validator::{leader_election::LeaderElection, weighted_round_robin::WeightedRoundRobin};
use tracing::{error, info, warn};
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer},
    layer::SubscriberExt,
};

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::Registry::default().with(
        Layer::default()
            .json()
            .with_span_events(FmtSpan::NONE)
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(std::io::stdout)
            .with_ansi(false),
    );
    tracing::subscriber::set_global_default(subscriber).expect("unable to set default subscriber");

    let mut visited_blocks: LruCache<
        BlockId,
        ConsensusBlockHeader<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
    > = LruCache::new(NonZero::new(100).unwrap());

    let ledger_path: PathBuf = PathBuf::from("/monad/ledger");
    let node_config: MonadNodeConfig = toml::from_str(
        &std::fs::read_to_string("/monad/config/node.toml").expect("node.toml not found"),
    )
    .unwrap();
    let node_dns: HashMap<_, _> = node_config
        .bootstrap
        .peers
        .iter()
        .map(|peer| (peer.secp256k1_pubkey, peer.address.clone()))
        .collect();

    let mut epoch_validators = BTreeMap::default();

    let block_persist: FileBlockPersist<
        SignatureType,
        SignatureCollectionType,
        ExecutionProtocolType,
    > = FileBlockPersist::new(ledger_path.clone());

    let mut last_round = GENESIS_ROUND;
    let mut block_stream = Box::pin(new_blocks(&ledger_path));
    while let Some(mut next_block) = block_stream.next().await {
        let mut block_queue = Vec::new();
        loop {
            let next_block_id = next_block.get_id();
            if visited_blocks.contains(&next_block_id) {
                break;
            }
            block_queue.push(next_block.clone());
            if let Some(next_block_parent) =
                read_full_block(&block_persist, &next_block.get_parent_id())
            {
                next_block = next_block_parent;
            } else {
                break;
            }
        }

        for block in block_queue.into_iter().rev() {
            let now_ts = std::time::UNIX_EPOCH.elapsed().unwrap();

            let validators = epoch_validators
                .entry(block.get_epoch())
                .or_insert_with(|| {
                    let validators: ValidatorsConfig<SignatureCollectionType> =
                        ValidatorsConfig::read_from_path("/monad/config/validators.toml")
                            .unwrap_or_else(|err| panic!("failed to read validators.toml, or validators.toml corrupt. was this edited manually? err={:?}", err));
                    validators
                        .get_validator_set(&block.get_epoch())
                        .get_stakes()
                        .into_iter()
                        .collect()
                });

            for skipped_round in (last_round.0 + 1)
                .max(block.get_block_round().0 - 5)
                .min(block.get_block_round().0)
                ..block.get_block_round().0
            {
                let skipped_leader =
                    WeightedRoundRobin::default().get_leader(Round(skipped_round), validators);
                info!(
                    round =? skipped_round,
                    author =? skipped_leader,
                    now_ts_ms =? now_ts.as_millis(),
                    author_dns = node_dns.get(&skipped_leader.pubkey()).cloned().unwrap_or_default(),
                    "skipped_block"
                );
            }
            last_round = block.get_block_round();
            visited_blocks.put(block.get_id(), block.header().clone());

            info!(
                round =? block.get_block_round().0,
                epoch =? block.header().epoch.0,
                seq_num =? block.header().seq_num.0,
                num_tx =? block.body().execution_body.transactions.len(),
                author =? block.header().author,
                block_ts_ms =? block.header().timestamp_ns / 1_000_000,
                now_ts_ms =? now_ts.as_millis(),
                author_dns = node_dns.get(&block.header().author.pubkey()).cloned().unwrap_or_default(),
                "proposed_block"
            );
        }
        while epoch_validators.len() > 1_000 {
            epoch_validators.pop_first();
        }
    }
}

pub fn new_blocks(
    ledger_path: &Path,
) -> impl Stream<Item = ConsensusFullBlock<SignatureType, SignatureCollectionType, ExecutionProtocolType>>
{
    let inotify = Inotify::init().expect("error initializing inotify");
    inotify
        .watches()
        .add(
            {
                let mut headers_path = PathBuf::from(ledger_path);
                headers_path.push(BLOCKDB_HEADERS_PATH);
                headers_path
            },
            WatchMask::CLOSE_WRITE,
        )
        .expect("failed to watch bft_block_header_path");

    let inotify_buffer = [0; 1024];
    let inotify_events = inotify
        .into_event_stream(inotify_buffer)
        .expect("failed to create inotify event stream");

    let block_persist: FileBlockPersist<
        SignatureType,
        SignatureCollectionType,
        ExecutionProtocolType,
    > = FileBlockPersist::new(ledger_path.to_owned());

    inotify_events.filter_map(move |maybe_event| {
        // hack because filter_map takes in an impl Future<Option<_>>
        let result = (|| {
            let event = match maybe_event {
                Ok(event) => event,
                Err(err) if err.kind() == ErrorKind::InvalidInput => {
                    warn!(
                        ?err,
                        "ErrorKind::InvalidInput, are blocks being produced faster than indexer?"
                    );
                    return None;
                }
                Err(err) => {
                    error!(?err, "inotify error while reading events");
                    panic!("inotify error while reading events")
                }
            };
            let event_name = event.name?;
            let filename = event_name.to_str()?;
            let block_id = BlockId(Hash(hex::decode(filename).ok()?.try_into().ok()?));
            read_full_block(&block_persist, &block_id)
        })();
        async move { result }
    })
}

fn read_full_block(
    block_persist: &FileBlockPersist<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
    block_id: &BlockId,
) -> Option<ConsensusFullBlock<SignatureType, SignatureCollectionType, ExecutionProtocolType>> {
    let header = block_persist.read_bft_header(block_id).ok()?;
    let body = block_persist.read_bft_body(&header.block_body_id).ok()?;
    ConsensusFullBlock::new(header, body).ok()
}

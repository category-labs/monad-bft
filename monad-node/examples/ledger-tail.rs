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

use clap::{CommandFactory, FromArgMatches, Parser};
use futures_util::{Stream, StreamExt};
use inotify::{Inotify, WatchMask};
use lru::LruCache;
use monad_block_persist::{BlockPersist, FileBlockPersist, BLOCKDB_HEADERS_PATH};
use monad_consensus_types::{
    block::{ConsensusBlockHeader, ConsensusFullBlock},
    quorum_certificate::QuorumCertificate,
    validator_data::ValidatorsConfig,
    RoundCertificate,
};
use monad_node_config::{
    ExecutionProtocolType, ForkpointConfig, MonadNodeConfig, SignatureCollectionType, SignatureType,
};
use monad_types::{BlockId, Round};
use monad_validator::{leader_election::LeaderElection, weighted_round_robin::WeightedRoundRobin};
use tracing::{error, info, warn};
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer},
    layer::SubscriberExt,
};

const MAX_REWIND_QUEUE_LEN: usize = 100;

type BlockHeader =
    ConsensusBlockHeader<SignatureType, SignatureCollectionType, ExecutionProtocolType>;

struct CachedBlock {
    header: BlockHeader,
    is_finalized: bool,
}
impl CachedBlock {
    fn new(header: BlockHeader) -> Self {
        Self {
            header,
            is_finalized: false,
        }
    }
}
type CachedBlocks = LruCache<BlockId, CachedBlock>;

fn finalize_block(
    blocks: &mut CachedBlocks,
    finalized_block_id: &BlockId,
    finalize_fn: impl Fn(&BlockHeader),
) {
    let mut finalized_block_id = *finalized_block_id;
    let mut to_finalize = Vec::new();
    while let Some(finalized_block) = blocks.peek_mut(&finalized_block_id) {
        if finalized_block.is_finalized {
            // already finalized
            break;
        }
        finalized_block.is_finalized = true;
        let finalized_block_header = &finalized_block.header;
        to_finalize.push(finalized_block_header.clone());
        // try finalizing parent
        finalized_block_id = finalized_block_header.get_parent_id();
    }

    // finalize in reverse order (oldest first)
    for finalized_block_header in to_finalize.iter().rev() {
        finalize_fn(finalized_block_header)
    }
}

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

    let mut cmd = Cli::command();
    let Cli {
        forkpoint_path,
        ledger_path,
        node_config_path,
        validators_path,
    } = Cli::from_arg_matches_mut(&mut cmd.get_matches_mut()).expect("unable to parse CLI");

    let mut visited_blocks: CachedBlocks = LruCache::new(NonZero::new(100).unwrap());

    let node_config: MonadNodeConfig =
        toml::from_str(&std::fs::read_to_string(&node_config_path).expect("node.toml not found"))
            .unwrap();
    let addresses: HashMap<_, _> = node_config
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

    let mut last_high_certificate = RoundCertificate::Qc(QuorumCertificate::genesis_qc());
    let mut last_voted_head: Option<BlockId> = None;
    let mut tip_stream = Box::pin(latest_tip_stream(&forkpoint_path, &ledger_path));
    while let Some((high_certificate, proposed_head, voted_head)) = tip_stream.next().await {
        let now_ts = std::time::UNIX_EPOCH.elapsed().unwrap();

        if last_high_certificate != high_certificate {
            if let RoundCertificate::Tc(tc) = &high_certificate {
                let validators = epoch_validators
                .entry(tc.epoch)
                .or_insert_with(|| {
                    let validators: ValidatorsConfig<SignatureCollectionType> =
                        ValidatorsConfig::read_from_path(&validators_path)
                            .unwrap_or_else(|err| panic!("failed to read validators.toml, or validators.toml corrupt. was this edited manually? err={:?}", err));
                    validators
                        .get_validator_set(&tc.epoch)
                        .get_stakes()
                        .into_iter()
                        .collect()
                });

                let skipped_round = tc.round;
                let skipped_leader =
                    WeightedRoundRobin::default().get_leader(skipped_round, validators);
                info!(
                    round =? skipped_round,
                    author =? skipped_leader,
                    now_ts_ms =? now_ts.as_millis(),
                    author_address = addresses.get(&skipped_leader.pubkey()).cloned().unwrap_or_default(),
                    "timeout"
                );
            }
        }

        let mut block_queue = Vec::new();
        let mut next_block_id = if high_certificate.qc().get_round() >= proposed_head.block_round {
            high_certificate.qc().get_block_id()
        } else {
            proposed_head.get_id()
        };
        loop {
            if visited_blocks.contains(&next_block_id) || block_queue.len() > MAX_REWIND_QUEUE_LEN {
                break;
            }
            if let Ok(next_block_header) = block_persist.read_bft_header(&next_block_id) {
                next_block_id = next_block_header.get_parent_id();
                block_queue.push(next_block_header);
            } else {
                break;
            }
        }

        for block_header in block_queue.into_iter().rev() {
            let Ok(block_body) = block_persist.read_bft_body(&block_header.block_body_id) else {
                // no body for block header, so skip and move on
                continue;
            };
            let block = ConsensusFullBlock::new(block_header, block_body).expect("block is valid");

            visited_blocks.put(block.get_id(), CachedBlock::new(block.header().clone()));

            info!(
                round =? block.get_block_round().0,
                parent_round =? block.get_qc().get_round().0,
                epoch =? block.header().epoch.0,
                seq_num =? block.header().seq_num.0,
                num_tx =? block.body().execution_body.transactions.len(),
                author =? block.header().author,
                block_ts_ms =? block.header().timestamp_ns / 1_000_000,
                now_ts_ms =? now_ts.as_millis(),
                author_address = addresses.get(&block.header().author.pubkey()).cloned().unwrap_or_default(),
                "proposed_block"
            );
        }

        // Emit voted_block when voted_head changes
        let voted_block_id = voted_head.get_id();
        if last_voted_head.as_ref() != Some(&voted_block_id) {
            info!(
                round =? voted_head.block_round.0,
                parent_round =? voted_head.qc.get_round().0,
                epoch =? voted_head.epoch.0,
                seq_num =? voted_head.seq_num.0,
                author =? voted_head.author,
                block_ts_ms =? voted_head.timestamp_ns / 1_000_000,
                now_ts_ms =? now_ts.as_millis(),
                author_address = addresses.get(&voted_head.author.pubkey()).cloned().unwrap_or_default(),
                "voted_block"
            );
            last_voted_head = Some(voted_block_id);
        }

        if last_high_certificate != high_certificate {
            if let RoundCertificate::Qc(qc) = &high_certificate {
                if let Some(parent_block) = visited_blocks.peek(&qc.get_block_id()) {
                    let parent_qc = parent_block.header.qc.clone();
                    if qc.get_round() == parent_qc.get_round() + Round(1) {
                        // commit rule passed
                        finalize_block(
                            &mut visited_blocks,
                            &parent_qc.get_block_id(),
                            |finalized_block_header| {
                                info!(
                                    round =? finalized_block_header.block_round.0,
                                    parent_round =? finalized_block_header.qc.get_round().0,
                                    epoch =? finalized_block_header.epoch.0,
                                    seq_num =? finalized_block_header.seq_num.0,
                                    author =? finalized_block_header.author,
                                    block_ts_ms =? finalized_block_header.timestamp_ns / 1_000_000,
                                    now_ts_ms =? now_ts.as_millis(),
                                    author_address = addresses.get(&finalized_block_header.author.pubkey()).cloned().unwrap_or_default(),
                                    "finalized_block"
                                )
                            },
                        );
                    }
                }
            }
        }

        last_high_certificate = high_certificate.clone();
        while epoch_validators.len() > 1_000 {
            epoch_validators.pop_first();
        }
    }
}

pub fn latest_tip_stream(
    forkpoint_path: &Path,
    ledger_path: &Path,
) -> impl Stream<
    Item = (
        RoundCertificate<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
        BlockHeader,
        BlockHeader,
    ),
> {
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
        .expect("failed to watch ledger headers path");
    inotify
        .watches()
        .add(
            {
                let mut forkpoint_dir = PathBuf::from(forkpoint_path);
                forkpoint_dir.pop();
                forkpoint_dir
            },
            WatchMask::CLOSE_WRITE | WatchMask::MOVE,
        )
        .expect("failed to watch forkpoint path");
    inotify
        .watches()
        .add(ledger_path, WatchMask::CLOSE_WRITE | WatchMask::CREATE)
        .expect("failed to watch ledger path for voted_head");

    let inotify_buffer = [0; 1024];
    let inotify_events = inotify
        .into_event_stream(inotify_buffer)
        .expect("failed to create inotify event stream");

    let block_persist: FileBlockPersist<
        SignatureType,
        SignatureCollectionType,
        ExecutionProtocolType,
    > = FileBlockPersist::new(ledger_path.to_owned());

    let forkpoint_path = forkpoint_path.to_owned();

    inotify_events.filter_map(move |maybe_event| {
        let result = (|| match maybe_event {
            Ok(_event) => {
                let forkpoint_config = ForkpointConfig::try_parse_bytes(
                    &forkpoint_path.to_string_lossy(),
                    &std::fs::read(&forkpoint_path).ok()?,
                )
                .ok()?;
                let proposed_head = block_persist.read_proposed_head_bft_header().ok()?;
                let voted_head = block_persist.read_voted_head_bft_header().ok()?;
                Some((forkpoint_config.high_certificate, proposed_head, voted_head))
            }
            Err(err) if err.kind() == ErrorKind::InvalidInput => {
                warn!(
                    ?err,
                    "ErrorKind::InvalidInput, are files being produced faster than indexer?"
                );
                None
            }
            Err(err) => {
                error!(?err, "inotify error while reading events");
                panic!("inotify error while reading events")
            }
        })();
        async move { result }
    })
}

#[derive(Debug, Parser)]
#[command(about, long_about = None)]
pub struct Cli {
    #[arg(long, default_value = "/monad/ledger")]
    pub ledger_path: PathBuf,

    #[arg(long, default_value = "/monad/config/forkpoint/forkpoint.toml")]
    pub forkpoint_path: PathBuf,

    #[arg(long, default_value = "/monad/config/node.toml")]
    pub node_config_path: PathBuf,

    #[arg(long, default_value = "/monad/config/validators/validators.toml")]
    pub validators_path: PathBuf,
}

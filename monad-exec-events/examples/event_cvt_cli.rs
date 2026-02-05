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

use std::{io::Read, path::PathBuf};

use clap::Parser;
use monad_event_ring::{DecodedEventRing, EventNextResult};
use monad_exec_events::{
    BlockCommitState, CommitStateBlockBuilder, CommitStateBlockUpdate, ExecSnapshotEventRing,
    ExecutedBlock,
};

#[derive(Parser)]
#[command(about = "Event cross-validation test utility")]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(clap::Subcommand, Clone, Debug)]
pub enum Command {
    #[command(about = "Dump CVT updates to stdout")]
    Dump {
        #[arg(help = "path to the expected value JSON or ring shared memory snapshot file")]
        input_file: PathBuf,

        #[arg(short, long, help = "aggregate all updates into an array first")]
        array: bool,
    },

    #[command(about = "Run CVT test (compare execution event stream to ground truth)")]
    Compare {
        #[arg(long, help = "path of the expected value JSON file")]
        expected: PathBuf,

        #[arg(long, help = "path of the event ring snapshot file")]
        event_ring_snapshot: PathBuf,
    },
}

fn main() {
    match Cli::parse().cmd {
        Command::Dump { input_file, array } => dump_cvt_updates(input_file, array),
        Command::Compare {
            expected,
            event_ring_snapshot,
        } => run_cvt_test(expected, event_ring_snapshot),
    }
}

/// Attributes of a block proposal used by the consensus algorithm
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockTag {
    /// If the proposal is accepted, the block will be commited to the
    /// blockchain with this height
    pub block_number: u64,
    /// Unique ID that distinguishes different block proposals competing
    /// for the same block_number
    pub id: alloy_primitives::B256,
}

impl BlockTag {
    fn new_from_executed_block(block: &ExecutedBlock) -> Self {
        BlockTag {
            block_number: block.start.block_tag.block_number,
            id: alloy_primitives::FixedBytes::from(block.start.block_tag.id.bytes),
        }
    }
}

/// Collection of all event information aggregated during the execution
/// of a single block
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutedBlockInfo {
    pub block_tag: BlockTag,
    pub chain_id: alloy_primitives::ChainId,
    pub commit_state: BlockCommitState,
    pub eth_block_hash: alloy_primitives::B256,
    pub eth_header: alloy_consensus::Header,
    pub transactions: Box<[TransactionInfo]>,
}

/// All info about each transaction that occurs in a block
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionInfo {
    pub txn_index: alloy_primitives::TxIndex,
    pub txn_envelope: alloy_consensus::TxEnvelope,
    pub sender: alloy_primitives::Address,
    pub receipt: alloy_consensus::Receipt,
    pub txn_gas_used: u128,
}

/// Represents a single event in the cross-validation test. The CVT test
/// is defined in the following way:
///
/// - The execution daemon can export a JSON view of what it thinks a Rust
///   CrossValidationTestUpdate should look like. Because all the major
///   structures in the `exec_event_test_util.rs` module use
///   #[derive(serde::Deserialize)], Rust can easily load the serialized
///   C++ "ground truth."
///
/// - A test starts by loading a persisted snapshot of the event ring file
///   that has been saved to disk, using the event ring's "test utility"
///   module. This snapshot should contain the same low-level event stream for
///   which C++ exported the "ground truth" JSON for CrossValidationTestUpdate
///   objects.
///
/// - The CrossValidationTestStream uses the ExecEventStream, BlockBuilder,
///   and ConsensusStateTracker objects to reassemble the persisted event
///   stream into its own view of the CrossValidationTestUpdate stream.
///
/// - As a final step, we zip the two Vec<CrossValidationTestUpdate> inputs
///   together and check that each pair matches exactly
///
/// The idea behind the cross-validation test is that we're computing the
/// same thing in two very different ways, one of which is very circuitous
/// (execution recorder -> shared memory -> Rust event library -> Rust
/// reassembly library), the other of which is direct and does not use any
/// of the same code.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CrossValidationTestUpdate {
    Executed(Box<ExecutedBlockInfo>),
    Referendum {
        block_tag: BlockTag,
        outcome: BlockCommitState,
        superseded_proposals: Box<[BlockTag]>,
    },
    UnknownProposal {
        block_number: u64,
        block_id: alloy_primitives::B256,
        commit_state: BlockCommitState,
    },
    UnexpectedEventError(String),
}

impl CrossValidationTestUpdate {
    fn new_from_executed_block(commit_state: BlockCommitState, block: &ExecutedBlock) -> Self {
        Self::Executed(Box::new(ExecutedBlockInfo {
            commit_state,
            block_tag: BlockTag::new_from_executed_block(block),
            chain_id: alloy_primitives::U256::from_limbs(block.start.chain_id.limbs)
                .try_into()
                .unwrap(),
            eth_header: block.to_alloy_header(),
            eth_block_hash: alloy_primitives::B256::from(block.end.eth_block_hash.bytes),
            transactions: block
                .txns
                .to_vec()
                .into_iter()
                .enumerate()
                .fold(
                    (Vec::default(), 0u128),
                    |(mut txns, mut cumulative_gas_used), (txn_index, txn)| {
                        cumulative_gas_used += txn.output.receipt.gas_used as u128;

                        txns.push(TransactionInfo {
                            txn_index: txn_index as u64,
                            txn_envelope: txn.to_alloy(),
                            sender: alloy_primitives::Address::from(txn.sender.bytes),
                            receipt: alloy_consensus::Receipt {
                                status: alloy_consensus::Eip658Value::Eip658(
                                    txn.output.receipt.status,
                                ),
                                cumulative_gas_used,
                                logs: txn.to_alloy_logs(),
                            },
                            txn_gas_used: txn.output.receipt.gas_used as u128,
                        });

                        (txns, cumulative_gas_used)
                    },
                )
                .0
                .into_boxed_slice(),
        }))
    }
}

enum CvtInput {
    Expected(Box<[CrossValidationTestUpdate]>),
    Actual(ExecSnapshotEventRing),
}

fn open_cvt_input_file(file_name: PathBuf) -> CvtInput {
    let error_name = file_name.to_str().unwrap();

    let mut input_file = std::fs::File::open(&file_name)
        .expect(&format!("open of CVT input file `{error_name}` failed"));

    let mut buf = Vec::default();
    input_file.read_to_end(&mut buf).unwrap();

    if u32::from_le_bytes(buf[0..4].try_into().unwrap()) == zstd::zstd_safe::MAGICNUMBER {
        const MAX_FILE_SIZE: usize = 1 << 30;

        let zstd_buf = zstd::bulk::decompress(&buf, MAX_FILE_SIZE)
            .expect(&format!("zstd decompression of {error_name} failed"));

        const RING_MAGIC_4: [u8; 4] = [b'R', b'I', b'N', b'G'];

        if zstd_buf.as_slice()[..4] == RING_MAGIC_4 {
            return CvtInput::Actual(
                ExecSnapshotEventRing::new_from_zstd_bytes(&buf, error_name).unwrap(),
            );
        }

        buf = zstd_buf;
    }

    CvtInput::Expected(serde_json::from_slice(&buf).unwrap())
}

fn dump_cvt_updates(file_path: PathBuf, array: bool) {
    let cvt_updates = match open_cvt_input_file(file_path) {
        CvtInput::Expected(updates) => updates,
        CvtInput::Actual(snapshot_event_ring) => read_actual_block_updates(snapshot_event_ring),
    };

    if array {
        println!("{}", serde_json::to_string_pretty(&cvt_updates).unwrap());
    } else {
        for update in cvt_updates {
            println!("{}", serde_json::to_string_pretty(&update).unwrap());
        }
    }
}

fn run_cvt_test(path_expected: PathBuf, path_event_ring_snapshot: PathBuf) {
    let expected_block_updates: Box<[CrossValidationTestUpdate]> =
        match open_cvt_input_file(path_expected) {
            CvtInput::Expected(expected) => expected,
            CvtInput::Actual(_) => {
                panic!("Expected JSON input file is an event ring snapshot file!");
            }
        };

    let actual_block_updates: Box<[CrossValidationTestUpdate]> =
        match open_cvt_input_file(path_event_ring_snapshot) {
            CvtInput::Actual(snapshot_event_ring) => read_actual_block_updates(snapshot_event_ring),
            CvtInput::Expected(_) => {
                panic!("Event ring snapshot file is an expected JSON file!");
            }
        };

    let max_num_updates = expected_block_updates.len().max(actual_block_updates.len());
    let mut total_updates_matched = 0;

    for idx in 0..max_num_updates {
        let expected_update = expected_block_updates.get(idx);
        let actual_update = actual_block_updates.get(idx);

        if expected_update.is_some() && actual_update.is_none() {
            break;
        }

        if expected_update == actual_update {
            total_updates_matched += 1;
            continue;
        }

        let expected = serde_json::to_string_pretty(&expected_update).unwrap();
        let actual = serde_json::to_string_pretty(&actual_update).unwrap();

        println!("expected:\n{expected}");
        println!("actual:\n{actual}");
        panic!("failure after {idx} successfully matched updates");
    }

    println!("matched ({total_updates_matched}/{max_num_updates}) block updates");

    let skipped = max_num_updates.checked_sub(total_updates_matched).unwrap();
    if skipped > 0 {
        println!(" -> skipped {skipped} updates since actual ran out");
    }
}

fn read_actual_block_updates(
    event_ring: ExecSnapshotEventRing,
) -> Box<[CrossValidationTestUpdate]> {
    let mut actual_block_updates: Vec<CrossValidationTestUpdate> = Vec::new();

    let mut event_reader = event_ring.create_reader();

    let mut commit_state_block_builder = CommitStateBlockBuilder::default();

    loop {
        let event_descriptor = match event_reader.next_descriptor() {
            EventNextResult::Gap => panic!("snapshot gapped"),
            EventNextResult::NotReady => break,
            EventNextResult::Ready(event_descriptor) => event_descriptor,
        };

        let CommitStateBlockUpdate {
            block,
            state,
            abandoned,
        } = match commit_state_block_builder.process_event_descriptor(&event_descriptor) {
            None => continue,
            Some(Err(err)) => {
                panic!("err: {err:#?}");
            }
            Some(Ok(update)) => update,
        };

        if state == BlockCommitState::Proposed {
            actual_block_updates.push(CrossValidationTestUpdate::new_from_executed_block(
                state, &block,
            ));
        } else {
            actual_block_updates.push(CrossValidationTestUpdate::Referendum {
                block_tag: BlockTag::new_from_executed_block(&block),
                outcome: state,
                superseded_proposals: abandoned
                    .into_iter()
                    .map(|block| BlockTag::new_from_executed_block(&block))
                    .collect(),
            });
        }
    }

    actual_block_updates.into_boxed_slice()
}

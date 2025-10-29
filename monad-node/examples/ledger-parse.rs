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
    cmp::Ordering, collections::BTreeMap, error::Error, fs::File, io::Read, path::Path,
    path::PathBuf, time::SystemTime,
};

use alloy_consensus::transaction::Transaction;
use alloy_primitives::{hex, Address, U256};
use alloy_rlp::Encodable;
use clap::{CommandFactory, FromArgMatches, Parser};
use monad_bls::{BlsSignature, BlsSignatureCollection};
use monad_chain_config::{
    revision::MonadChainRevision, MonadChainConfig, MONAD_DEVNET_CHAIN_ID, MONAD_TESTNET_CHAIN_ID,
};
use monad_consensus_types::{
    block::{ConsensusBlockHeader, ConsensusFullBlock, PassthruBlockPolicy},
    block_validator::BlockValidator,
    metrics::Metrics,
    payload::ConsensusBlockBody,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_block_validator::EthBlockValidator;
use monad_eth_types::EthExecutionProtocol;
use monad_secp::{PubKey, SecpSignature};
use monad_state_backend::NopStateBackend;
use monad_types::{BlockId, Round, SeqNum};
use serde_json::json;

const ONE_ETHER: u128 = 1_000_000_000_000_000_000;

type ProductionBlockHeader =
    ConsensusBlockHeader<SecpSignature, BlsSignatureCollection<PubKey>, EthExecutionProtocol>;
type ProductionBlockBody = ConsensusBlockBody<EthExecutionProtocol>;
type ProductionBlockPolicy = EthBlockPolicy<
    SecpSignature,
    BlsSignatureCollection<PubKey>,
    MonadChainConfig,
    MonadChainRevision,
>;

fn main() -> Result<(), Box<dyn Error>> {
    let mut cmd = Cli::command();
    let Cli {
        block_id,
        chain_id,
        headers_dir,
        bodies_dir,
        validate,
        round,
        seqnum,
    } = Cli::from_arg_matches_mut(&mut cmd.get_matches_mut()).expect("failed to parse cli");
    let block_id = clean_block_id(block_id);

    // return the block header id for the target round
    if let Some(round) = round {
        let mut files = get_files_with_timestamps(&headers_dir).unwrap();
        files.sort_by_key(|(_, time)| *time);

        let comparator = |header: &ProductionBlockHeader| -> Ordering {
            if header.block_round == Round(round) {
                Ordering::Equal
            } else if header.block_round < Round(round) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        };

        match search_block_headers(&files, comparator) {
            Some(blockid) => println!("{}", blockid.0),
            None => println!("no block found"),
        }

        return Ok(());
    }

    // return the block header id for the target seqnum
    if let Some(seqnum) = seqnum {
        let mut files = get_files_with_timestamps(&headers_dir).unwrap();
        files.sort_by_key(|(_, time)| *time);

        let comparator = |header: &ProductionBlockHeader| -> Ordering {
            if header.seq_num == SeqNum(seqnum) {
                Ordering::Equal
            } else if header.seq_num < SeqNum(seqnum) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        };

        match search_block_headers(&files, comparator) {
            Some(blockid) => println!("{}", blockid.0),
            None => println!("no block found"),
        }

        return Ok(());
    }

    let header_path = PathBuf::from(headers_dir).join(block_id);
    let header = get_header(header_path, true).unwrap();
    let body = get_body(&header, &bodies_dir, true).unwrap();

    if validate {
        let validator: EthBlockValidator<SecpSignature, BlsSignatureCollection<PubKey>> =
            EthBlockValidator::default();
        let chain_config = MonadChainConfig::new(chain_id, None).unwrap();
        let round = header.block_round;
        let seq_num = header.seq_num;
        BlockValidator::<
            SecpSignature,
            BlsSignatureCollection<PubKey>,
            EthExecutionProtocol,
            EthBlockPolicy<
                SecpSignature,
                BlsSignatureCollection<PubKey>,
                MonadChainConfig,
                MonadChainRevision,
            >,
            NopStateBackend,
            MonadChainConfig,
            MonadChainRevision,
        >::validate(
            &validator,
            header,
            body,
            None,
            &chain_config,
            &mut Metrics::default(),
        )
        .unwrap();

        // todo: need to get range of blocks create RootInfo
        let block_policy = ProductionBlockPolicy::new(seq_num, 3);
        let sender = Address::default();
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(sender, U256::from(15 * ONE_ETHER))]),
            ..Default::default()
        };
    }

    Ok(())
}

#[derive(Debug, Parser)]
pub struct Cli {
    #[arg(short, long, default_value = "finalized_head")]
    pub block_id: String,

    #[arg(long, default_value_t = MONAD_DEVNET_CHAIN_ID)]
    pub chain_id: u64,

    #[arg(long, default_value = "ledger/headers")]
    pub headers_dir: String,

    #[arg(long, default_value = "ledger/bodies")]
    pub bodies_dir: String,

    #[arg(long)]
    pub validate: bool,

    #[arg(short, long)]
    pub round: Option<u64>,

    #[arg(short, long)]
    pub seqnum: Option<u64>,
}

fn load_file(path: PathBuf) -> Result<Vec<u8>, std::io::Error> {
    let mut file = File::open(path)?;
    let file_size = file.metadata()?.len();
    let mut buf = vec![0; file_size as usize];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

fn decode_header(buf: &Vec<u8>) -> Result<ProductionBlockHeader, std::io::Error> {
    alloy_rlp::decode_exact(buf)
        .map_err(|e| std::io::Error::other(format!("rlp decode block header failed: {:?}", e)))
}

fn decode_body(buf: &Vec<u8>) -> Result<ProductionBlockBody, std::io::Error> {
    alloy_rlp::decode_exact(buf)
        .map_err(|e| std::io::Error::other(format!("rlp decode block body failed: {:?}", e)))
}

fn get_header(path: PathBuf, json_output: bool) -> Result<ProductionBlockHeader, std::io::Error> {
    let header_raw = load_file(path)?;
    let header = decode_header(&header_raw)?;
    if json_output {
        let output = json!({
            "object": "ConsensusBlockHeader",
            "block_id": header.get_id(),
            "parent_block_id": header.get_parent_id(),
            "block_body_id": header.block_body_id,
            "round": header.block_round,
            "seq_num": header.seq_num,
        });
        println!("{}", serde_json::to_string_pretty(&output).unwrap());
    }
    Ok(header)
}

fn get_body(
    header: &ProductionBlockHeader,
    bodies_dir: &str,
    json_output: bool,
) -> Result<ProductionBlockBody, std::io::Error> {
    let body_id = hex::encode(header.block_body_id.0 .0);
    let path = PathBuf::from(bodies_dir).join(body_id);
    let body_raw = load_file(path)?;
    let body = decode_body(&body_raw)?;
    if json_output {
        let total_network_bytes: usize = body
            .execution_body
            .transactions
            .iter()
            .map(|txn| txn.length())
            .sum();
        let num_create = body
            .execution_body
            .transactions
            .iter()
            .filter(|txn| (*txn).is_create())
            .count();
        let num_access_list = body
            .execution_body
            .transactions
            .iter()
            .filter(|txn| (*txn).access_list().is_some())
            .count();
        let num_7702 = body
            .execution_body
            .transactions
            .iter()
            .filter(|txn| (*txn).authorization_list().is_some())
            .count();

        let output = json!({
            "object": "ConsensusBlockBody",
            "num_txns": body.execution_body.transactions.len(),
            "num_create_txns": num_create,
            "num_access_list_txns": num_access_list,
            "num_7702_auth_list_txns": num_7702,
            "total_bytes": total_network_bytes,
        });
        println!("{}", serde_json::to_string_pretty(&output).unwrap());
    }
    Ok(body)
}

fn get_files_with_timestamps(dir: &str) -> std::io::Result<Vec<(String, SystemTime)>> {
    let mut files = Vec::new();

    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();

        if path.is_file() {
            let metadata = std::fs::metadata(&path).unwrap();
            let modified = metadata.modified().unwrap();
            files.push((path.to_string_lossy().to_string(), modified));
        }
    }

    Ok(files)
}

fn search_block_headers<F>(files: &[(String, SystemTime)], comparator: F) -> Option<BlockId>
where
    F: Fn(&ProductionBlockHeader) -> Ordering,
{
    let mut left = 0;
    let mut right = files.len();

    while left < right {
        let mid = left + (right - left) / 2;
        let (path, _) = &files[mid];

        let header = get_header(path.into(), false).unwrap();

        match comparator(&header) {
            Ordering::Equal => return Some(header.get_id()),
            Ordering::Less => left = mid + 1,
            Ordering::Greater => right = mid,
        }
    }

    None
}

fn clean_block_id(block_id: String) -> String {
    block_id.strip_prefix("0x").unwrap_or(&block_id).to_string()
}

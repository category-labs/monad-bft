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
    cmp::Ordering, error::Error, fs::File, io::Read, path::Path, path::PathBuf, time::SystemTime,
};

use alloy_primitives::hex;
use clap::{CommandFactory, FromArgMatches, Parser};
use monad_bls::{BlsSignature, BlsSignatureCollection};
use monad_chain_config::{
    revision::MonadChainRevision, MonadChainConfig, MONAD_DEVNET_CHAIN_ID, MONAD_TESTNET_CHAIN_ID,
};
use monad_consensus_types::{
    block::{ConsensusBlockHeader, ConsensusFullBlock, PassthruBlockPolicy},
    block_validator::BlockValidator,
    payload::ConsensusBlockBody,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_block_validator::EthBlockValidator;
use monad_eth_types::EthExecutionProtocol;
use monad_secp::{PubKey, SecpSignature};
use monad_state_backend::NopStateBackend;
use monad_types::{BlockId, Round};

type ProductionBlockHeader =
    ConsensusBlockHeader<SecpSignature, BlsSignatureCollection<PubKey>, EthExecutionProtocol>;
type ProductionBlockBody = ConsensusBlockBody<EthExecutionProtocol>;

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

    if let Some(round) = round {
        // return the block header id for the round
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

    if seqnum.is_some() {
        // return the block header id for the seqnum

        return Ok(());
    }

    let header_path = PathBuf::from(headers_dir).join(block_id);
    let header = get_header(header_path, true).unwrap();
    let body = get_body(&header, &bodies_dir, true).unwrap();

    if validate {
        let validator: EthBlockValidator<SecpSignature, BlsSignatureCollection<PubKey>> =
            EthBlockValidator::default();
        let chain_config = MonadChainConfig::new(chain_id, None).unwrap();
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
        >::validate(&validator, header, body, None, &chain_config)
        .unwrap();
    }

    Ok(())
}

#[derive(Debug, Parser)]
pub struct Cli {
    #[arg(long, default_value = "finalized_head")]
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

fn get_header(path: PathBuf, print_output: bool) -> Result<ProductionBlockHeader, std::io::Error> {
    let header_raw = load_file(path)?;
    let header = decode_header(&header_raw)?;
    if print_output {
        println!("recovered block header");
        println!("---");
        println!(
            "round={:?}, seq_num={:?}",
            header.block_round, header.seq_num
        );
    }
    Ok(header)
}

fn get_body(
    header: &ProductionBlockHeader,
    bodies_dir: &str,
    print_output: bool,
) -> Result<ProductionBlockBody, std::io::Error> {
    let body_id = hex::encode(header.block_body_id.0 .0);
    let path = PathBuf::from(bodies_dir).join(body_id);
    let body_raw = load_file(path)?;
    let body = decode_body(&body_raw)?;
    if print_output {
        println!("recovered block body");
        println!("---");
        println!("num_txns={:?}", body.execution_body.transactions.len());
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

use std::{error::Error, path::PathBuf};

use alloy_consensus::Header;
use alloy_rlp::Decodable;
use clap::{Parser, Subcommand};
use monad_triedb_utils::{
    key::{create_triedb_key, KeyInput, Version},
    TriedbReader,
};
use monad_types::{Round, SeqNum};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    pub triedb_path: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Account {
        seq_num: u64,
        proposed_round: Option<u64>,
        eth_address: String,
    },
    LatestFinalized,
    EarliestFinalized,
    BftId {
        seq_num: u64,
        proposed_round: u64,
    },
    EthHeader {
        seq_num: u64,
        proposed_round: Option<u64>,
    },
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let reader = TriedbReader::try_new(&args.triedb_path)
        .unwrap_or_else(|| panic!("failed to open triedb path: {:?}", &args.triedb_path));

    match args.command {
        Commands::Account {
            seq_num,
            proposed_round,
            eth_address,
        } => {
            let seq_num = SeqNum(seq_num);
            let version = match proposed_round {
                // hack because subcommand doesn't generate option properly
                None | Some(0) => Version::Finalized,
                Some(round) => Version::Proposal(Round(round)),
            };
            let eth_address: [u8; 20] = hex::decode(&eth_address)?.as_slice().try_into()?;
            println!(
                "version={:?}, account={:?}",
                version,
                reader.get_account(&seq_num, version, &eth_address)
            );
        }
        Commands::LatestFinalized => {
            println!("{:?}", reader.get_latest_finalized_block());
        }
        Commands::EarliestFinalized => {
            println!("{:?}", reader.get_earliest_finalized_block());
        }
        Commands::BftId {
            seq_num,
            proposed_round,
        } => {
            let seq_num = SeqNum(seq_num);
            let round = Round(proposed_round);
            println!("{:?}", reader.get_bft_id(&seq_num, &round));
        }
        Commands::EthHeader {
            seq_num,
            proposed_round,
        } => {
            let seq_num = SeqNum(seq_num);
            let version = match proposed_round {
                // hack because subcommand doesn't generate option properly
                None | Some(0) => Version::Finalized,
                Some(round) => Version::Proposal(Round(round)),
            };

            let (triedb_key, key_len_nibbles) = create_triedb_key(version, KeyInput::BlockHeader);
            let maybe_eth_header_bytes = unsafe {
                reader
                    .handle()
                    .read(&triedb_key, key_len_nibbles, seq_num.0)
            };
            let maybe_eth_header = maybe_eth_header_bytes.map(|eth_header_bytes| {
                let mut rlp_buf = eth_header_bytes.as_slice();
                Header::decode(&mut rlp_buf).expect("invalid rlp eth header")
            });

            println!("version={:?}, eth_header={:?}", version, maybe_eth_header);
        }
    };

    Ok(())
}

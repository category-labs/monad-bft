use std::{error::Error, path::PathBuf};

use alloy_primitives::{Uint, B256};
use clap::Parser;
use monad_eth_testutil::secret_to_eth_address;
use monad_eth_types::EthAddress;
use monad_triedb_utils::TriedbReader;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    pub triedb_path: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let triedb_path = args.triedb_path.unwrap_or(PathBuf::new());

    let reader = TriedbReader::try_new(triedb_path.as_path()).unwrap();

    let block_id = reader.get_latest_block();
    println!("latest block id: {}", block_id);

    let num_accounts = 1000;
    let eth_addresses: Vec<EthAddress> = (0..num_accounts)
        .map(|idx| secret_to_eth_address(B256::from(Uint::from(10000 + idx))))
        .collect();
    let results = reader
        .get_accounts_async(eth_addresses.iter(), block_id)
        .unwrap();
    println!("{:?}", results);

    Ok(())
}

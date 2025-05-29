use std::{error::Error, path::PathBuf};

use alloy_primitives::{Uint, B256};
use clap::Parser;
use monad_eth_testutil::secret_to_eth_address;
use monad_triedb_utils::{key::Version, TriedbReader};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    pub triedb_path: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let triedb_path = args.triedb_path.unwrap_or(PathBuf::new());

    let reader = TriedbReader::new(&[triedb_path; 1]).unwrap();

    let block_id = reader
        .get_latest_finalized_block()
        .expect("latest_finalized_block is none");
    println!("latest block id: {:?}", block_id);

    let num_accounts = 1000;
    let eth_addresses: Vec<_> = (0..num_accounts)
        .map(|idx| secret_to_eth_address(B256::from(Uint::from(10000 + idx))))
        .collect();
    let results = reader
        .get_accounts_async(&block_id, Version::Finalized, eth_addresses.iter())
        .unwrap();
    println!("{:?}", results);

    Ok(())
}

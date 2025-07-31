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

    let reader = TriedbReader::try_new(triedb_path.as_path()).unwrap();

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

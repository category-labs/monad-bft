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

use std::path::Path;

use monad_pq_chorus::sig::MlDsa44Signer;
use monad_pq_node::{
    RunError,
    config::{PqNodeConfig, load_ml_dsa_seed},
    init_logging, run_node,
};

const PUBLIC_KEY_FLAG: &str = "--ml-dsa-public-key";

#[tokio::main]
async fn main() -> Result<(), RunError> {
    init_logging();

    let usage = format!("usage: monad-pq-node <node.toml> | {PUBLIC_KEY_FLAG} <seed-file>");
    let mut args = std::env::args().skip(1);
    let first = args.next().ok_or(usage.clone())?;
    if first == PUBLIC_KEY_FLAG {
        // the [[quantum.validators]] public_key of a secret_key_file
        let path = args.next().ok_or(usage)?;
        let seed = load_ml_dsa_seed(Path::new(&path))?;
        println!(
            "{}",
            hex::encode(MlDsa44Signer::from_seed(&seed).public_key().to_bytes())
        );
        return Ok(());
    }
    let config = PqNodeConfig::load(first)?;

    tokio::select! {
        result = run_node(config) => result,
        _ = tokio::signal::ctrl_c() => Ok(()),
    }
}

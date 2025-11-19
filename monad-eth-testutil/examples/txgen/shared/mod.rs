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

use alloy_primitives::{Address, TxHash};
use alloy_rpc_client::ReqwestClient;
use tokio::time::sleep;

use crate::prelude::*;

pub mod blockstream;
pub mod ecmul;
pub mod eip7702;
pub mod erc20;
pub mod eth_json_rpc;
pub mod key_pool;
pub mod nft_sale;
pub mod private_key;
pub mod uniswap;

async fn ensure_contract_deployed(
    client: &ReqwestClient,
    addr: Address,
    hash: &TxHash,
) -> Result<()> {
    let mut timeout = Duration::from_millis(200);
    for _ in 0..10 {
        info!(
            "Waiting {}ms for contract to be deployed...",
            timeout.as_millis()
        );
        sleep(timeout).await;

        if let Ok(receipt) = client.get_transaction_receipt(hash).await {
            info!(receipt = ?receipt, "Contract deployment receipt");
            return Ok(());
        }

        let code = client.get_code(&addr).await?;
        if code != "0x" {
            info!(addr = addr.to_string(), "Deployed contract");
            return Ok(());
        }

        // else exponential backoff
        timeout *= 2;
    }

    Err(eyre::eyre!(
        "Failed to deployed contract {}",
        addr.to_string()
    ))
}

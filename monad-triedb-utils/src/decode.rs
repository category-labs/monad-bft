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

use alloy_primitives::U256;
use alloy_rlp::Decodable;
use monad_eth_types::{AccountCodeOrHash, EthAccount};
use tracing::warn;

pub fn rlp_decode_account(account_rlp: Vec<u8>) -> Option<EthAccount> {
    let mut buf = account_rlp.as_slice();
    let Ok(mut buf) = alloy_rlp::Header::decode_bytes(&mut buf, true) else {
        warn!("rlp decode failed: {:?}", buf);
        return None;
    };

    // address (currently not needed)
    let Ok(_) = <[u8; 20]>::decode(&mut buf) else {
        warn!("rlp address decode failed: {:?}", buf);
        return None;
    };

    // account incarnation decode (currently not needed)
    let Ok(_) = u64::decode(&mut buf) else {
        warn!("rlp incarnation decode failed: {:?}", buf);
        return None;
    };

    let Ok(nonce) = u64::decode(&mut buf) else {
        warn!("rlp nonce decode failed: {:?}", buf);

        return None;
    };
    let Ok(balance) = U256::decode(&mut buf) else {
        warn!("rlp balance decode failed: {:?}", buf);
        return None;
    };

    let code_or_hash = match AccountCodeOrHash::decode(&mut buf) {
        Ok(v) => v,
        Err(_e) => {
            return None;
        }
    };

    Some(EthAccount {
        nonce,
        balance,
        code_or_hash,
        is_delegated: false,
    })
}

pub fn rlp_decode_storage_slot(storage_rlp: Vec<u8>) -> Option<[u8; 32]> {
    let mut buf = storage_rlp.as_slice();
    let Ok(mut buf) = alloy_rlp::Header::decode_bytes(&mut buf, true) else {
        warn!("rlp decode failed: {:?}", buf);
        return None;
    };

    // storage key (currently not needed)
    let Ok(_) = U256::decode(&mut buf) else {
        warn!("rlp storage key decode failed: {:?}", buf);
        return None;
    };

    // storage value
    match U256::decode(&mut buf) {
        Ok(res) => {
            let mut storage_value = [0_u8; 32];
            for (byte, storage) in res
                .to_be_bytes_vec()
                .into_iter()
                .zip(storage_value.iter_mut())
            {
                *storage = byte;
            }
            Some(storage_value)
        }
        Err(e) => {
            warn!("rlp storage value decode failed: {:?}", e);
            None
        }
    }
}

pub fn rlp_decode_transaction_location(transaction_location_rlp: Vec<u8>) -> Option<(u64, u64)> {
    let mut buf = transaction_location_rlp.as_slice();

    let Ok(mut buf) = alloy_rlp::Header::decode_bytes(&mut buf, true) else {
        warn!("rlp decode failed: {:?}", buf);
        return None;
    };

    let Ok(block_num) = u64::decode(&mut buf) else {
        warn!("rlp block number decode failed: {:?}", buf);
        return None;
    };

    let Ok(tx_index) = u64::decode(&mut buf) else {
        warn!("rlp transaction index decode failed: {:?}", buf);
        return None;
    };

    Some((block_num, tx_index))
}

pub fn rlp_decode_block_num(block_num_rlp: Vec<u8>) -> Option<u64> {
    let mut buf = block_num_rlp.as_slice();

    let Ok(block_num) = u64::decode(&mut buf) else {
        warn!("rlp block number decode failed: {:?}", buf);
        return None;
    };

    Some(block_num)
}

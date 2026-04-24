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
use monad_eth_types::EthAccount;
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

    let is_delegated = false;
    let code_hash = if buf.is_empty() {
        None
    } else {
        match <[u8; 32]>::decode(&mut buf) {
            Ok(x) => Some(x),
            Err(e) => {
                warn!("rlp code_hash decode failed: {:?}", e);
                return None;
            }
        }
    };

    Some(EthAccount {
        nonce,
        balance,
        code_hash,
        is_delegated,
    })
}

pub fn rlp_decode_storage_slot(storage_rlp: Vec<u8>) -> Option<[u8; 32]> {
    if let Some(storage_value) = rlp_decode_storage_slot_with_key(&storage_rlp) {
        return Some(storage_value);
    }

    rlp_decode_storage_value_only(storage_rlp.as_slice())
}

fn rlp_decode_storage_slot_with_key(storage_rlp: &[u8]) -> Option<[u8; 32]> {
    let mut buf = storage_rlp;
    let Ok(mut buf) = alloy_rlp::Header::decode_bytes(&mut buf, true) else {
        return None;
    };

    // storage key (currently not needed)
    let Ok(_) = U256::decode(&mut buf) else {
        return None;
    };

    // storage value
    decode_storage_value(&mut buf)
}

fn rlp_decode_storage_value_only(storage_rlp: &[u8]) -> Option<[u8; 32]> {
    let mut buf = storage_rlp.as_slice();
    let Ok(mut buf) = alloy_rlp::Header::decode_bytes(&mut buf, true) else {
        warn!("rlp decode failed: {:?}", buf);
        return None;
    };

    decode_storage_value(&mut buf)
}

fn decode_storage_value(buf: &mut &[u8]) -> Option<[u8; 32]> {
    match U256::decode(buf) {
        Ok(res) => {
            if !buf.is_empty() {
                warn!("rlp storage value had trailing bytes: {:?}", buf);
                return None;
            }
            Some(res.to_be_bytes())
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

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;
    use alloy_rlp::{Encodable, Header};

    use super::rlp_decode_storage_slot;

    #[test]
    fn decodes_storage_slot_with_key_and_value() {
        let mut payload = Vec::new();
        U256::ZERO.encode(&mut payload);
        U256::from(0x1234_u64).encode(&mut payload);

        let mut encoded = Vec::new();
        Header {
            list: true,
            payload_length: payload.len(),
        }
        .encode(&mut encoded);
        encoded.extend_from_slice(&payload);

        assert_eq!(
            rlp_decode_storage_slot(encoded),
            Some(U256::from(0x1234_u64).to_be_bytes())
        );
    }

    #[test]
    fn decodes_storage_value_only() {
        let mut encoded = Vec::new();
        U256::from(0x1234_u64).encode(&mut encoded);

        assert_eq!(
            rlp_decode_storage_slot(encoded),
            Some(U256::from(0x1234_u64).to_be_bytes())
        );
    }

    #[test]
    fn decodes_zero_storage_value_only() {
        let mut encoded = Vec::new();
        U256::ZERO.encode(&mut encoded);

        assert_eq!(rlp_decode_storage_slot(encoded), Some([0_u8; 32]));
    }
}

pub fn rlp_decode_block_num(block_num_rlp: Vec<u8>) -> Option<u64> {
    let mut buf = block_num_rlp.as_slice();

    let Ok(block_num) = u64::decode(&mut buf) else {
        warn!("rlp block number decode failed: {:?}", buf);
        return None;
    };

    Some(block_num)
}

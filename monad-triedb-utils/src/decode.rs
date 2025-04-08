use alloy_primitives::{B256, U256};
use alloy_rlp::Decodable;
use monad_eth_types::EthAccount;
use tracing::warn;

pub fn rlp_decode_account(account_rlp: impl AsRef<[u8]>) -> Option<EthAccount> {
    let mut buf = account_rlp.as_ref();
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
        code_hash: code_hash.map(B256::from),
    })
}

pub fn rlp_decode_storage_slot(storage_rlp: impl AsRef<[u8]>) -> Option<[u8; 32]> {
    let mut buf = storage_rlp.as_ref();
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

pub fn rlp_decode_transaction_location(
    transaction_location_rlp: impl AsRef<[u8]>,
) -> Option<(u64, u64)> {
    let mut buf = transaction_location_rlp.as_ref();

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

pub fn rlp_decode_block_num(block_num_rlp: impl AsRef<[u8]>) -> Option<u64> {
    let mut buf = block_num_rlp.as_ref();

    let Ok(block_num) = u64::decode(&mut buf) else {
        warn!("rlp block number decode failed: {:?}", buf);
        return None;
    };

    Some(block_num)
}

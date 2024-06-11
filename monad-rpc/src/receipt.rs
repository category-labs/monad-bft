use alloy_primitives::{Address, Bloom, Bytes};
use alloy_rlp::{Decodable, RlpDecodable, RlpEncodable};
use reth_primitives::{B256, U64, U8};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, RlpEncodable, RlpDecodable, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReceiptDetails {
    #[rlp(skip)]
    #[rlp(default)]
    pub tx_type: U8,
    pub status: U64,
    pub cumulative_gas_used: U64,
    pub logs_bloom: Bloom,
    pub logs: Vec<ReceiptLog>,
}

#[derive(Debug, Clone, RlpDecodable, RlpEncodable, Serialize, Deserialize)]
pub struct ReceiptLog {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
}

pub fn decode_receipt(rlp_buf: &mut &[u8]) -> alloy_rlp::Result<ReceiptDetails> {
    let tx_type = decode_tx_type(rlp_buf)?;
    let receipt = ReceiptDetails::decode(rlp_buf)?;
    Ok(ReceiptDetails { tx_type, ..receipt })
}

pub fn decode_tx_type(rlp_buf: &mut &[u8]) -> Result<U8, alloy_rlp::Error> {
    match rlp_buf.first() {
        None => Err(alloy_rlp::Error::InputTooShort),
        Some(&x) if x < 0xc0 => {
            // first byte represents transaction type
            let tx_type = match x {
                1 => 1, // EIP2930
                2 => 2, // EIP1559
                // TODO: add support for EIP4844
                _ => return Err(alloy_rlp::Error::Custom("InvalidTxnType")),
            };
            *rlp_buf = &rlp_buf[1..]; // advance the buffer
            Ok(U8::from(tx_type))
        }
        Some(_) => Ok(U8::from(0)), // legacy transactions do not have first byte as transaction type
    }
}

#[cfg(test)]
mod tests {
    use reth_primitives::U8;

    use crate::hex;

    #[test]
    fn decode_receipt() {
        let rlp = hex::decode("0x02f90109018301ab80b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c0").unwrap();
        let mut rlp_buf = rlp.as_slice();
        let receipt = super::decode_receipt(&mut rlp_buf).unwrap();
        assert_eq!(receipt.tx_type, U8::from(2));
    }
}

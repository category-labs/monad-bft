use serde::{Deserialize, Deserializer, Serializer};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct EvmBlockHeader {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
    pub ommers_hash: [u8; 32],
    pub beneficiary: [u8; 20],
    pub state_root: [u8; 32],
    pub transactions_root: [u8; 32],
    pub receipts_root: [u8; 32],
    #[serde(
        serialize_with = "serialize_logs_bloom",
        deserialize_with = "deserialize_logs_bloom"
    )]
    pub logs_bloom: [[u8; 64]; 4],
    pub difficulty: [u8; 32],
    pub gas_limit: u64,
    pub gas_used: u64,
    pub timestamp: u64,
    pub extra_data: Vec<u8>,
    pub mix_hash: [u8; 32],
    pub nonce: [u8; 8],
    pub base_fee_per_gas: Option<[u8; 32]>,
    pub withdrawals_root: Option<[u8; 32]>,
    pub blob_gas_used: Option<u64>,
    pub excess_blob_gas: Option<u64>,
    pub parent_beacon_block_root: Option<[u8; 32]>,
    pub requests_hash: Option<[u8; 32]>,
}

impl EvmBlockHeader {
    pub fn minimal(number: u64, hash: [u8; 32], parent_hash: [u8; 32]) -> Self {
        Self {
            number,
            hash,
            parent_hash,
            ommers_hash: [0; 32],
            beneficiary: [0; 20],
            state_root: [0; 32],
            transactions_root: [0; 32],
            receipts_root: [0; 32],
            logs_bloom: [[0; 64]; 4],
            difficulty: [0; 32],
            gas_limit: 0,
            gas_used: 0,
            timestamp: 0,
            extra_data: Vec::new(),
            mix_hash: [0; 32],
            nonce: [0; 8],
            base_fee_per_gas: None,
            withdrawals_root: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            parent_beacon_block_root: None,
            requests_hash: None,
        }
    }
}

fn serialize_logs_bloom<S>(
    logs_bloom: &[[u8; 64]; 4],
    serializer: S,
) -> core::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut bytes = [0u8; 256];
    for (index, chunk) in logs_bloom.iter().enumerate() {
        let start = index * 64;
        let end = start + 64;
        bytes[start..end].copy_from_slice(chunk);
    }
    serializer.serialize_bytes(&bytes)
}

fn deserialize_logs_bloom<'de, D>(deserializer: D) -> core::result::Result<[[u8; 64]; 4], D::Error>
where
    D: Deserializer<'de>,
{
    let bytes = <Vec<u8>>::deserialize(deserializer)?;
    if bytes.len() != 256 {
        return Err(serde::de::Error::invalid_length(bytes.len(), &"256 bytes"));
    }

    let mut logs_bloom = [[0u8; 64]; 4];
    for (index, chunk) in logs_bloom.iter_mut().enumerate() {
        let start = index * 64;
        let end = start + 64;
        chunk.copy_from_slice(&bytes[start..end]);
    }
    Ok(logs_bloom)
}

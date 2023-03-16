// sha3 keccak-256 hasher -> replace with monad-crypto
extern crate crypto;
use self::crypto::digest::Digest;
use self::crypto::sha3::Sha3;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
pub struct BlockHeight(pub i64);

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct H256([u8; 32]);

impl AsRef<[u8]> for H256 {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

// TODO: replace serde with protobuf/add protobuf encoding support
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct BlockPart {
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct BlockMeta {
    pub height: BlockHeight,
    pub data: Vec<u8>,
    pub total_parts: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct QC {
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct DoubleQC {
    pub qc1: QC,
    pub qc2: QC,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct TC {
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Block {
    pub meta: BlockMeta,
    pub parts: Vec<BlockPart>,
    pub qc: DoubleQC,
}

// to be replaced with monad-crypto
pub trait Hashable {
    fn hash(&self) -> H256;
}

impl Hashable for Block {
    fn hash(&self) -> H256 {
        let mut hasher = Sha3::keccak256();
        // a placeholder impl; may not be the right fields to hash
        hasher.input(bincode::serialize(&self.meta).unwrap().as_ref());
        hasher.input(bincode::serialize(&self.parts).unwrap().as_ref());
        let mut result: H256 = H256::default();
        hasher.result(&mut result.0);
        result
    }
}

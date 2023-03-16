use crate::{
    store::Result,
    types::{BlockHeight, BlockMeta, BlockPart, DoubleQC, H256, TC},
};
use ordcode;
use rocksdb::{ColumnFamily, WriteBatch as RocksWriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
// TODO: create an abstraction around database to fit different underlying interface

// ColumnFamily names
const BLOCK_PART_CF: &str = "block_part";
const BLOCK_META_CF: &str = "block_meta";
const QC_CF: &str = "qc";
const TC_CF: &str = "tc";
const BLOCK_HEIGHT_CF: &str = "block_height";
// ColumnFamily list
pub const CFS: [&'static str; 5] = [BLOCK_PART_CF, BLOCK_META_CF, QC_CF, TC_CF, BLOCK_HEIGHT_CF];

pub trait Column {
    const NAME: &'static str;
    type KeyType;
    type ValueType;

    fn cf_handle(db: &DB) -> &ColumnFamily {
        db.cf_handle(Self::NAME).unwrap()
    }
    // ordcode preserves the lexicogrpahic ordering of values
    fn ser_key(key: &Self::KeyType) -> Vec<u8>
    where
        <Self as Column>::KeyType: Serialize,
    {
        // Note: serializing with descending lexicographic order is particularly useful for key-value databases like rocksdb, where reverse iteration is slower than forward iteration.
        // https://docs.rs/ordcode/latest/ordcode/index.html
        ordcode::ser_to_vec_ordered(key, ordcode::Order::Descending).unwrap()
    }
    fn ser_val(val: &Self::ValueType) -> Result<Vec<u8>>
    where
        <Self as Column>::ValueType: Serialize,
    {
        bincode::serialize(&val).map_err(|e| e.into())
    }
    fn deser_val<'a>(buf: &'a Vec<u8>) -> Result<Self::ValueType>
    where
        <Self as Column>::ValueType: Deserialize<'a>,
    {
        bincode::deserialize(&buf).map_err(|e| e.into())
    }
}

impl Column for BlockPart {
    const NAME: &'static str = BLOCK_PART_CF;
    type KeyType = (BlockHeight, u32);
    type ValueType = BlockPart;
}

impl Column for BlockMeta {
    const NAME: &'static str = BLOCK_META_CF;
    type KeyType = BlockHeight;
    type ValueType = BlockMeta;
}

impl Column for DoubleQC {
    const NAME: &'static str = QC_CF;
    type KeyType = BlockHeight;
    type ValueType = DoubleQC;
}

impl Column for TC {
    const NAME: &'static str = TC_CF;
    type KeyType = BlockHeight;
    type ValueType = TC;
}

impl Column for BlockHeight {
    const NAME: &'static str = BLOCK_HEIGHT_CF;
    type KeyType = H256;
    type ValueType = BlockHeight;
}

pub struct DBWriteBatch<'a> {
    pub batch: RocksWriteBatch,
    pub map: HashMap<&'static str, &'a ColumnFamily>,
}

impl<'a> DBWriteBatch<'a> {
    pub fn add<C>(&mut self, key: &C::KeyType, value: &C::ValueType) -> Result<()>
    where
        C: Column,
        <C as Column>::KeyType: Serialize,
        <C as Column>::ValueType: Serialize,
    {
        self.batch
            .put_cf(self.map[C::NAME], C::ser_key(key), C::ser_val(value)?);
        Ok(())
    }
}

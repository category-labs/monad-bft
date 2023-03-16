use crate::{
    store_db::{Column, DBWriteBatch, CFS},
    types::{Block, BlockHeight, BlockMeta, BlockPart, DoubleQC, Hashable, H256, TC},
};
use rocksdb::{Options, SingleThreaded, WriteBatch as RocksWriteBatch, DB};
use serde::Serialize;
use std::{error, fmt};

pub type Result<T> = std::result::Result<T, BlockstoreError>;

#[derive(Debug)]
pub enum BlockstoreError {
    RocksError(rocksdb::Error),
    BincodeError(bincode::Error),
    InvalidArgument(String),
    NotFound(String),
}

impl fmt::Display for BlockstoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RocksError(err) => write!(f, "RocksDB error: {}", err),
            Self::BincodeError(err) => write!(f, "Bincode error: {}", err),
            Self::InvalidArgument(s) => write!(f, "Invalid argument: {}", s),
            Self::NotFound(s) => write!(f, "Key not found: {}", s),
        }
    }
}

impl error::Error for BlockstoreError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            BlockstoreError::RocksError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl From<rocksdb::Error> for BlockstoreError {
    fn from(value: rocksdb::Error) -> Self {
        BlockstoreError::RocksError(value)
    }
}

impl From<bincode::Error> for BlockstoreError {
    fn from(value: bincode::Error) -> Self {
        BlockstoreError::BincodeError(value)
    }
}

pub struct BlockStore {
    db: DB,
}

impl BlockStore {
    pub fn new(db: DB) -> Self {
        BlockStore { db: db }
    }
    pub fn save_block(&self, block: &Block) -> Result<()> {
        if block.meta.total_parts as usize != block.parts.len() {
            return Err(BlockstoreError::InvalidArgument(
                "meta.total_parts!=block.parts.len()".to_owned(),
            ));
        }
        assert!(block.meta.total_parts as usize == block.parts.len());
        let mut batch = DBWriteBatch {
            batch: RocksWriteBatch::default(),
            map: CFS
                .iter()
                .map(|cf_name| (*cf_name, self.db.cf_handle(cf_name).unwrap()))
                .collect(),
        };
        let hash = block.hash();
        let height = block.meta.height;
        batch.add::<BlockMeta>(&height, &block.meta)?;
        for (i, bp) in block.parts.iter().enumerate() {
            batch.add::<BlockPart>(&(height, i as u32), bp)?;
        }
        batch.add::<DoubleQC>(&height, &block.qc)?;
        batch.add::<BlockHeight>(&hash, &height)?;
        self.db.write(batch.batch)?;
        Ok(())
    }

    pub fn save_tc(&self, tc: &TC, height: &BlockHeight) -> Result<()> {
        let mut batch = DBWriteBatch {
            batch: RocksWriteBatch::default(),
            map: CFS
                .iter()
                .map(|cf_name| (*cf_name, self.db.cf_handle(cf_name).unwrap()))
                .collect(),
        };
        batch.add::<TC>(&height, tc)?;
        self.db.write(batch.batch)?;
        Ok(())
    }

    fn load_bytes<C>(&self, key: &C::KeyType) -> Result<Option<Vec<u8>>>
    where
        C: Column,
        C::KeyType: Serialize,
    {
        self.db
            .get_cf(C::cf_handle(&self.db), C::ser_key(key))
            .map_err(|e| e.into())
    }

    pub fn load_block(&self, height: &BlockHeight) -> Result<Block> {
        let meta = self.load_blockmeta(height)?;
        let mut bps = Vec::new();
        bps.reserve(meta.total_parts as usize);
        for i in 0..meta.total_parts {
            bps.push(self.load_blockpart(height, i)?);
        }
        let qc = self.load_qc(height)?;
        Ok(Block {
            meta: meta,
            parts: bps,
            qc: qc,
        })
    }

    pub fn load_block_by_hash(&self, hash: &H256) -> Result<Block> {
        let height = self.load_blockheight(hash)?;
        self.load_block(&height)
    }

    pub fn load_blockmeta(&self, height: &BlockHeight) -> Result<BlockMeta> {
        self.load_bytes::<BlockMeta>(height)?
            .map(|buf| BlockMeta::deser_val(&buf))
            .ok_or(BlockstoreError::NotFound("blockmeta".to_owned()))?
    }

    pub fn load_blockmeta_by_hash(&self, hash: &H256) -> Result<BlockMeta> {
        let height = self.load_blockheight(hash)?;
        self.load_blockmeta(&height)
    }

    pub fn load_blockpart(&self, height: &BlockHeight, idx: u32) -> Result<BlockPart> {
        self.load_bytes::<BlockPart>(&(*height, idx))?
            .map(|buf| BlockPart::deser_val(&buf))
            .ok_or(BlockstoreError::NotFound("blockpart".to_owned()))?
    }

    pub fn load_qc(&self, height: &BlockHeight) -> Result<DoubleQC> {
        self.load_bytes::<DoubleQC>(height)?
            .map(|buf| DoubleQC::deser_val(&buf))
            .ok_or(BlockstoreError::NotFound("qc".to_owned()))?
    }

    pub fn load_tc(&self, height: &BlockHeight) -> Result<TC> {
        self.load_bytes::<TC>(height)?
            .map(|buf| TC::deser_val(&buf))
            .ok_or(BlockstoreError::NotFound("tc".to_owned()))?
    }

    fn load_blockheight(&self, hash: &H256) -> Result<BlockHeight> {
        self.load_bytes::<BlockHeight>(hash)?
            .map(|buf| BlockHeight::deser_val(&buf))
            .ok_or(BlockstoreError::NotFound("blockheight".to_owned()))?
    }
}

fn rocks_open(opts: &Options, path: &str) -> DB {
    rocksdb::DBWithThreadMode::<SingleThreaded>::open_cf(opts, path, CFS).unwrap()
}

fn rocks_destroy(opts: &Options, path: &str) {
    DB::destroy(opts, path).unwrap();
}

#[cfg(test)]
mod test {

    use crate::{
        store_db::CFS,
        types::{Block, BlockHeight, BlockMeta, BlockPart, DoubleQC, Hashable, QC, TC},
    };
    use tempfile::tempdir;

    use super::{rocks_destroy, rocks_open, BlockStore};
    use std::fs::create_dir_all;

    use rocksdb::{Options, SingleThreaded};
    // create a new rocksdb for testing
    fn rocks_new(path: &str) {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let mut db = rocksdb::DBWithThreadMode::<SingleThreaded>::open(&opts, path).unwrap();
        for cf in CFS {
            db.create_cf(cf, &Options::default()).unwrap();
        }
    }

    #[test]
    fn test_block_save_and_load() {
        let mut block = Block {
            meta: BlockMeta {
                height: BlockHeight { 0: 1 },
                data: vec![1, 2, 3],
                total_parts: 0,
            },
            parts: vec![BlockPart { data: vec![1] }, BlockPart { data: vec![2] }],
            qc: DoubleQC {
                qc1: QC { data: vec![] },
                qc2: QC { data: vec![] },
            },
        };
        block.meta.total_parts = block.parts.len() as u32;

        let path = tempdir().unwrap();
        create_dir_all(path.path()).unwrap();
        let path_str = path.path().to_str().unwrap();
        rocks_new(path_str);
        {
            let db = rocks_open(&Options::default(), path_str);
            let store = BlockStore::new(db);
            store.save_block(&block).unwrap();

            let block_load = store.load_block(&block.meta.height);

            assert_eq!(block_load.unwrap(), block);

            let meta_by_hash = store.load_blockmeta_by_hash(&block.hash());

            assert_eq!(meta_by_hash.unwrap(), block.meta);

            let block_by_hash = store.load_block_by_hash(&block.hash());

            assert_eq!(block_by_hash.unwrap(), block);
        }
        rocks_destroy(&Options::default(), path_str);
    }

    #[test]
    fn test_tc_save_and_load() {
        let tc = TC {
            data: vec![1, 2, 3, 4],
        };
        let height = BlockHeight(2);

        let path = tempdir().unwrap();
        create_dir_all(path.path()).unwrap();
        let path_str = path.path().to_str().unwrap();
        rocks_new(path_str);
        {
            let db = rocks_open(&Options::default(), path_str);
            let store = BlockStore::new(db);
            let save_result = store.save_tc(&tc, &height);
            assert!(save_result.is_ok());

            let tc_load = store.load_tc(&height);

            assert_eq!(tc_load.unwrap(), tc);
        }
        rocks_destroy(&Options::default(), path_str);
    }
}

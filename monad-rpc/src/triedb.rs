use std::{
    cell::RefCell,
    path::{Path, PathBuf},
};

use monad_blockdb::BlockTagKey;
use monad_blockdb_utils::BlockTags;
use monad_triedb_utils::TriedbReader;

type EthAddress = [u8; 20];
type EthStorageKey = [u8; 32];
type EthCodeHash = [u8; 32];

#[derive(Debug)]
pub enum TriedbResult {
    Null,
    // (nonce, balance, code_hash)
    Account(u64, u128, [u8; 32]),
    Storage([u8; 32]),
    Code(Vec<u8>),
    Receipt(Vec<u8>),
    BlockNum(u64),
}

#[derive(Clone)]
pub struct TriedbEnv {
    triedb_path: PathBuf,
}

pub trait Triedb {
    fn get_latest_block(&self) -> impl std::future::Future<Output = TriedbResult> + Send;
    fn get_receipt(
        &self,
        txn_index: u64,
        block_id: u64,
    ) -> impl std::future::Future<Output = TriedbResult> + Send;
    fn get_account(
        &self,
        addr: EthAddress,
        block_tag: BlockTags,
    ) -> impl std::future::Future<Output = TriedbResult> + Send;
    fn get_storage_at(
        &self,
        addr: EthAddress,
        at: EthStorageKey,
        block_tag: BlockTags,
    ) -> impl std::future::Future<Output = TriedbResult> + Send;
    fn get_code(
        &self,
        code_hash: EthCodeHash,
        block_tag: BlockTags,
    ) -> impl std::future::Future<Output = TriedbResult> + Send;
}

impl TriedbEnv {
    thread_local! {
        pub static DB: RefCell<Option<TriedbReader>> = RefCell::new(None);
    }

    pub fn new(triedb_path: &Path) -> Self {
        Self {
            triedb_path: triedb_path.to_path_buf(),
        }
    }

    pub fn path(&self) -> PathBuf {
        self.triedb_path.clone()
    }
}

impl Triedb for TriedbEnv {
    async fn get_latest_block(&self) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            TriedbEnv::DB.with_borrow_mut(|db_reader| {
                let db = db_reader.get_or_insert_with(|| {
                    TriedbReader::try_new(&triedb_path).expect("triedb should exist in path")
                });

                let result = db.get_latest_block();
                let _ = send.send(TriedbResult::BlockNum(result));
            });
        });
        recv.await.expect("rayon panic get_latest_block")
    }

    async fn get_account(&self, addr: EthAddress, block_tag: BlockTags) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            TriedbEnv::DB.with_borrow_mut(|db_handle| {
                let db = db_handle.get_or_insert_with(|| {
                    TriedbReader::try_new(&triedb_path).expect("triedb should exist in path")
                });

                // parse block tag
                let block_num = match block_tag {
                    BlockTags::Number(q) => q,
                    BlockTags::Default(t) => match t {
                        BlockTagKey::Latest => db.get_latest_block(),
                        BlockTagKey::Finalized => db.get_latest_block(),
                    },
                };

                let Some(account) = db.get_account(&addr, block_num) else {
                    let _ = send.send(TriedbResult::Null);
                    return;
                };

                let _ = send.send(TriedbResult::Account(
                    account.nonce,
                    account.balance,
                    account.code_hash.map_or([0u8; 32], |bytes| bytes.0),
                ));
            });
        });
        recv.await.expect("rayon panic get_account")
    }

    async fn get_storage_at(
        &self,
        addr: EthAddress,
        at: EthStorageKey,
        block_tag: BlockTags,
    ) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            TriedbEnv::DB.with_borrow_mut(|db_handle| {
                let db = db_handle.get_or_insert_with(|| {
                    TriedbReader::try_new(&triedb_path).expect("triedb should exist in path")
                });

                // parse block tag
                let block_num = match block_tag {
                    BlockTags::Number(q) => q,
                    BlockTags::Default(t) => match t {
                        BlockTagKey::Latest => db.get_latest_block(),
                        BlockTagKey::Finalized => db.get_latest_block(),
                    },
                };

                let Some(storage_value) = db.get_storage_at(&addr, &at, block_num) else {
                    let _ = send.send(TriedbResult::Null);
                    return;
                };

                let _ = send.send(TriedbResult::Storage(storage_value));
            });
        });
        recv.await.expect("rayon panic get_storage_at")
    }

    async fn get_code(&self, code_hash: EthCodeHash, block_tag: BlockTags) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            TriedbEnv::DB.with_borrow_mut(|db_handle| {
                let db = db_handle.get_or_insert_with(|| {
                    TriedbReader::try_new(&triedb_path).expect("triedb should exist in path")
                });

                // parse block tag
                let block_num = match block_tag {
                    BlockTags::Number(q) => q,
                    BlockTags::Default(t) => match t {
                        BlockTagKey::Latest => db.get_latest_block(),
                        BlockTagKey::Finalized => db.get_latest_block(),
                    },
                };

                let Some(code) = db.get_code(&code_hash, block_num) else {
                    let _ = send.send(TriedbResult::Null);
                    return;
                };

                let _ = send.send(TriedbResult::Code(code));
            });
        });
        recv.await.expect("rayon panic get_code")
    }

    async fn get_receipt(&self, txn_index: u64, block_num: u64) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            TriedbEnv::DB.with_borrow_mut(|db_handle| {
                let db = db_handle.get_or_insert_with(|| {
                    TriedbReader::try_new(&triedb_path).expect("triedb should exist in path")
                });

                let Some(receipt) = db.get_receipt(txn_index, block_num) else {
                    let _ = send.send(TriedbResult::Null);
                    return;
                };

                let _ = send.send(TriedbResult::Receipt(receipt));
            });
        });
        recv.await.expect("rayon panic get_receipt")
    }
}

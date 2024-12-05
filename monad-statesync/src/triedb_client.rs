use std::ffi::CString;

use monad_executor_glue::StateSyncUpsertType;
use monad_types::SeqNum;

use crate::{bindings, ffi::SyncClientBackend, Target};

fn num_prefixes() -> u64 {
    unsafe { bindings::monad_statesync_client_prefixes() as u64 }
}

pub struct TriedbSyncClient {
    ctx: *mut bindings::monad_statesync_client_context,
}

impl From<Target> for bindings::monad_sync_target {
    fn from(target: Target) -> Self {
        Self {
            n: target.n.0,
            state_root: target.state_root,
        }
    }
}

fn to_monad_sync_type(upsert_type: StateSyncUpsertType) -> bindings::monad_sync_type {
    match upsert_type {
        StateSyncUpsertType::Code => bindings::monad_sync_type_SYNC_TYPE_UPSERT_CODE,
        StateSyncUpsertType::Account => bindings::monad_sync_type_SYNC_TYPE_UPSERT_ACCOUNT,
        StateSyncUpsertType::Storage => bindings::monad_sync_type_SYNC_TYPE_UPSERT_STORAGE,
        StateSyncUpsertType::AccountDelete => {
            bindings::monad_sync_type_SYNC_TYPE_UPSERT_ACCOUNT_DELETE
        }
        StateSyncUpsertType::StorageDelete => {
            bindings::monad_sync_type_SYNC_TYPE_UPSERT_STORAGE_DELETE
        }
    }
}

impl TriedbSyncClient {
    pub fn new(dbname_paths: &[String], genesis_path: &str) -> Self {
        let dbname_paths: Vec<CString> = dbname_paths
            .iter()
            .map(|path| {
                CString::new(path.to_owned()).expect("invalid db_path - does it contain null byte?")
            })
            .collect();
        let genesis_path =
            CString::new(genesis_path).expect("invalid genesis_path - does it contain null byte?");
        let dbname_path_ptrs = dbname_paths
            .iter()
            .map(|s| s.as_ptr() as *const ::std::os::raw::c_char)
            .collect::<Vec<_>>();

        let ctx = unsafe {
            bindings::monad_statesync_client_context_create(
                dbname_path_ptrs.as_ptr(),
                dbname_path_ptrs.len(),
                genesis_path.as_ptr(),
            )
        };

        for i in 0..num_prefixes() {
            unsafe {
                bindings::monad_statesync_client_handle_new_peer(
                    ctx,
                    i,
                    bindings::monad_statesync_version(),
                );
            }
        }

        Self { ctx }
    }
}

impl Drop for TriedbSyncClient {
    fn drop(&mut self) {
        unsafe {
            bindings::monad_statesync_client_context_destroy(self.ctx);
        }
    }
}

impl SyncClientBackend for TriedbSyncClient {
    fn create(dbname_paths: &[String], genesis_file: &str) -> Self {
        Self::new(dbname_paths, genesis_file)
    }

    fn handle_upsert(
        &mut self,
        prefix: u64,
        upsert_type: StateSyncUpsertType,
        value: &[u8],
    ) -> bool {
        unsafe {
            bindings::monad_statesync_client_handle_upsert(
                self.ctx,
                prefix,
                to_monad_sync_type(upsert_type),
                value.as_ptr(),
                value.len() as u64,
            )
        }
    }

    fn latest_block(&self) -> SeqNum {
        unsafe {
            SeqNum(bindings::monad_statesync_client_get_latest_block_id(
                self.ctx,
            ))
        }
    }

    fn commit(&mut self) {
        unsafe {
            bindings::monad_statesync_client_commit(self.ctx);
        }
    }

    fn finalize(&mut self, target: Target) -> bool {
        unsafe { bindings::monad_statesync_client_finalize(self.ctx, target.into()) }
    }
}

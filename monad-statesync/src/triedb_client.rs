use std::ffi::CString;

use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::{StateSyncRequest, StateSyncUpsertType, SELF_STATESYNC_VERSION};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    bindings,
    ffi::{statesync_send_request, StateSyncContext, SyncClientBackend},
    SyncRequest, Target,
};

fn num_prefixes() -> u64 {
    unsafe { bindings::monad_statesync_client_prefixes() as u64 }
}

pub struct TriedbSyncClient {
    ctx: *mut bindings::monad_statesync_client_context,
    _request_ctx: Box<StateSyncContext>,
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
    pub fn new<PT: PubKey>(
        dbname_paths: &[String],
        genesis_path: &str,
        request_tx: UnboundedSender<SyncRequest<StateSyncRequest, PT>>,
        target: Target,
    ) -> Self {
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

        let mut request_ctx: Box<StateSyncContext> = Box::new(Box::new({
            let request_tx = request_tx.clone();
            move |request| {
                let result = request_tx.send(SyncRequest::Request(StateSyncRequest {
                    version: SELF_STATESYNC_VERSION,
                    prefix: request.prefix,
                    prefix_bytes: request.prefix_bytes,
                    target: request.target,
                    from: request.from,
                    until: request.until,
                    old_target: request.old_target,
                }));
                if result.is_err() {
                    eprintln!("invariant broken: send_request called after destroy");
                    // we can't panic because that's not safe in a C callback
                    std::process::exit(1)
                }
            }
        }));

        let ctx = unsafe {
            bindings::monad_statesync_client_context_create(
                dbname_path_ptrs.as_ptr(),
                dbname_path_ptrs.len(),
                genesis_path.as_ptr(),
                &mut *request_ctx as *mut _ as *mut bindings::monad_statesync_client,
                Some(statesync_send_request),
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

        unsafe {
            bindings::monad_statesync_client_handle_target(ctx, target.into());
        }

        Self {
            ctx,
            _request_ctx: request_ctx,
        }
    }
}

impl Drop for TriedbSyncClient {
    fn drop(&mut self) {
        unsafe {
            bindings::monad_statesync_client_context_destroy(self.ctx);
        }
    }
}

impl<PT: PubKey> SyncClientBackend<PT> for TriedbSyncClient {
    fn create(
        dbname_paths: &[String],
        genesis_file: &str,
        request_tx: tokio::sync::mpsc::UnboundedSender<SyncRequest<StateSyncRequest, PT>>,
        target: Target,
    ) -> Self {
        Self::new(dbname_paths, genesis_file, request_tx, target)
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

    fn handle_done(&mut self, prefix: u64, n: monad_types::SeqNum) {
        unsafe {
            bindings::monad_statesync_client_handle_done(
                self.ctx,
                bindings::monad_sync_done {
                    success: true,
                    prefix: prefix,
                    n: n.0,
                },
            )
        }
    }

    fn try_finalize(&mut self) -> bool {
        if unsafe { bindings::monad_statesync_client_has_reached_target(self.ctx) } {
            unsafe { bindings::monad_statesync_client_finalize(self.ctx) }
        } else {
            false
        }
    }
}

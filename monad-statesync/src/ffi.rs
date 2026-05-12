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

pub(crate) use self::bindings::{
    monad_statesync_client, monad_statesync_client_context, monad_statesync_client_handle_done,
    monad_statesync_client_handle_target, monad_statesync_client_handle_upsert, monad_sync_done,
    monad_sync_request, monad_sync_type_SYNC_TYPE_DONE, monad_sync_type_SYNC_TYPE_REQUEST,
    monad_sync_type_SYNC_TYPE_TARGET, monad_sync_type_SYNC_TYPE_UPSERT_ACCOUNT,
    monad_sync_type_SYNC_TYPE_UPSERT_ACCOUNT_DELETE, monad_sync_type_SYNC_TYPE_UPSERT_CODE,
    monad_sync_type_SYNC_TYPE_UPSERT_HEADER, monad_sync_type_SYNC_TYPE_UPSERT_STORAGE,
    monad_sync_type_SYNC_TYPE_UPSERT_STORAGE_DELETE,
};

#[allow(dead_code, non_camel_case_types, non_upper_case_globals)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub(crate) type StateSyncContext = Box<dyn FnMut(monad_sync_request)>;

// void (*statesync_send_request)(struct StateSync *, struct SyncRequest)
#[no_mangle]
pub extern "C" fn statesync_send_request(
    statesync: *mut monad_statesync_client,
    request: monad_sync_request,
) {
    let statesync = statesync as *mut StateSyncContext;
    unsafe { (*statesync)(request) }
}

/// Thin unsafe wrapper around statesync_client_context that handles destruction and finalization
/// checking
pub struct SyncCtx {
    dbname_paths: *const *const ::std::os::raw::c_char,
    len: usize,
    sq_thread_cpu: Option<::std::os::raw::c_uint>,
    request_ctx: *mut monad_statesync_client,
    statesync_send_request: ::std::option::Option<
        unsafe extern "C" fn(arg1: *mut monad_statesync_client, arg2: monad_sync_request),
    >,

    // TODO(andr-dev): Make field non-pub
    pub ctx: Option<*mut monad_statesync_client_context>,
}

impl SyncCtx {
    /// Initialize SyncCtx. There should only ever be *one* SyncCtx at any given time.
    pub fn new(
        dbname_paths: *const *const ::std::os::raw::c_char,
        len: usize,
        sq_thread_cpu: Option<::std::os::raw::c_uint>,
        request_ctx: *mut monad_statesync_client,
        statesync_send_request: ::std::option::Option<
            unsafe extern "C" fn(arg1: *mut monad_statesync_client, arg2: monad_sync_request),
        >,
    ) -> Self {
        Self {
            dbname_paths,
            len,
            sq_thread_cpu,
            request_ctx,
            statesync_send_request,

            ctx: None,
        }
    }

    pub fn get_or_create_ctx(&mut self) -> *mut monad_statesync_client_context {
        *self.ctx.get_or_insert_with(|| unsafe {
            let ctx = self::bindings::monad_statesync_client_context_create(
                self.dbname_paths,
                self.len,
                self.sq_thread_cpu
                    .unwrap_or(self::bindings::MONAD_SQPOLL_DISABLED),
                self.request_ctx,
                self.statesync_send_request,
            );
            let client_version = self::bindings::monad_statesync_version();
            assert!(self::bindings::monad_statesync_client_compatible(
                client_version
            ));
            let num_prefixes = self::bindings::monad_statesync_client_prefixes();
            for prefix in 0..num_prefixes {
                self::bindings::monad_statesync_client_handle_new_peer(
                    ctx,
                    prefix as u64,
                    client_version,
                );
            }
            ctx
        })
    }

    /// Returns true if reached target and successfully finalized
    pub fn try_finalize(&mut self) -> bool {
        let ctx = self
            .ctx
            .expect("try_finalize should only be called on active SyncCtx");

        if unsafe { self::bindings::monad_statesync_client_has_reached_target(ctx) } {
            let root_matches = unsafe { self::bindings::monad_statesync_client_finalize(ctx) };
            assert!(root_matches, "state root doesn't match, are peers trusted?");

            unsafe { self::bindings::monad_statesync_client_context_destroy(ctx) }
            self.ctx = None;
            return true;
        }
        false
    }
}

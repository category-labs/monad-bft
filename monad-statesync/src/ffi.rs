use std::{
    ffi::CString,
    ops::DerefMut,
    pin::{pin, Pin},
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use alloy_consensus::Header;
use alloy_rlp::Encodable;
use futures::{FutureExt, Stream};
use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::{
    StateSyncRequest, StateSyncResponse, StateSyncUpsertType, SELF_STATESYNC_VERSION,
};
use monad_types::{NodeId, SeqNum};
use rand::seq::SliceRandom;

use crate::{bindings, outbound_requests::OutboundRequests};

type StateSyncContext = Box<dyn FnMut(bindings::monad_sync_request)>;

// void (*statesync_send_request)(struct StateSync *, struct SyncRequest)
#[no_mangle]
pub extern "C" fn statesync_send_request(
    statesync: *mut bindings::monad_statesync_client,
    request: bindings::monad_sync_request,
) {
    let statesync = statesync as *mut StateSyncContext;
    unsafe { (*statesync)(request) }
}

pub(crate) struct StateSync<PT: PubKey> {
    state_sync_peers: Vec<NodeId<PT>>,
    outbound_requests: OutboundRequests<PT>,
    current_target: Option<Header>,

    request_rx: tokio::sync::mpsc::UnboundedReceiver<SyncRequest<StateSyncRequest>>,
    response_tx: std::sync::mpsc::Sender<SyncResponse>,

    progress: Arc<Mutex<Progress>>,
}

/// This name is confusing, but I can't think of a better name. This is basically an output event
/// from the perspective of this module. OutputEvent would also be a confusing name, because from
/// the perspetive of the caller, this is an input event.
pub(crate) enum SyncRequest<R> {
    Request(R),
    DoneSync(Header),
}

/// This name is confusing, but I can't think of a better name. This is basically an input event
/// from the perspective of this module. InputEvent would also be a confusing name, because from
/// the perspetive of the caller, this is an output event.
pub(crate) enum SyncResponse {
    Response(StateSyncResponse),
    UpdateTarget(Header),
}

const NUM_PREFIXES: u64 = 256;

#[derive(Clone, Copy, Default)]
struct Progress {
    start_target: Option<SeqNum>,
    end_target: Option<SeqNum>,

    // our guess for the minimum block at which servicers statesync'd before
    min_until_guess: Option<SeqNum>,
    current_progress: Option<u64>,
}

impl Progress {
    fn update_target(&mut self, target: &Header) {
        self.end_target = Some(SeqNum(target.number));
        if self.min_until_guess.is_none() {
            // guess that the statesync servicers have been up for at least 10_000 blocks
            self.min_until_guess = Some(SeqNum(target.number.max(10_000) - 10_000));
        }
    }

    fn update_handled_request(&mut self, request: &StateSyncRequest) {
        assert_eq!(self.end_target, Some(SeqNum(request.target)));
        let min_until_guess = self.min_until_guess.expect("self.end_target exists").0;

        if self.start_target.is_none() {
            self.start_target = Some(SeqNum(request.from));
        }
        let start_target = self.start_target.expect("start_target set").0;

        if self.current_progress.is_none() {
            self.current_progress = Some(request.from * NUM_PREFIXES);
        }

        if request.until >= min_until_guess {
            let adjusted_from = if request.from <= min_until_guess {
                start_target
            } else {
                request.from
            };
            *self
                .current_progress
                .as_mut()
                .expect("current_progress was set") += request.until - adjusted_from;
        }
    }

    fn update_reached_target(&mut self, target: &Header) {
        assert_eq!(self.end_target, Some(SeqNum(target.number)));
        self.start_target = Some(SeqNum(target.number));
        self.current_progress = None;
    }

    fn estimate(&self) -> Option<SeqNum> {
        let start_target = self.start_target?;
        let end_target = self.end_target?;

        if start_target == end_target {
            return Some(end_target);
        }

        assert!(end_target > start_target);

        let _total_progress = (end_target - start_target).0 * NUM_PREFIXES;
        // current_progress / _total_progress would estimate progress in percentage terms

        // current_progress / num_prefixes can be used as a target estimate
        Some(SeqNum(self.current_progress? / NUM_PREFIXES))
    }
}

impl<PT: PubKey> StateSync<PT> {
    pub fn start(
        db_paths: &[String],
        genesis_path: &str,
        state_sync_peers: &[NodeId<PT>],
        max_parallel_requests: usize,
        request_timeout: Duration,
    ) -> Self {
        let db_paths: Vec<CString> = db_paths
            .iter()
            .map(|path| {
                CString::new(path.to_owned()).expect("invalid db_path - does it contain null byte?")
            })
            .collect();
        let genesis_path =
            CString::new(genesis_path).expect("invalid genesis_path - does it contain null byte?");

        let (request_tx, request_rx) =
            tokio::sync::mpsc::unbounded_channel::<SyncRequest<StateSyncRequest>>();
        let (response_tx, response_rx) = std::sync::mpsc::channel::<SyncResponse>();

        let progress = Arc::new(Mutex::new(Progress::default()));
        let progress_clone = Arc::clone(&progress);

        std::thread::spawn(move || {
            let db_paths_ptrs: Vec<*const i8> = db_paths.iter().map(|s| s.as_ptr()).collect();
            let db_paths_ptr = db_paths_ptrs.as_ptr();
            let num_db_paths = db_paths_ptrs.len();

            // callback function must be kept alive until statesync_client_context_destroy is
            // called
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

            let mut sync_ctx = SyncCtx::create(
                db_paths_ptr,
                num_db_paths,
                genesis_path.as_ptr(),
                &mut *request_ctx as *mut _ as *mut bindings::monad_statesync_client,
                Some(statesync_send_request),
            );
            let mut current_target = None;

            while let Ok(response) = response_rx.recv() {
                match response {
                    SyncResponse::UpdateTarget(target) => {
                        tracing::debug!(
                            ?current_target,
                            new_target =? target,
                            "updating statesync target"
                        );
                        let mut buf = Vec::new();
                        target.encode(&mut buf);
                        unsafe {
                            bindings::monad_statesync_client_handle_target(
                                sync_ctx.ctx,
                                buf.as_ptr(),
                                buf.len() as u64,
                            )
                        };
                        progress.lock().unwrap().update_target(&target);
                        current_target = Some(target);
                    }
                    SyncResponse::Response(response) => {
                        assert!(current_target.is_some());
                        unsafe {
                            for upsert in &response.response {
                                let upsert_result = bindings::monad_statesync_client_handle_upsert(
                                    sync_ctx.ctx,
                                    response.request.prefix,
                                    match upsert.upsert_type {
                                        StateSyncUpsertType::Code => {
                                            bindings::monad_sync_type_SYNC_TYPE_UPSERT_CODE
                                        }
                                        StateSyncUpsertType::Account => {
                                            bindings::monad_sync_type_SYNC_TYPE_UPSERT_ACCOUNT
                                        }
                                        StateSyncUpsertType::Storage => {
                                            bindings::monad_sync_type_SYNC_TYPE_UPSERT_STORAGE
                                        }
                                        StateSyncUpsertType::AccountDelete => {
                                            bindings::monad_sync_type_SYNC_TYPE_UPSERT_ACCOUNT_DELETE
                                        }
                                        StateSyncUpsertType::StorageDelete => {
                                            bindings::monad_sync_type_SYNC_TYPE_UPSERT_STORAGE_DELETE
                                        }
                                        StateSyncUpsertType::Header => {
                                            bindings::monad_sync_type_SYNC_TYPE_UPSERT_HEADER
                                        }
                                    },
                                    upsert.data.as_ptr(),
                                    upsert.data.len() as u64,
                                );
                                assert!(
                                    upsert_result,
                                    "failed upsert for response: {:?}",
                                    &response
                                );
                            }
                            bindings::monad_statesync_client_handle_done(
                                sync_ctx.ctx,
                                bindings::monad_sync_done {
                                    success: true,
                                    prefix: response.request.prefix,
                                    n: response.response_n,
                                },
                            )
                        }
                        progress
                            .lock()
                            .unwrap()
                            .update_handled_request(&response.request)
                    }
                }
                if sync_ctx.try_finalize() {
                    let target = current_target
                        .take()
                        .expect("current_target must have been set");
                    tracing::debug!(?target, "done statesync");
                    progress.lock().unwrap().update_reached_target(&target);
                    request_tx
                        .send(SyncRequest::DoneSync(target))
                        .expect("request_rx dropped mid DoneSync");
                }
            }
            // this loop exits when execution is about to start
        });

        Self {
            state_sync_peers: state_sync_peers.to_vec(),
            outbound_requests: OutboundRequests::new(max_parallel_requests, request_timeout),
            current_target: None,

            request_rx,
            response_tx,

            progress: progress_clone,
        }
    }

    pub fn update_target(&mut self, target: Header) {
        if let Some(old_target) = &self.current_target {
            assert!(old_target.number < target.number);
        }
        self.current_target = Some(target.clone());
        self.outbound_requests.clear();
        self.response_tx
            .send(SyncResponse::UpdateTarget(target))
            .expect("response_rx dropped");
    }

    pub fn handle_response(&mut self, from: NodeId<PT>, response: StateSyncResponse) {
        if !self.state_sync_peers.iter().any(|trusted| trusted == &from) {
            tracing::warn!(
                ?from,
                ?response,
                "dropping statesync response from untrusted peer",
            );
            return;
        }

        if !response.version.is_compatible() {
            tracing::debug!(
                ?from,
                ?response,
                ?SELF_STATESYNC_VERSION,
                "dropping statesync response, version incompatible"
            );
            return;
        }

        if let Some(full_response) = self.outbound_requests.handle_response(from, response) {
            self.response_tx
                .send(SyncResponse::Response(full_response))
                .expect("response_rx dropped");
        }
    }

    /// An estimate of current sync progress in `Target` units
    pub fn progress_estimate(&self) -> Option<SeqNum> {
        self.progress.lock().unwrap().estimate()
    }
}

impl<PT: PubKey> Stream for StateSync<PT> {
    type Item = SyncRequest<(NodeId<PT>, StateSyncRequest)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        while let Poll::Ready(request) = this.request_rx.poll_recv(cx) {
            match request.expect("request_tx dropped") {
                SyncRequest::Request(request) => {
                    if this
                        .current_target
                        .as_ref()
                        .is_some_and(|current_target| current_target.number != request.target)
                    {
                        tracing::debug!(
                            ?request,
                            current_target =? this.current_target,
                            "dropping request for stale target"
                        );
                        continue;
                    }
                    this.outbound_requests.queue_request(request);
                }
                SyncRequest::DoneSync(target) => {
                    // Justification for assertion:
                    //
                    // DoneSync being emitted implies that all outstanding requests were handled
                    // from the perspective of the statesync thread. Any subsequent queued requests
                    // therefore must have been sequenced after this DoneSync is handled.
                    assert!(this.outbound_requests.is_empty());
                    this.outbound_requests.clear_prefix_peers();

                    return Poll::Ready(Some(SyncRequest::DoneSync(target)));
                }
            }
        }

        let fut = this.outbound_requests.poll();
        if let Poll::Ready((maybe_locked_peer, request)) = pin!(fut).poll_unpin(cx) {
            let servicer = maybe_locked_peer.unwrap_or_else(|| {
                this.state_sync_peers
                    .choose(&mut rand::thread_rng())
                    .expect("unable to send statesync request, no peers")
            });
            return Poll::Ready(Some(SyncRequest::Request((*servicer, request))));
        }

        Poll::Pending
    }
}

/// Thin unsafe wrapper around statesync_client_context that handles destruction and finalization
/// checking
struct SyncCtx {
    dbname_paths: *const *const ::std::os::raw::c_char,
    len: usize,
    genesis_file: *const ::std::os::raw::c_char,
    request_ctx: *mut bindings::monad_statesync_client,
    statesync_send_request: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut bindings::monad_statesync_client,
            arg2: bindings::monad_sync_request,
        ),
    >,

    ctx: *mut bindings::monad_statesync_client_context,
}
impl SyncCtx {
    /// Initialize SyncCtx. There should only ever be *one* SyncCtx at any given time.
    fn create(
        dbname_paths: *const *const ::std::os::raw::c_char,
        len: usize,
        genesis_file: *const ::std::os::raw::c_char,
        request_ctx: *mut bindings::monad_statesync_client,
        statesync_send_request: ::std::option::Option<
            unsafe extern "C" fn(
                arg1: *mut bindings::monad_statesync_client,
                arg2: bindings::monad_sync_request,
            ),
        >,
    ) -> Self {
        Self {
            dbname_paths,
            len,
            genesis_file,
            request_ctx,
            statesync_send_request,

            ctx: unsafe {
                let ctx = bindings::monad_statesync_client_context_create(
                    dbname_paths,
                    len,
                    genesis_file,
                    request_ctx,
                    statesync_send_request,
                );
                let client_version = bindings::monad_statesync_version();
                assert!(bindings::monad_statesync_client_compatible(client_version));
                let num_prefixes = bindings::monad_statesync_client_prefixes();
                for prefix in 0..num_prefixes {
                    bindings::monad_statesync_client_handle_new_peer(
                        ctx,
                        prefix as u64,
                        client_version,
                    );
                }
                ctx
            },
        }
    }

    /// Returns true if reached target and successfully finalized
    fn try_finalize(&mut self) -> bool {
        if unsafe { bindings::monad_statesync_client_has_reached_target(self.ctx) } {
            let root_matches = unsafe { bindings::monad_statesync_client_finalize(self.ctx) };
            assert!(root_matches, "state root doesn't match, are peers trusted?");

            unsafe { bindings::monad_statesync_client_context_destroy(self.ctx) }
            self.ctx = unsafe {
                let ctx = bindings::monad_statesync_client_context_create(
                    self.dbname_paths,
                    self.len,
                    self.genesis_file,
                    self.request_ctx,
                    self.statesync_send_request,
                );
                let client_version = bindings::monad_statesync_version();
                assert!(bindings::monad_statesync_client_compatible(client_version));
                let num_prefixes = bindings::monad_statesync_client_prefixes();
                for prefix in 0..num_prefixes {
                    bindings::monad_statesync_client_handle_new_peer(
                        ctx,
                        prefix as u64,
                        client_version,
                    );
                }
                ctx
            };
            return true;
        }
        false
    }
}
impl Drop for SyncCtx {
    fn drop(&mut self) {
        unsafe { bindings::monad_statesync_client_context_destroy(self.ctx) }
    }
}

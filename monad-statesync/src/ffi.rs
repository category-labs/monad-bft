use std::{
    ffi::CString,
    ops::DerefMut,
    pin::{pin, Pin},
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{FutureExt, Stream};
use monad_executor_glue::{StateSyncRequest, StateSyncResponse};
use monad_types::SeqNum;

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

pub(crate) struct StateSync {
    target: SeqNum,
    outbound_requests: OutboundRequests,

    execution_ctx: *mut bindings::monad_statesync_client_context,
    request_rx: tokio::sync::mpsc::UnboundedReceiver<bindings::monad_sync_request>,

    /// callback function that must be kept alive until statesync_execution_context_destroy is
    /// called
    _request_ctx: Box<Box<dyn FnMut(bindings::monad_sync_request)>>,

    waker: Option<Waker>,
}

pub(crate) struct Target {
    pub n: SeqNum,
    pub state_root: [u8; 32],
}

impl StateSync {
    pub fn start(
        db_paths: &[String],
        genesis_path: &str,
        max_parallel_requests: usize,
        request_timeout: Duration,
        target: Target,
    ) -> Self {
        let db_paths: Vec<CString> = db_paths
            .iter()
            .map(|path| {
                CString::new(path.to_owned()).expect("invalid db_path - does it contain null byte?")
            })
            .collect();
        let db_paths_ptrs: Vec<*const i8> = db_paths.iter().map(|s| s.as_ptr()).collect();
        let db_paths_ptr = db_paths_ptrs.as_ptr();
        let num_db_paths = db_paths_ptrs.len();
        let genesis_path =
            CString::new(genesis_path).expect("invalid genesis_path - does it contain null byte?");

        let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel();

        let mut request_ctx: Box<StateSyncContext> = Box::new(Box::new(move |request| {
            let result = request_tx.send(request);
            if result.is_err() {
                eprintln!("invariant broken: send_request called after destroy");
                // we can't panic because that's not safe in a C callback
                std::process::exit(1)
            }
        }));
        let execution_ctx = unsafe {
            let ctx = bindings::monad_statesync_client_context_create(
                db_paths_ptr,
                num_db_paths,
                genesis_path.as_ptr(),
                &mut *request_ctx as *mut _ as *mut bindings::monad_statesync_client,
                Some(statesync_send_request),
            );

            bindings::monad_statesync_client_handle_target(
                ctx,
                bindings::monad_sync_target {
                    n: target.n.0,
                    state_root: target.state_root,
                },
            );

            ctx
        };

        Self {
            target: target.n,
            outbound_requests: OutboundRequests::new(max_parallel_requests, request_timeout),

            execution_ctx,
            request_rx,

            _request_ctx: request_ctx,

            waker: None,
        }
    }

    pub fn target(&self) -> SeqNum {
        self.target
    }

    pub fn handle_response(&mut self, response: StateSyncResponse) {
        if self.outbound_requests.handle_response(&response) {
            // valid request
            unsafe {
                for (code, key, value) in response.response {
                    bindings::monad_statesync_client_handle_upsert(
                        self.execution_ctx,
                        key.as_ptr(),
                        key.len() as u64,
                        value.as_ptr(),
                        value.len() as u64,
                        code,
                    )
                }
                bindings::monad_statesync_client_handle_done(
                    self.execution_ctx,
                    bindings::monad_sync_done {
                        success: true,
                        prefix: response.request.prefix,
                        n: response.response_n,
                    },
                )
            }
            if let Some(waker) = self.waker.take() {
                waker.wake()
            }
        }
    }

    fn has_reached_target(&self) -> bool {
        unsafe { bindings::monad_statesync_client_has_reached_target(self.execution_ctx) }
    }
}

impl Drop for StateSync {
    fn drop(&mut self) {
        unsafe { bindings::monad_statesync_client_context_destroy(self.execution_ctx) }
    }
}

impl Stream for StateSync {
    type Item = StateSyncRequest;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if this.has_reached_target() {
            let root_matches =
                unsafe { bindings::monad_statesync_client_finalize(this.execution_ctx) };
            assert!(root_matches, "state root doesn't match, are peers trusted?");
            assert!(this.outbound_requests.is_empty());

            // done statesyncing
            return Poll::Ready(None);
        }

        while let Poll::Ready(request) = this.request_rx.poll_recv(cx) {
            let request = request.expect("request_tx is never dropped");
            this.outbound_requests.queue_request(StateSyncRequest {
                prefix: request.prefix,
                prefix_bytes: request.prefix_bytes,
                target: request.target,
                from: request.from,
                until: request.until,
                old_target: request.old_target,
            });
        }

        let fut = this.outbound_requests.poll();
        if let Poll::Ready(request) = pin!(fut).poll_unpin(cx) {
            return Poll::Ready(Some(request));
        }

        Poll::Pending
    }
}

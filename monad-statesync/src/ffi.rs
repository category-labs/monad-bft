use std::{
    ops::DerefMut,
    pin::{pin, Pin},
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use futures::{FutureExt, Stream};
use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::{
    StateSyncRequest, StateSyncResponse, StateSyncResponseOk, StateSyncUpsertType,
    SELF_STATESYNC_VERSION,
};
use monad_types::{NodeId, SeqNum};

use crate::{bindings, outbound_requests::OutboundRequests};

pub(crate) type StateSyncContext = Box<dyn FnMut(bindings::monad_sync_request)>;

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
    current_target: Option<Target>,

    request_rx: tokio::sync::mpsc::UnboundedReceiver<SyncRequest<StateSyncRequest, PT>>,
    response_tx: std::sync::mpsc::Sender<SyncResponse<PT>>,

    progress: Arc<Mutex<Progress>>,
}

#[derive(Debug, Clone, Copy)]
pub struct Target {
    pub n: SeqNum,
    pub state_root: [u8; 32],
}

/// This name is confusing, but I can't think of a better name. This is basically an output event
/// from the perspective of this module. OutputEvent would also be a confusing name, because from
/// the perspetive of the caller, this is an input event.
pub enum SyncRequest<R, PT: PubKey> {
    Request(R),
    DoneSync(Target),
    Completion(NodeId<PT>),
}

/// This name is confusing, but I can't think of a better name. This is basically an input event
/// from the perspective of this module. InputEvent would also be a confusing name, because from
/// the perspetive of the caller, this is an output event.
pub(crate) enum SyncResponse<P: PubKey> {
    Response((NodeId<P>, StateSyncRequest, StateSyncResponseOk)),
    UpdateTarget(Target),
}

pub(crate) const NUM_PREFIXES: u64 = 256;

const INVALID_SEQ_NUM: SeqNum = SeqNum(u64::MAX);

#[derive(Clone, Copy, Default)]
struct Progress {
    start_target: Option<SeqNum>,
    end_target: Option<SeqNum>,

    // our guess for the minimum block at which servicers statesync'd before
    min_until_guess: Option<SeqNum>,
    current_progress: Option<u64>,
}

impl Progress {
    fn update_target(&mut self, target: Target) {
        self.end_target = Some(target.n);
        if self.min_until_guess.is_none() {
            // guess that the statesync servicers have been up for at least 10_000 blocks
            self.min_until_guess = Some(SeqNum(target.n.0.max(10_000) - 10_000));
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

    fn update_reached_target(&mut self, target: Target) {
        assert_eq!(self.end_target, Some(target.n));
        self.start_target = Some(target.n);
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
    pub fn start<B: SyncClientBackend<PT>>(
        db_paths: Vec<String>,
        genesis_path: String,
        state_sync_peers: Vec<NodeId<PT>>,
        max_parallel_requests: usize,
        request_timeout: Duration,
    ) -> Self {
        let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel();
        let (response_tx, response_rx) = std::sync::mpsc::channel::<SyncResponse<PT>>();

        let progress = Arc::new(Mutex::new(Progress::default()));
        let progress_clone = Arc::clone(&progress);

        std::thread::spawn(move || {
            let mut sync_ctx = None;
            let mut current_target = Target {
                n: SeqNum(0),
                state_root: [0; 32],
            };

            while let Ok(response) = response_rx.recv() {
                match response {
                    SyncResponse::UpdateTarget(target) => {
                        tracing::debug!(
                            ?current_target,
                            new_target =? target,
                            "updating statesync target"
                        );
                        current_target = target;
                        sync_ctx = Some(B::create(
                            &db_paths,
                            &genesis_path,
                            request_tx.clone(),
                            target,
                        ));
                        progress.lock().unwrap().update_target(target)
                    }
                    SyncResponse::Response((from, request, response)) => {
                        let sync_ctx = sync_ctx.as_mut().expect("sync_ctx must be set");
                        for upsert in &response.response {
                            let upsert_result = sync_ctx.handle_upsert(
                                request.prefix,
                                upsert.upsert_type,
                                &upsert.data,
                            );
                            assert!(upsert_result, "failed upsert for response: {:?}", &response);
                        }
                        if response.response_n != 0 {
                            sync_ctx.handle_done(request.prefix, SeqNum(response.response_n));
                        }
                        request_tx
                            .send(SyncRequest::Completion(from))
                            .expect("request_rx dropped");
                        progress.lock().unwrap().update_handled_request(&request)
                    }
                }
                let ctx = sync_ctx.as_mut().expect("sync_ctx must be set");
                if ctx.try_finalize() {
                    tracing::debug!(?current_target, "done statesync");
                    request_tx
                        .send(SyncRequest::DoneSync(current_target))
                        .expect("request_rx dropped mid DoneSync");

                    sync_ctx = None;
                    progress
                        .lock()
                        .unwrap()
                        .update_reached_target(current_target);
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

    pub fn update_target(&mut self, target: Target) {
        if let Some(old_target) = self.current_target {
            assert!(old_target.n < target.n);
        }
        self.current_target = Some(target);
        self.outbound_requests.clear();
        self.response_tx
            .send(SyncResponse::UpdateTarget(target))
            .expect("response_rx dropped");
    }

    pub fn handle_response(&mut self, from: NodeId<PT>, response: StateSyncResponse) {
        if !response.version.is_compatible() {
            tracing::debug!(
                ?from,
                ?response,
                ?SELF_STATESYNC_VERSION,
                "dropping statesync response, version incompatible"
            );
            return;
        }

        for (request, ready_response) in self.outbound_requests.handle_response(from, response) {
            self.response_tx
                .send(SyncResponse::Response((from, request, ready_response)))
                .expect("response_rx dropped");
        }
    }

    /// An estimate of current sync progress in `Target` units
    pub fn progress_estimate(&self) -> Option<SeqNum> {
        self.progress.lock().unwrap().estimate()
    }
}

impl<PT: PubKey> Stream for StateSync<PT> {
    type Item = SyncRequest<(NodeId<PT>, StateSyncRequest), PT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        while let Poll::Ready(request) = this.request_rx.poll_recv(cx) {
            match request.expect("request_tx dropped") {
                SyncRequest::Request(request) => {
                    if this
                        .current_target
                        .is_some_and(|current_target| current_target.n != SeqNum(request.target))
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

                    return Poll::Ready(Some(SyncRequest::DoneSync(target)));
                }
                SyncRequest::Completion(from) => {
                    return Poll::Ready(Some(SyncRequest::Completion(from)));
                }
            }
        }

        let fut = this.outbound_requests.poll(&this.state_sync_peers);
        if let Poll::Ready(res) = pin!(fut).poll_unpin(cx) {
            return Poll::Ready(Some(SyncRequest::Request(res)));
        }

        Poll::Pending
    }
}

pub trait SyncClientBackend<PT: PubKey> {
    fn create(
        dbname_paths: &[String],
        genesis_file: &str,
        request_tx: tokio::sync::mpsc::UnboundedSender<SyncRequest<StateSyncRequest, PT>>,
        target: Target,
    ) -> Self;

    fn handle_upsert(
        &mut self,
        prefix: u64,
        upsert_type: StateSyncUpsertType,
        upsert_data: &[u8],
    ) -> bool;

    fn handle_done(&mut self, prefix: u64, n: SeqNum);

    fn try_finalize(&mut self) -> bool;
}

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
    StateSyncRequest, StateSyncResponse, StateSyncSessionId, StateSyncUpsertType,
    SELF_STATESYNC_VERSION,
};
use monad_types::{NodeId, SeqNum};
use rand::{thread_rng, Rng};

use crate::outbound_requests::OutboundRequests;

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
pub(crate) enum SyncRequest<R, PT: PubKey> {
    Request(R),
    DoneSync(Target),
    Completion(NodeId<PT>),
}

/// This name is confusing, but I can't think of a better name. This is basically an input event
/// from the perspective of this module. InputEvent would also be a confusing name, because from
/// the perspetive of the caller, this is an output event.
pub(crate) enum SyncResponse<P: PubKey> {
    Response((NodeId<P>, StateSyncRequest, StateSyncResponse)),
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
    pub fn start<B: SyncClientBackend>(
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
            let mut sync_ctx = SyncCtx::<B>::create(db_paths, genesis_path);
            let mut current_target = None;

            while let Ok(response) = response_rx.recv() {
                match response {
                    SyncResponse::UpdateTarget(target) => {
                        tracing::debug!(
                            ?current_target,
                            new_target =? target,
                            "updating statesync target"
                        );
                        current_target = Some(target);
                        progress.lock().unwrap().update_target(target);
                        let from = if sync_ctx.current == INVALID_SEQ_NUM {
                            0
                        } else {
                            sync_ctx.current.0 + 1
                        };
                        if from != target.n.0 {
                            for prefix in 0..NUM_PREFIXES {
                                sync_ctx.done[prefix as usize] = false;
                                request_tx
                                    .send(SyncRequest::Request(StateSyncRequest {
                                        version: SELF_STATESYNC_VERSION,
                                        session_id: StateSyncSessionId(thread_rng().gen()),
                                        prefix,
                                        prefix_bytes: 1,
                                        target: target.n.0,
                                        from,
                                        until: target.n.0,
                                        old_target: sync_ctx.current.0,
                                    }))
                                    .expect("request_rx dropped mid UpdateTarget");
                            }
                        }
                    }
                    SyncResponse::Response((from, request, response)) => {
                        assert!(current_target.is_some());
                        for (upsert_type, upsert_data) in &response.response {
                            let upsert_result = sync_ctx.ctx.handle_upsert(
                                request.prefix,
                                *upsert_type,
                                &upsert_data,
                            );
                            assert!(upsert_result, "failed upsert for response: {:?}", &response);
                        }
                        if response.response_n != 0 {
                            sync_ctx.done[request.prefix as usize] = true;
                        }
                        request_tx
                            .send(SyncRequest::Completion(from))
                            .expect("request_rx dropped");
                        progress.lock().unwrap().update_handled_request(&request)
                    }
                }
                let target = current_target.expect("current_target must have been set");
                if sync_ctx.try_finalize(target) {
                    tracing::debug!(?target, "done statesync");
                    request_tx
                        .send(SyncRequest::DoneSync(target))
                        .expect("request_rx dropped mid DoneSync");

                    current_target = None;
                    progress.lock().unwrap().update_reached_target(target);
                }
            }
            // this loop exits when execution is about to start
        });

        Self {
            state_sync_peers: state_sync_peers,
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

pub trait SyncClientBackend {
    fn create(dbname_paths: &[String], genesis_file: &str) -> Self;

    fn handle_upsert(
        &mut self,
        prefix: u64,
        upsert_type: StateSyncUpsertType,
        upsert_data: &[u8],
    ) -> bool;

    fn latest_block(&self) -> SeqNum;

    fn commit(&mut self);

    fn finalize(&mut self, target: Target) -> bool;
}

/// Thin unsafe wrapper around statesync_client_context that handles destruction and finalization
/// checking
struct SyncCtx<B: SyncClientBackend> {
    dbname_paths: Vec<String>,
    genesis_file: String,
    ctx: B,
    done: Vec<bool>,
    current: SeqNum,
}

impl<B: SyncClientBackend> SyncCtx<B> {
    /// Initialize SyncCtx. There should only ever be *one* SyncCtx at any given time.
    fn create(dbname_paths: Vec<String>, genesis_file: String) -> Self {
        let num_prefixes = NUM_PREFIXES as usize;
        let ctx = B::create(&dbname_paths, &genesis_file);
        let current = ctx.latest_block();
        Self {
            dbname_paths,
            genesis_file,
            ctx,
            done: vec![false; num_prefixes],
            current,
        }
    }

    /// Returns true if reached target and successfully finalized
    fn try_finalize(&mut self, target: Target) -> bool {
        let done = self.done.iter().all(|done| *done);
        tracing::info!("try_finalize: {:?} {:?} {}", self.current, target.n, done);
        if target.n == SeqNum(0) || self.current == target.n || done {
            if done {
                self.ctx.commit();
            }

            let root_matches = self.ctx.finalize(target);
            assert!(root_matches, "state root doesn't match, are peers trusted?");
            self.ctx = B::create(&self.dbname_paths, &self.genesis_file);
            return true;
        }
        false
    }
}

use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use ffi::SyncRequest;
use futures::{Stream, StreamExt};
use ipc::StateSyncIpc;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, StateSyncCommand, StateSyncEvent, StateSyncNetworkMessage};
use monad_types::NodeId;

use crate::ffi::{SyncClientBackend, Target};

mod ffi;
mod ipc;
mod outbound_requests;
mod triedb_client;

pub use triedb_client::TriedbSyncClient;

#[allow(
    dead_code,
    non_camel_case_types,
    non_upper_case_globals,
    non_snake_case
)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/state_sync.rs"));
}

const GAUGE_STATESYNC_PROGRESS_ESTIMATE: &str = "monad.statesync.progress_estimate";
const GAUGE_STATESYNC_LAST_TARGET: &str = "monad.statesync.last_target";

pub struct StateSync<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
{
    incoming_request_timeout: Duration,
    uds_path: String,

    mode: StateSyncMode<CertificateSignaturePubKey<ST>>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    _phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> StateSync<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new<B: SyncClientBackend<CertificateSignaturePubKey<ST>>>(
        db_paths: Vec<String>,
        genesis_path: String,
        state_sync_peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
        max_parallel_requests: usize,
        request_timeout: Duration,
        incoming_request_timeout: Duration,
        uds_path: String,
    ) -> Self {
        Self {
            incoming_request_timeout,
            uds_path,

            mode: StateSyncMode::Sync(ffi::StateSync::start::<B>(
                db_paths,
                genesis_path,
                state_sync_peers,
                max_parallel_requests,
                request_timeout,
            )),

            waker: None,
            metrics: Default::default(),
            _phantom: Default::default(),
        }
    }
}

enum StateSyncMode<PT: PubKey> {
    Sync(ffi::StateSync<PT>),
    /// transitions to Live once the StartExecution command is executed
    /// note that Live -> Sync is not a valid state transition
    Live(StateSyncIpc<PT>),
}

impl<ST, SCT> Executor for StateSync<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = StateSyncCommand<CertificateSignaturePubKey<ST>>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                StateSyncCommand::RequestSync(state_root_info) => {
                    let statesync = match &mut self.mode {
                        StateSyncMode::Sync(sync) => sync,
                        StateSyncMode::Live(_) => {
                            unreachable!("Live -> Sync is not a valid state transition")
                        }
                    };
                    statesync.update_target(Target {
                        n: state_root_info.seq_num,
                        state_root: state_root_info.state_root_hash.0 .0,
                    });

                    self.metrics[GAUGE_STATESYNC_LAST_TARGET] = state_root_info.seq_num.0;
                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
                StateSyncCommand::Message((from, StateSyncNetworkMessage::Response(response))) => {
                    let statesync = match &mut self.mode {
                        StateSyncMode::Sync(sync) => sync,
                        StateSyncMode::Live(_) => {
                            tracing::trace!(
                                ?from,
                                "dropping statesync response, already done syncing"
                            );
                            continue;
                        }
                    };
                    statesync.handle_response(from, response);
                }
                StateSyncCommand::Message((from, StateSyncNetworkMessage::Completion)) => {
                    let execution_ipc = match &mut self.mode {
                        StateSyncMode::Sync(_) => {
                            tracing::trace!(?from, "dropping statesync request, still syncing");
                            continue;
                        }
                        StateSyncMode::Live(live) => live,
                    };
                    if execution_ipc
                        .request_tx
                        .try_send((from, StateSyncNetworkMessage::Completion))
                        .is_err()
                    {
                        tracing::warn!("dropping inbound completion, execution backlogged?")
                    }
                }
                StateSyncCommand::Message((from, StateSyncNetworkMessage::Request(request))) => {
                    let execution_ipc = match &mut self.mode {
                        StateSyncMode::Sync(_) => {
                            tracing::trace!(?from, "dropping statesync request, still syncing");
                            continue;
                        }
                        StateSyncMode::Live(live) => live,
                    };
                    if execution_ipc
                        .request_tx
                        .try_send((from, StateSyncNetworkMessage::Request(request)))
                        .is_err()
                    {
                        tracing::warn!("dropping inbound statesync request, execution backlogged?")
                    }
                }
                StateSyncCommand::StartExecution => {
                    let valid_transition = match self.mode {
                        StateSyncMode::Sync(_) => true,
                        StateSyncMode::Live(_) => false,
                    };
                    assert!(valid_transition);
                    self.mode = StateSyncMode::Live(StateSyncIpc::new(
                        &self.uds_path,
                        self.incoming_request_timeout,
                    ));
                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT> Stream for StateSync<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Unpin,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        match &mut this.mode {
            StateSyncMode::Sync(sync) => {
                if let Some(progress) = sync.progress_estimate() {
                    this.metrics[GAUGE_STATESYNC_PROGRESS_ESTIMATE] = progress.0;
                }

                if let Poll::Ready(event) = sync.poll_next_unpin(cx) {
                    let event = match event.expect("StateSyncMode::Sync event channel dropped") {
                        SyncRequest::Request((servicer, request)) => {
                            tracing::debug!(?request, ?servicer, "sending request");
                            StateSyncEvent::Outbound(
                                servicer,
                                StateSyncNetworkMessage::Request(request),
                            )
                        }
                        SyncRequest::DoneSync(target) => StateSyncEvent::DoneSync(target.n),
                        SyncRequest::Completion(servicer) => {
                            StateSyncEvent::Outbound(servicer, StateSyncNetworkMessage::Completion)
                        }
                    };
                    return Poll::Ready(Some(MonadEvent::StateSyncEvent(event)));
                }
            }
            StateSyncMode::Live(execution_ipc) => {
                if let Poll::Ready(maybe_response) = execution_ipc.response_rx.poll_recv(cx) {
                    let (to, response) = maybe_response.expect("did StateSyncIpc die?");
                    tracing::debug!(
                        ?to,
                        ?response,
                        upserts_len = response.response.len(),
                        "sending response"
                    );
                    return Poll::Ready(Some(MonadEvent::StateSyncEvent(
                        StateSyncEvent::Outbound(to, StateSyncNetworkMessage::Response(response)),
                    )));
                }
            }
        };

        Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use lazy_static::lazy_static;
    use monad_consensus_types::state_root_hash::{StateRootHash, StateRootHashInfo};
    use monad_crypto::{NopPubKey, NopSignature};
    use monad_executor::Executor;
    use monad_executor_glue::{StateSyncRequest, StateSyncResponse, SELF_STATESYNC_VERSION};
    use monad_multi_sig::MultiSig;
    use monad_types::{Hash, NodeId, SeqNum};
    use rand::{thread_rng, Rng};
    use tokio::time::timeout;

    use super::*;
    use crate::ffi::NUM_PREFIXES;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;

    struct TestSyncClientBackend<PT: PubKey> {
        s: String,
        request_tx: tokio::sync::mpsc::UnboundedSender<SyncRequest<StateSyncRequest, PT>>,
        done: [bool; NUM_PREFIXES as usize],
        target: Target,
    }

    struct BackendState {
        latest_block: SeqNum,
    }

    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    lazy_static! {
        static ref BACKEND_STATE: Arc<Mutex<HashMap<String, BackendState>>> =
            Arc::new(Mutex::new(Default::default()));
    }

    fn add_backend_state(s: String, latest_block: SeqNum) {
        BACKEND_STATE
            .lock()
            .unwrap()
            .insert(s, BackendState { latest_block });
    }

    fn delete_backend_state(s: String) {
        BACKEND_STATE.lock().unwrap().remove(&s);
    }

    impl<PT: PubKey> SyncClientBackend<PT> for TestSyncClientBackend<PT> {
        fn create(
            dbname_paths: &[String],
            _genesis_file: &str,
            request_tx: tokio::sync::mpsc::UnboundedSender<SyncRequest<StateSyncRequest, PT>>,
            target: Target,
        ) -> Self {
            let ctx = Self {
                s: dbname_paths[0].clone(),
                request_tx,
                done: [false; NUM_PREFIXES as usize],
                target,
            };

            let latest_block = BACKEND_STATE
                .lock()
                .unwrap()
                .get(&ctx.s)
                .unwrap()
                .latest_block;
            if target.n != SeqNum(0) {
                for i in 0..NUM_PREFIXES {
                    let from = if latest_block == SeqNum(u64::MAX) {
                        SeqNum(0)
                    } else {
                        latest_block
                    };
                    let req = StateSyncRequest {
                        version: SELF_STATESYNC_VERSION,
                        prefix: i,
                        prefix_bytes: 1,
                        from: from.0,
                        until: target.n.0,
                        old_target: from.0,
                        target: target.n.0,
                    };
                    ctx.request_tx.send(SyncRequest::Request(req)).unwrap();
                }
            }
            ctx
        }

        fn handle_upsert(
            &mut self,
            _prefix: u64,
            _upsert_type: monad_executor_glue::StateSyncUpsertType,
            _upsert_data: &[u8],
        ) -> bool {
            true
        }

        fn handle_done(&mut self, prefix: u64, _n: SeqNum) {
            self.done[prefix as usize] = true;
        }

        fn try_finalize(&mut self) -> bool {
            if self.target.n == SeqNum(0) {
                return true;
            }
            self.done.iter().all(|&x| x)
        }
    }

    #[tokio::test]
    async fn test_genesis_sync() {
        let db_name = thread_rng().gen::<u64>().to_string();
        add_backend_state(db_name.clone(), SeqNum(u64::MAX));
        let mut state_sync = StateSync::<SignatureType, SignatureCollectionType>::new::<
            TestSyncClientBackend<NopPubKey>,
        >(
            vec![db_name.clone()],
            "genesis_path".to_string(),
            vec![NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap())],
            1,
            Duration::from_secs(1),
            Duration::from_secs(1),
            "uds_path".to_string(),
        );

        let commands = vec![StateSyncCommand::RequestSync(StateRootHashInfo {
            seq_num: SeqNum(0),
            state_root_hash: StateRootHash(Hash([0; 32])),
        })];

        state_sync.exec(commands);

        loop {
            match timeout(Duration::from_secs(5), state_sync.next()).await {
                Ok(Some(MonadEvent::StateSyncEvent(StateSyncEvent::DoneSync(_)))) => {
                    break;
                }
                Ok(Some(e)) => {
                    assert!(
                        false,
                        "sync to zero block should succeed immediately, got event {:?}",
                        e
                    );
                }
                Ok(None) => {
                    panic!("unexpected stream end");
                }
                Err(_) => {
                    panic!("timeout");
                }
            }
        }

        delete_backend_state(db_name);
    }

    use tracing_subscriber;

    #[tokio::test]
    async fn test_client_completion() {
        let _ = tracing_subscriber::fmt::try_init();

        let db_name = thread_rng().gen::<u64>().to_string();
        add_backend_state(db_name.clone(), SeqNum(u64::MAX));

        let max_in_flight = 2;
        let mut state_sync = StateSync::<SignatureType, SignatureCollectionType>::new::<
            TestSyncClientBackend<NopPubKey>,
        >(
            vec![db_name.clone()],
            "genesis_path".to_string(),
            vec![NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap())],
            max_in_flight,
            Duration::from_secs(1),
            Duration::from_secs(1),
            "uds_path".to_string(),
        );

        let commands = vec![StateSyncCommand::RequestSync(StateRootHashInfo {
            seq_num: SeqNum(1),
            state_root_hash: StateRootHash(Hash([0; 32])),
        })];

        state_sync.exec(commands);

        let mut messages = Vec::new();
        loop {
            match timeout(Duration::from_secs(1), state_sync.next()).await {
                Ok(Some(MonadEvent::StateSyncEvent(StateSyncEvent::DoneSync(_)))) => {
                    assert!(false, "sync should send messages");
                }
                Ok(Some(MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(n, m)))) => {
                    println!("got message from {:?}: {:?}", n, m);
                    messages.push((n, m));
                    if messages.len() == max_in_flight {
                        break;
                    }
                }
                Ok(Some(MonadEvent::StateSyncEvent(e))) => {
                    assert!(false, "unexpected event {:?}", e);
                }
                Ok(Some(e)) => {
                    assert!(false, "unexpected event {:?}", e);
                }
                Ok(None) => {
                    assert!(false, "unexpected stream end");
                }
                Err(_) => {
                    assert!(false, "timeout");
                }
            }
        }

        let request = if let StateSyncNetworkMessage::Request(req) = &messages[0].1 {
            req.clone()
        } else {
            panic!("Expected StateSyncNetworkMessage::Request");
        };
        let msg = StateSyncNetworkMessage::Response(StateSyncResponse {
            version: SELF_STATESYNC_VERSION,
            nonce: 1,
            response: vec![],
            request,
            response_index: 0,
            response_n: 1,
        });
        let commands = vec![StateSyncCommand::Message((messages[0].0, msg))];
        state_sync.exec(commands);

        let mut got_completion = false;
        let mut got_request = false;
        for _ in 0..2 {
            let event = timeout(Duration::from_secs(1), state_sync.next())
                .await
                .expect("timeout")
                .expect("unexpected stream end");
            println!("event: {:?}", event);
            match event {
                MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                    to,
                    StateSyncNetworkMessage::Completion,
                )) => {
                    assert!(
                        to == messages[0].0,
                        "client should send completion to same peer"
                    );
                    got_completion = true;
                }
                MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(_, _)) => {
                    got_request = true;
                }
                e => {
                    assert!(false, "unexpected event {:?}", e);
                }
            }
        }

        assert!(got_completion, "client should send completion");
        assert!(got_request, "client should send next request");

        delete_backend_state(db_name);
    }
}

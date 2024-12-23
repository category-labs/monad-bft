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
use monad_executor_glue::{
    MonadEvent, StateSyncCommand, StateSyncEvent, StateSyncNetworkMessage, StateSyncRequest,
    StateSyncResponseBody,
};
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

impl From<StateSyncRequest> for bindings::monad_sync_request {
    fn from(req: StateSyncRequest) -> Self {
        Self {
            from: req.from,
            until: req.until,
            old_target: req.old_target,
            target: req.target,
            prefix: req.prefix,
            prefix_bytes: req.prefix_bytes,
        }
    }
}

const GAUGE_STATESYNC_PROGRESS_ESTIMATE: &str = "monad.statesync.progress_estimate";
const GAUGE_STATESYNC_LAST_TARGET: &str = "monad.statesync.last_target";

#[derive(Debug, Copy, Clone)]
pub struct StateSyncConfig {
    pub max_parallel_requests: usize, // max number of client requests to send in parallel
    pub request_timeout: Duration,    // client request timeout
    pub incoming_request_timeout: Duration, // server request timeout
    pub max_outstanding_responses: usize, // max server outstanding responses
    pub max_response_size: usize,     // max server response message size
}

pub struct StateSync<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
{
    config: StateSyncConfig,
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
    pub fn new<B: SyncClientBackend>(
        db_paths: Vec<String>,
        genesis_path: String,
        state_sync_peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
        config: StateSyncConfig,
        uds_path: String,
    ) -> Self {
        Self {
            config,
            uds_path,

            mode: StateSyncMode::Sync(ffi::StateSync::start::<B>(
                db_paths,
                genesis_path,
                state_sync_peers,
                config.max_parallel_requests,
                config.request_timeout,
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
                    self.mode = StateSyncMode::Live(StateSyncIpc::new(&self.uds_path, self.config));
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
                        upserts_len = match &response.body {
                            StateSyncResponseBody::Ok(ok) => ok.response.len(),
                            StateSyncResponseBody::Err(_) => 0,
                        },
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
    use std::{
        collections::HashMap,
        fs,
        path::Path,
        sync::{Arc, Mutex},
    };

    use futures::task::noop_waker;
    use lazy_static::lazy_static;
    use monad_consensus_types::state_root_hash::{StateRootHash, StateRootHashInfo};
    use monad_crypto::{NopPubKey, NopSignature};
    use monad_executor::Executor;
    use monad_executor_glue::{
        StateSyncRequest, StateSyncResponse, StateSyncResponseOk, StateSyncSessionId,
        StateSyncUpsert, StateSyncUpsertType, StateSyncVersion, SELF_STATESYNC_VERSION,
    };
    use monad_multi_sig::MultiSig;
    use monad_proto::proto::message::StateSyncResponseErr;
    use monad_types::{Hash, NodeId, SeqNum};
    use rand::{thread_rng, Rng};
    use tokio::{io::Interest, time::timeout};
    use tracing_subscriber;

    use super::*;
    use crate::{ffi::NUM_PREFIXES, ipc::SyncRequest};

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;

    struct TestSyncClientBackend {
        backend: Arc<Mutex<BackendState>>,
    }

    struct BackendState {
        latest_block: SeqNum,
        uds_path: String,
        shutdown_tx: Option<tokio::sync::mpsc::Sender<()>>,
        handle: Option<tokio::task::JoinHandle<()>>,
        upserts: Vec<Vec<Vec<u8>>>,
        upserts_bufs: Vec<Vec<u8>>,
    }

    impl BackendState {
        fn new(latest_block: SeqNum, uds_path: String) -> Self {
            Self {
                latest_block,
                uds_path,
                shutdown_tx: None,
                upserts: vec![Vec::new(); NUM_PREFIXES as usize],
                upserts_bufs: vec![Vec::new(); NUM_PREFIXES as usize],
                handle: None,
            }
        }

        fn run_server(&mut self) {
            let (tx, mut rx) = tokio::sync::mpsc::channel(1);
            let uds_path = self.uds_path.clone();
            let upserts_bufs = self.upserts_bufs.clone();
            let server = async move {
                let mut stream = loop {
                    tokio::select! {
                        _ = rx.recv() => {
                            return;
                        }
                        result = tokio::net::UnixStream::connect(&uds_path) => {
                            match result {
                                Ok(server) => {
                                    break server;
                                }
                                Err(_) => {
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                            }
                        }
                    }
                };
                let mut request: Option<SyncRequest> = None;
                let mut offset = 0;
                let mut request_buf =
                    [0_u8; std::mem::size_of::<bindings::monad_sync_request>() + 1];
                let mut read_offset = 0;
                loop {
                    let interest = if request.is_none() {
                        Interest::READABLE
                    } else {
                        Interest::WRITABLE
                    };
                    println!("waiting for interest {:?}", interest);
                    tokio::select! {
                        _ = rx.recv() => {
                            return;
                        }
                        result = stream.ready(interest) => {
                            match result {
                                Ok(result) => {
                                    if result.is_readable() {
                                        while read_offset < request_buf.len() {
                                            match stream.try_read(&mut request_buf[read_offset..]) {
                                                Ok(0) => {
                                                    println!("reconnecting");
                                                    stream = loop {
                                                            tokio::select! {
                                                            _ = rx.recv() => {
                                                                return;
                                                            }
                                                            result = tokio::net::UnixStream::connect(&uds_path) => {
                                                                match result {
                                                                    Ok(server) => {
                                                                        break server;
                                                                    }
                                                                    Err(_) => {
                                                                        tokio::time::sleep(Duration::from_millis(100)).await;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    };
                                                    read_offset = 0;
                                                    request = None;
                                                    offset = 0;
                                                    continue;
                                                }
                                                Ok(n) => {
                                                    read_offset += n;
                                                }
                                                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                                    break;
                                                }
                                                Err(e) => {
                                                    eprintln!("read failed: {:?}", e);
                                                    return;
                                                }
                                            }
                                        }
                                        if read_offset == request_buf.len() {
                                            let typ = request_buf[0];
                                            let mut buf = [0_u8; std::mem::size_of::<bindings::monad_sync_request>()];
                                            buf.copy_from_slice(&request_buf[1..]);
                                            match typ {
                                                bindings::monad_sync_type_SYNC_TYPE_REQUEST => {
                                                    request = Some(unsafe {
                                                        std::mem::transmute::<_, bindings::monad_sync_request>(buf)
                                                    });
                                                    println!("got request {:?}", request);
                                                }
                                                _ => {
                                                    eprintln!("unexpected type: {}", typ);
                                                    return;
                                                }
                                            }
                                            read_offset = 0;
                                        }
                                    }
                                    if result.is_writable() {
                                        let mut buf = upserts_bufs[request.unwrap().prefix as usize].clone();
                                        buf.push(bindings::monad_sync_type_SYNC_TYPE_DONE);
                                        let done = bindings::monad_sync_done {
                                            prefix: request.unwrap().prefix,
                                            n: request.unwrap().target,
                                            success: true,
                                        };
                                        let done_buf = unsafe {
                                            std::mem::transmute::<_, [u8; std::mem::size_of::<bindings::monad_sync_done>()]>(done)
                                        };
                                        buf.extend_from_slice(&done_buf);
                                        while offset < buf.len() {
                                            match stream.try_write(&buf[offset..]) {
                                                Ok(n) => {
                                                    offset += n;
                                                }
                                                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                                    break;
                                                }
                                                Err(e) => {
                                                    eprintln!("write failed: {:?}", e);
                                                    return;
                                                }
                                            }
                                        }
                                        if offset == buf.len() {
                                            offset = 0;
                                            request = None;
                                        }
                                    }
                                }
                                Err(_) => {
                                    return;
                                }
                            }
                        }
                    }
                }
            };
            self.handle = Some(tokio::spawn(server));
            self.shutdown_tx = Some(tx);
        }

        fn handle_upsert(
            &mut self,
            prefix: u64,
            upsert_type: StateSyncUpsertType,
            upsert_data: &[u8],
        ) {
            self.upserts[prefix as usize].push(upsert_data.to_vec());
            self.upserts_bufs[prefix as usize].push(to_binding_type(upsert_type));
            self.upserts_bufs[prefix as usize].extend_from_slice(&upsert_data.len().to_le_bytes());
            self.upserts_bufs[prefix as usize].extend_from_slice(upsert_data);
        }

        async fn shutdown(&mut self) {
            if let Some(tx) = self.shutdown_tx.take() {
                let _ = tx.send(());
            }
            if let Some(handle) = self.handle.take() {
                let _ = handle.await;
            }
        }
    }

    lazy_static! {
        static ref BACKEND_STATE: Arc<Mutex<HashMap<String, Arc<Mutex<BackendState>>>>> =
            Arc::new(Mutex::new(Default::default()));
    }

    fn add_backend_state(s: String, latest_block: SeqNum, uds_path: String) {
        BACKEND_STATE.lock().unwrap().insert(
            s,
            Arc::new(Mutex::new(BackendState::new(latest_block, uds_path))),
        );
    }

    fn delete_backend_state(s: &str) {
        BACKEND_STATE.lock().unwrap().remove(s);
    }

    fn get_backend_state(s: &str) -> Arc<Mutex<BackendState>> {
        BACKEND_STATE
            .lock()
            .unwrap()
            .get(s)
            .expect("state not found")
            .clone()
    }

    fn to_binding_type(t: StateSyncUpsertType) -> u8 {
        match t {
            StateSyncUpsertType::Account => bindings::monad_sync_type_SYNC_TYPE_UPSERT_ACCOUNT,
            StateSyncUpsertType::Code => bindings::monad_sync_type_SYNC_TYPE_UPSERT_CODE,
            StateSyncUpsertType::Storage => bindings::monad_sync_type_SYNC_TYPE_UPSERT_STORAGE,
            StateSyncUpsertType::AccountDelete => {
                bindings::monad_sync_type_SYNC_TYPE_UPSERT_ACCOUNT_DELETE
            }
            StateSyncUpsertType::StorageDelete => {
                bindings::monad_sync_type_SYNC_TYPE_UPSERT_STORAGE_DELETE
            }
        }
    }

    impl SyncClientBackend for TestSyncClientBackend {
        fn create(dbname_paths: &[String], _genesis_file: &str) -> Self {
            Self {
                backend: get_backend_state(&dbname_paths[0]),
            }
        }

        fn commit(&mut self) {}

        fn handle_upsert(
            &mut self,
            prefix: u64,
            upsert_type: monad_executor_glue::StateSyncUpsertType,
            upsert_data: &[u8],
        ) -> bool {
            self.backend
                .lock()
                .unwrap()
                .handle_upsert(prefix, upsert_type, upsert_data);
            true
        }

        fn finalize(&mut self, _target: Target) -> bool {
            true
        }

        fn latest_block(&self) -> SeqNum {
            self.backend.lock().unwrap().latest_block
        }
    }

    struct BackendHandle {
        db_name: String,
        backend: Arc<Mutex<BackendState>>,
    }

    impl BackendHandle {
        fn new(latest_block: SeqNum) -> Self {
            let db_name = thread_rng().gen::<u64>().to_string();
            let uds_path = format!("/tmp/{}", thread_rng().gen::<u64>().to_string());
            add_backend_state(db_name.clone(), latest_block, uds_path);
            let backend = get_backend_state(&db_name);
            Self { db_name, backend }
        }

        fn uds_path(&self) -> String {
            self.backend.lock().unwrap().uds_path.clone()
        }
    }

    impl Drop for BackendHandle {
        fn drop(&mut self) {
            let _ = fs::remove_file(Path::new(&self.uds_path()));
            delete_backend_state(&self.db_name);
        }
    }

    fn make_config() -> StateSyncConfig {
        StateSyncConfig {
            max_parallel_requests: 2,
            request_timeout: Duration::from_secs(1),
            incoming_request_timeout: Duration::from_secs(1),
            max_outstanding_responses: 2,
            max_response_size: 4 * 1024,
        }
    }

    #[tokio::test]
    async fn test_genesis_sync() {
        let bh = BackendHandle::new(SeqNum(u64::MAX));
        let mut state_sync =
            StateSync::<SignatureType, SignatureCollectionType>::new::<TestSyncClientBackend>(
                vec![bh.db_name.clone()],
                "genesis_path".to_string(),
                vec![NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap())],
                make_config(),
                bh.uds_path(),
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
    }

    #[tokio::test]
    async fn test_client_completion() {
        let _ = tracing_subscriber::fmt::try_init();

        let bh = BackendHandle::new(SeqNum(u64::MAX));

        let config = make_config();
        let mut state_sync =
            StateSync::<SignatureType, SignatureCollectionType>::new::<TestSyncClientBackend>(
                vec![bh.db_name.clone()],
                "genesis_path".to_string(),
                vec![NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap())],
                config.clone(),
                bh.uds_path(),
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
                    if messages.len() == config.max_parallel_requests {
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
            session_id: request.session_id,
            body: StateSyncResponseBody::Ok(StateSyncResponseOk {
                response: vec![],
                response_index: 0,
                response_n: 1,
            }),
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
    }

    fn stream_idle(s: &mut StateSync<SignatureType, SignatureCollectionType>) -> bool {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut s_pin = Pin::new(s);
        match s_pin.as_mut().poll_next(&mut cx) {
            Poll::Pending => true,
            Poll::Ready(Some(e)) => {
                eprintln!("unexpected event {:?}", e);
                false
            }
            Poll::Ready(None) => {
                eprintln!("unexpected stream end");
                false
            }
        }
    }

    #[tokio::test]
    async fn test_server() {
        let _ = tracing_subscriber::fmt::try_init();

        let bh = BackendHandle::new(SeqNum(u64::MAX));

        let node = NodeId::new(NopPubKey::from_bytes(&[1; 32]).unwrap());

        let mut state_sync =
            StateSync::<SignatureType, SignatureCollectionType>::new::<TestSyncClientBackend>(
                vec![bh.db_name.clone()],
                "genesis_path".to_string(),
                vec![NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap())],
                make_config(),
                bh.uds_path(),
            );

        let commands = vec![StateSyncCommand::StartExecution];
        state_sync.exec(commands);

        assert!(matches!(state_sync.mode, StateSyncMode::Live(_)));

        assert!(stream_idle(&mut state_sync));

        {
            let mut backend = bh.backend.lock().unwrap();
            backend.handle_upsert(0, StateSyncUpsertType::Account, &[1; 20]);
            backend.handle_upsert(0, StateSyncUpsertType::Account, &[2; 20]);

            backend.run_server();
        }

        let session_id = StateSyncSessionId(thread_rng().gen::<u64>());
        let cmds = vec![StateSyncCommand::Message((
            node,
            StateSyncNetworkMessage::Request(StateSyncRequest {
                version: SELF_STATESYNC_VERSION,
                session_id: session_id,
                from: 0,
                until: 1,
                old_target: 0,
                target: 1,
                prefix: 0,
                prefix_bytes: 1,
            }),
        ))];

        state_sync.exec(cmds);

        let e = timeout(Duration::from_secs(1), state_sync.next())
            .await
            .expect("timeout")
            .expect("stream closed unexpectedly");
        match e {
            MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                n,
                StateSyncNetworkMessage::Response(r),
            )) => {
                assert_eq!(n, node);
                match r.body {
                    StateSyncResponseBody::Ok(r) => {
                        assert_eq!(r.response_index, 0);
                        assert_eq!(r.response_n, 1);
                        assert_eq!(r.response.len(), 2);
                    }
                    StateSyncResponseBody::Err(_) => {
                        assert!(false, "unexpected error response");
                    }
                }
                assert_eq!(r.session_id, session_id);
            }
            _ => {
                assert!(
                    false,
                    "unexpected event {:?}, should produce response message",
                    e
                );
            }
        }

        bh.backend.lock().unwrap().shutdown().await;
    }

    #[tokio::test]
    async fn test_server_flow_control() {
        let _ = tracing_subscriber::fmt::try_init();

        let bh = BackendHandle::new(SeqNum(u64::MAX));

        let node = NodeId::new(NopPubKey::from_bytes(&[1; 32]).unwrap());
        let config = make_config();
        let mut state_sync =
            StateSync::<SignatureType, SignatureCollectionType>::new::<TestSyncClientBackend>(
                vec![bh.db_name.clone()],
                "genesis_path".to_string(),
                vec![NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap())],
                config.clone(),
                bh.uds_path(),
            );

        let commands = vec![StateSyncCommand::StartExecution];
        state_sync.exec(commands);

        assert!(matches!(state_sync.mode, StateSyncMode::Live(_)));

        const DATA_SIZE: usize = 1000;
        {
            let mut backend = bh.backend.lock().unwrap();

            for i in 0..100 {
                let mut data = [i as u8; DATA_SIZE];
                let n = thread_rng().gen::<u64>();
                data[0..8].copy_from_slice(n.to_le_bytes().as_ref());
                backend.handle_upsert(0, StateSyncUpsertType::Account, &data);
            }

            backend.run_server();
        }

        let session_id = StateSyncSessionId(thread_rng().gen::<u64>());
        let cmds = vec![StateSyncCommand::Message((
            node,
            StateSyncNetworkMessage::Request(StateSyncRequest {
                version: SELF_STATESYNC_VERSION,
                session_id,
                from: 0,
                until: 1,
                old_target: 0,
                target: 1,
                prefix: 0,
                prefix_bytes: 1,
            }),
        ))];

        state_sync.exec(cmds);

        let mut responses_sent: usize = 0;
        loop {
            let e = timeout(Duration::from_secs(1), state_sync.next())
                .await
                .expect("timeout")
                .expect("stream closed unexpectedly");
            match e {
                MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                    n,
                    StateSyncNetworkMessage::Response(r),
                )) => {
                    assert_eq!(n, node);
                    match r.body {
                        StateSyncResponseBody::Ok(r) => {
                            assert_eq!(r.response_index, responses_sent as u64);
                            assert_eq!(
                                r.response.len(),
                                config.max_response_size.div_ceil(DATA_SIZE)
                            );
                            assert_eq!(r.response_n, 0);
                        }
                        StateSyncResponseBody::Err(_) => {
                            assert!(false, "unexpected error response");
                        }
                    }
                    assert_eq!(r.session_id, session_id);
                }
                _ => {
                    assert!(
                        false,
                        "unexpected event {:?}, should produce response message",
                        e
                    );
                }
            }
            responses_sent += 1;
            if responses_sent == config.max_outstanding_responses {
                break;
            }
        }

        assert!(
            stream_idle(&mut state_sync),
            "shouldn't send more messages without completions"
        );

        let cmds = vec![StateSyncCommand::Message((
            node,
            StateSyncNetworkMessage::Completion,
        ))];

        state_sync.exec(cmds);

        let e = timeout(Duration::from_secs(1), state_sync.next())
            .await
            .expect("timeout, should send next message after completion")
            .expect("stream closed unexpectedly");
        match e {
            MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                n,
                StateSyncNetworkMessage::Response(r),
            )) => {
                assert_eq!(n, node);
                match r.body {
                    StateSyncResponseBody::Ok(r) => {
                        assert_eq!(r.response_index, responses_sent as u64);
                        assert_eq!(
                            r.response.len(),
                            config.max_response_size.div_ceil(DATA_SIZE)
                        );
                        assert_eq!(r.response_n, 0);
                    }
                    StateSyncResponseBody::Err(_) => {
                        assert!(false, "unexpected error response");
                    }
                };
                assert_eq!(r.session_id, session_id);
            }
            _ => {
                assert!(
                    false,
                    "unexpected event {:?}, should produce response message",
                    e
                );
            }
        }

        responses_sent += 1;

        assert!(
            stream_idle(&mut state_sync),
            "shouldn't send more messages without completions"
        );

        loop {
            let cmds = vec![StateSyncCommand::Message((
                node,
                StateSyncNetworkMessage::Completion,
            ))];
            state_sync.exec(cmds);

            let e = timeout(Duration::from_secs(1), state_sync.next())
                .await
                .expect("timeout, should send next message after completion")
                .expect("stream closed unexpectedly");
            match e {
                MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                    n,
                    StateSyncNetworkMessage::Response(r),
                )) => {
                    assert_eq!(n, node);
                    assert_eq!(r.session_id, session_id);
                    match r.body {
                        StateSyncResponseBody::Ok(r) => {
                            assert_eq!(r.response_index, responses_sent as u64);
                            if r.response_n == 0 {
                                assert_eq!(
                                    r.response.len(),
                                    config.max_response_size.div_ceil(DATA_SIZE)
                                );
                            } else {
                                break;
                            }
                        }
                        StateSyncResponseBody::Err(_) => {
                            assert!(false, "unexpected error response");
                        }
                    }
                }
                _ => {
                    assert!(
                        false,
                        "unexpected event {:?}, should produce response message",
                        e
                    );
                }
            }
            responses_sent += 1;
        }

        bh.backend.lock().unwrap().shutdown().await;
    }

    #[tokio::test]
    async fn test_client_incremental() {
        let _ = tracing_subscriber::fmt::try_init();

        let bh = BackendHandle::new(SeqNum(u64::MAX));

        let config = make_config();
        let mut state_sync =
            StateSync::<SignatureType, SignatureCollectionType>::new::<TestSyncClientBackend>(
                vec![bh.db_name.clone()],
                "genesis_path".to_string(),
                vec![NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap())],
                config.clone(),
                bh.uds_path(),
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
                    if messages.len() == config.max_parallel_requests {
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
        for response_index in 0..10 {
            let msg = StateSyncNetworkMessage::Response(StateSyncResponse {
                version: SELF_STATESYNC_VERSION,
                session_id: request.session_id,
                body: StateSyncResponseBody::Ok(StateSyncResponseOk {
                    response: vec![StateSyncUpsert {
                        upsert_type: StateSyncUpsertType::Account,
                        data: vec![response_index as u8; 20],
                    }],
                    response_index,
                    response_n: 0,
                }),
            });
            let commands = vec![StateSyncCommand::Message((messages[0].0, msg))];
            state_sync.exec(commands);

            let event = timeout(Duration::from_millis(500), state_sync.next())
                .await
                .expect("timeout, should send completions after each response")
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
                }
                MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(_, _)) => {
                    assert!(false, "shouldn't send more requests until finished one");
                }
                e => {
                    assert!(
                        false,
                        "unexpected event {:?}, should send competions after each response",
                        e
                    );
                }
            }
        }
        let msg = StateSyncNetworkMessage::Response(StateSyncResponse {
            version: SELF_STATESYNC_VERSION,
            session_id: request.session_id,
            body: StateSyncResponseBody::Ok(StateSyncResponseOk {
                response: vec![StateSyncUpsert {
                    upsert_type: StateSyncUpsertType::Account,
                    data: vec![10 as u8; 20],
                }],
                response_index: 10,
                response_n: 1,
            }),
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
    }

    #[tokio::test]
    async fn test_client_side_timeout() {
        let _ = tracing_subscriber::fmt::try_init();

        let bh = BackendHandle::new(SeqNum(u64::MAX));

        let config = make_config();
        let mut state_sync =
            StateSync::<SignatureType, SignatureCollectionType>::new::<TestSyncClientBackend>(
                vec![bh.db_name.clone()],
                "genesis_path".to_string(),
                vec![NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap())],
                config.clone(),
                bh.uds_path(),
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
                    if messages.len() == config.max_parallel_requests {
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

        let e = timeout(config.request_timeout * 2, state_sync.next())
            .await
            .expect("no event timeout, should timeout request and send next")
            .expect("unexpected stream end");
        assert!(matches!(
            e,
            MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                _,
                StateSyncNetworkMessage::Request(_)
            ))
        ));
        let e = timeout(config.request_timeout / 2, state_sync.next())
            .await
            .expect("no event timeout, should timeout request and send next")
            .expect("unexpected stream end");
        assert!(matches!(
            e,
            MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                _,
                StateSyncNetworkMessage::Request(_)
            ))
        ));
    }

    #[tokio::test]
    async fn test_server_side_timeout() {
        let _ = tracing_subscriber::fmt::try_init();

        let bh = BackendHandle::new(SeqNum(u64::MAX));

        let node = NodeId::new(NopPubKey::from_bytes(&[1; 32]).unwrap());
        let config = make_config();
        let mut state_sync =
            StateSync::<SignatureType, SignatureCollectionType>::new::<TestSyncClientBackend>(
                vec![bh.db_name.clone()],
                "genesis_path".to_string(),
                vec![NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap())],
                config.clone(),
                bh.uds_path(),
            );

        let commands = vec![StateSyncCommand::StartExecution];
        state_sync.exec(commands);

        assert!(matches!(state_sync.mode, StateSyncMode::Live(_)));

        const DATA_SIZE: usize = 1000;
        {
            let mut backend = bh.backend.lock().unwrap();

            for i in 0..100 {
                let mut data = [i as u8; DATA_SIZE];
                let n = thread_rng().gen::<u64>();
                data[0..8].copy_from_slice(n.to_le_bytes().as_ref());
                backend.handle_upsert(0, StateSyncUpsertType::Account, &data);
            }

            backend.run_server();
        }

        let session_id = StateSyncSessionId(thread_rng().gen::<u64>());
        let cmds = vec![StateSyncCommand::Message((
            node,
            StateSyncNetworkMessage::Request(StateSyncRequest {
                version: SELF_STATESYNC_VERSION,
                session_id,
                from: 0,
                until: 1,
                old_target: 0,
                target: 1,
                prefix: 0,
                prefix_bytes: 1,
            }),
        ))];

        state_sync.exec(cmds);

        let mut responses_sent: usize = 0;
        loop {
            let e = timeout(Duration::from_secs(1), state_sync.next())
                .await
                .expect("timeout")
                .expect("stream closed unexpectedly");
            match e {
                MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                    n,
                    StateSyncNetworkMessage::Response(r),
                )) => {
                    assert_eq!(n, node);
                    match r.body {
                        StateSyncResponseBody::Ok(r) => {
                            assert_eq!(r.response_index, responses_sent as u64);
                            assert_eq!(
                                r.response.len(),
                                config.max_response_size.div_ceil(DATA_SIZE)
                            );
                            assert_eq!(r.response_n, 0);
                        }
                        StateSyncResponseBody::Err(_) => {
                            assert!(false, "unexpected error response");
                        }
                    }
                }
                _ => {
                    assert!(
                        false,
                        "unexpected event {:?}, should produce response message",
                        e
                    );
                }
            }
            responses_sent += 1;
            if responses_sent == config.max_outstanding_responses {
                break;
            }
        }

        assert!(
            stream_idle(&mut state_sync),
            "shouldn't send more messages without completions"
        );

        let cmds = vec![StateSyncCommand::Message((
            node,
            StateSyncNetworkMessage::Completion,
        ))];

        state_sync.exec(cmds);

        let e = timeout(Duration::from_secs(1), state_sync.next())
            .await
            .expect("timeout, should send next message after completion")
            .expect("stream closed unexpectedly");
        match e {
            MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                n,
                StateSyncNetworkMessage::Response(r),
            )) => {
                assert_eq!(n, node);
                match r.body {
                    StateSyncResponseBody::Ok(r) => {
                        assert_eq!(r.response_index, responses_sent as u64);
                        assert_eq!(
                            r.response.len(),
                            config.max_response_size.div_ceil(DATA_SIZE)
                        );
                        assert_eq!(r.response_n, 0);
                    }
                    StateSyncResponseBody::Err(_) => {
                        assert!(false, "unexpected error response");
                    }
                };
                assert_eq!(r.session_id, session_id);
            }
            _ => {
                assert!(
                    false,
                    "unexpected event {:?}, should produce response message",
                    e
                );
            }
        }

        assert!(
            stream_idle(&mut state_sync),
            "shouldn't send more messages without completions"
        );

        // Wait for the current request to timeout
        tokio::time::sleep(config.incoming_request_timeout * 3 / 2).await;
        println!("waited for request to timeout send new request");

        let session_id = StateSyncSessionId(thread_rng().gen::<u64>());
        let cmds = vec![StateSyncCommand::Message((
            node,
            StateSyncNetworkMessage::Request(StateSyncRequest {
                version: SELF_STATESYNC_VERSION,
                session_id,
                from: 0,
                until: 1,
                old_target: 0,
                target: 1,
                prefix: 0,
                prefix_bytes: 1,
            }),
        ))];
        state_sync.exec(cmds);

        // Should restart request from beginning
        responses_sent = 0;
        let e = timeout(Duration::from_secs(1), state_sync.next())
            .await
            .expect("timeout")
            .expect("stream closed unexpectedly");
        match e {
            MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                n,
                StateSyncNetworkMessage::Response(r),
            )) => {
                assert_eq!(n, node);
                match r.body {
                    StateSyncResponseBody::Ok(r) => {
                        assert_eq!(r.response_index, responses_sent as u64);
                        assert_eq!(
                            r.response.len(),
                            config.max_response_size.div_ceil(DATA_SIZE)
                        );
                        assert_eq!(r.response_n, 0);
                    }
                    StateSyncResponseBody::Err(_) => {
                        assert!(false, "unexpected error response");
                    }
                }
                assert_eq!(r.session_id, session_id);
            }
            _ => {
                assert!(
                    false,
                    "unexpected event {:?}, should produce response message",
                    e
                );
            }
        }

        bh.backend.lock().unwrap().shutdown().await;
    }

    #[tokio::test]
    // Restart client request if server responds with an error
    async fn test_client_error_restart() {
        let _ = tracing_subscriber::fmt::try_init();

        let bh = BackendHandle::new(SeqNum(u64::MAX));

        let config = make_config();
        let mut state_sync =
            StateSync::<SignatureType, SignatureCollectionType>::new::<TestSyncClientBackend>(
                vec![bh.db_name.clone()],
                "genesis_path".to_string(),
                vec![NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap())],
                config.clone(),
                bh.uds_path(),
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
                    if messages.len() == config.max_parallel_requests {
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

        let StateSyncNetworkMessage::Request(req) = &messages[0].1 else {
            panic!("Expected StateSyncNetworkMessage::Request");
        };
        let session_id = req.session_id;
        let cmds = vec![StateSyncCommand::Message((
            messages[0].0,
            StateSyncNetworkMessage::Response(StateSyncResponse {
                version: SELF_STATESYNC_VERSION,
                session_id,
                body: StateSyncResponseBody::Err(StateSyncResponseErr::InsufficientResources),
            }),
        ))];
        state_sync.exec(cmds);

        let e = timeout(config.request_timeout * 2, state_sync.next())
            .await
            .expect("no event timeout, should retry errored request")
            .expect("unexpected stream end");
        match e {
            MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                _,
                StateSyncNetworkMessage::Request(r),
            )) => {
                assert!(r.session_id != session_id, "should create new request");
            }
            _ => {
                assert!(
                    false,
                    "unexpected event {:?}, should retry errored request",
                    e
                );
            }
        }
    }

    #[tokio::test]
    async fn test_invalid_version() {
        let _ = tracing_subscriber::fmt::try_init();

        let bh = BackendHandle::new(SeqNum(u64::MAX));

        let node = NodeId::new(NopPubKey::from_bytes(&[1; 32]).unwrap());
        let config = make_config();
        let mut state_sync =
            StateSync::<SignatureType, SignatureCollectionType>::new::<TestSyncClientBackend>(
                vec![bh.db_name.clone()],
                "genesis_path".to_string(),
                vec![NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap())],
                config.clone(),
                bh.uds_path(),
            );

        let commands = vec![StateSyncCommand::StartExecution];
        state_sync.exec(commands);

        assert!(matches!(state_sync.mode, StateSyncMode::Live(_)));

        const DATA_SIZE: usize = 1000;
        {
            let mut backend = bh.backend.lock().unwrap();

            for i in 0..100 {
                let mut data = [i as u8; DATA_SIZE];
                let n = thread_rng().gen::<u64>();
                data[0..8].copy_from_slice(n.to_le_bytes().as_ref());
                backend.handle_upsert(0, StateSyncUpsertType::Account, &data);
            }

            backend.run_server();
        }

        let session_id = StateSyncSessionId(thread_rng().gen::<u64>());
        let cmds = vec![StateSyncCommand::Message((
            node,
            StateSyncNetworkMessage::Request(StateSyncRequest {
                version: StateSyncVersion::from_u32(SELF_STATESYNC_VERSION.to_u32() + (1 << 16)),
                session_id,
                from: 0,
                until: 1,
                old_target: 0,
                target: 1,
                prefix: 0,
                prefix_bytes: 1,
            }),
        ))];

        state_sync.exec(cmds);

        let e = timeout(Duration::from_secs(1), state_sync.next())
            .await
            .expect("timeout")
            .expect("stream closed unexpectedly");

        match e {
            MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                n,
                StateSyncNetworkMessage::Response(r),
            )) => {
                assert_eq!(n, node);
                assert_eq!(r.session_id, session_id);
                match r.body {
                    StateSyncResponseBody::Err(StateSyncResponseErr::InvalidVersion) => {}
                    _ => {
                        assert!(false, "unexpected response");
                    }
                }
            }
            _ => {
                assert!(
                    false,
                    "unexpected event {:?}, should produce response message",
                    e
                );
            }
        }

        bh.backend.lock().unwrap().shutdown().await;
    }

    #[tokio::test]
    async fn test_max_sessions() {
        let _ = tracing_subscriber::fmt::try_init();

        let bh = BackendHandle::new(SeqNum(u64::MAX));

        let node = NodeId::new(NopPubKey::from_bytes(&[1; 32]).unwrap());
        let config = make_config();
        let mut state_sync =
            StateSync::<SignatureType, SignatureCollectionType>::new::<TestSyncClientBackend>(
                vec![bh.db_name.clone()],
                "genesis_path".to_string(),
                vec![NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap())],
                config.clone(),
                bh.uds_path(),
            );

        let commands = vec![StateSyncCommand::StartExecution];
        state_sync.exec(commands);

        assert!(matches!(state_sync.mode, StateSyncMode::Live(_)));

        const DATA_SIZE: usize = 1000;
        {
            let mut backend = bh.backend.lock().unwrap();

            for i in 0..100 {
                let mut data = [i as u8; DATA_SIZE];
                let n = thread_rng().gen::<u64>();
                data[0..8].copy_from_slice(n.to_le_bytes().as_ref());
                backend.handle_upsert(0, StateSyncUpsertType::Account, &data);
            }

            backend.run_server();
        }

        let session_id = StateSyncSessionId(thread_rng().gen::<u64>());
        let cmds = vec![StateSyncCommand::Message((
            node,
            StateSyncNetworkMessage::Request(StateSyncRequest {
                version: SELF_STATESYNC_VERSION,
                session_id,
                from: 0,
                until: 1,
                old_target: 0,
                target: 1,
                prefix: 0,
                prefix_bytes: 1,
            }),
        ))];

        state_sync.exec(cmds);

        let mut responses_sent: usize = 0;
        loop {
            let e = timeout(Duration::from_secs(1), state_sync.next())
                .await
                .expect("timeout")
                .expect("stream closed unexpectedly");
            match e {
                MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                    n,
                    StateSyncNetworkMessage::Response(r),
                )) => {
                    assert_eq!(n, node);
                    match r.body {
                        StateSyncResponseBody::Ok(r) => {
                            assert_eq!(r.response_index, responses_sent as u64);
                            assert_eq!(
                                r.response.len(),
                                config.max_response_size.div_ceil(DATA_SIZE)
                            );
                            assert_eq!(r.response_n, 0);
                        }
                        StateSyncResponseBody::Err(_) => {
                            assert!(false, "unexpected error response");
                        }
                    }
                    assert_eq!(r.session_id, session_id);
                }
                _ => {
                    assert!(
                        false,
                        "unexpected event {:?}, should produce response message",
                        e
                    );
                }
            }
            responses_sent += 1;
            if responses_sent == config.max_outstanding_responses {
                break;
            }
        }

        assert!(
            stream_idle(&mut state_sync),
            "shouldn't send more messages without completions"
        );

        let session_id = StateSyncSessionId(thread_rng().gen::<u64>());
        let cmds = vec![StateSyncCommand::Message((
            node,
            StateSyncNetworkMessage::Request(StateSyncRequest {
                version: SELF_STATESYNC_VERSION,
                session_id,
                from: 0,
                until: 1,
                old_target: 0,
                target: 1,
                prefix: 1,
                prefix_bytes: 1,
            }),
        ))];
        state_sync.exec(cmds);

        let e = timeout(Duration::from_secs(1), state_sync.next())
            .await
            .expect("timeout, should send response to new request")
            .expect("stream closed unexpectedly");
        match e {
            MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                n,
                StateSyncNetworkMessage::Response(r),
            )) => {
                assert_eq!(n, node);
                assert_eq!(r.session_id, session_id);
                match r.body {
                    StateSyncResponseBody::Err(StateSyncResponseErr::InsufficientResources) => {}
                    _ => {
                        assert!(false, "unexpected response");
                    }
                };
            }
            _ => {
                assert!(
                    false,
                    "unexpected event {:?}, should produce response message",
                    e
                );
            }
        }

        bh.backend.lock().unwrap().shutdown().await;
    }
}

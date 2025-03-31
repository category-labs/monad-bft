use std::{marker::PhantomData, time::Duration};

use monad_chain_config::{revision::MockChainRevision, MockChainConfig};
use monad_compress::nop::NopCompression;
use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block::{MockExecutionProtocol, PassthruBlockPolicy},
    block_validator::MockValidator,
    signature_collection::SignatureCollection,
    validator_data::ValidatorSetData,
};
use monad_control_panel::ipc::ControlPanelIpcReceiver;
use monad_crypto::certificate_signature::{
    CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{Command, MonadEvent, RouterCommand, StateRootHashCommand};
use monad_raptorcast::{RaptorCast, RaptorCastConfig};
use monad_state::{Forkpoint, MonadMessage, MonadState, MonadStateBuilder, VerifiedMonadMessage};
use monad_state_backend::InMemoryState;
use monad_types::{ExecutionProtocol, NodeId, Round, SeqNum};
use monad_updaters::{
    checkpoint::MockCheckpoint, config_loader::MockConfigLoader, ledger::MockLedger,
    local_router::LocalPeerRouter, loopback::LoopbackExecutor, parent::ParentExecutor,
    state_root_hash::MockStateRootHashNop, statesync::MockStateSyncExecutor, timer::TokioTimer,
    tokio_timestamp::TokioTimestamp, txpool::MockTxPoolExecutor, BoxUpdater, Updater,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
use tracing_subscriber::EnvFilter;

pub enum RouterConfig<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Local(
        /// Must be passed ahead-of-time because they can't be instantiated individually
        LocalPeerRouter<MonadMessage<ST, SCT, EPT>, VerifiedMonadMessage<ST, SCT, EPT>>,
    ),
    RaptorCast(RaptorCastConfig<ST>),
}

pub enum LedgerConfig {
    Mock,
}

pub enum StateRootHashConfig<SCT>
where
    SCT: SignatureCollection,
{
    Mock {
        genesis_validator_data: ValidatorSetData<SCT>,
        val_set_update_interval: SeqNum,
    },
}

pub struct ExecutorConfig<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub router_config: RouterConfig<ST, SCT, EPT>,
    pub ledger_config: LedgerConfig,
    pub state_root_hash_config: StateRootHashConfig<SCT>,
    pub nodeid: NodeId<SCT::NodeIdPubKey>,
}

pub fn make_monad_executor<ST, SCT>(
    index: usize,
    state_backend: InMemoryState,
    config: ExecutorConfig<ST, SCT, MockExecutionProtocol>,
) -> ParentExecutor<
    BoxUpdater<
        'static,
        RouterCommand<
            CertificateSignaturePubKey<ST>,
            VerifiedMonadMessage<ST, SCT, MockExecutionProtocol>,
        >,
        MonadEvent<ST, SCT, MockExecutionProtocol>,
    >,
    TokioTimer<MonadEvent<ST, SCT, MockExecutionProtocol>>,
    MockLedger<ST, SCT, MockExecutionProtocol>,
    MockCheckpoint<SCT>,
    BoxUpdater<'static, StateRootHashCommand<SCT>, MonadEvent<ST, SCT, MockExecutionProtocol>>,
    TokioTimestamp<ST, SCT, MockExecutionProtocol>,
    MockTxPoolExecutor<ST, SCT, MockExecutionProtocol, PassthruBlockPolicy, InMemoryState>,
    ControlPanelIpcReceiver<ST, SCT, MockExecutionProtocol>,
    LoopbackExecutor<MonadEvent<ST, SCT, MockExecutionProtocol>>,
    MockStateSyncExecutor<ST, SCT, MockExecutionProtocol>,
    MockConfigLoader<ST, SCT, MockExecutionProtocol>,
>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Unpin,
    <ST as CertificateSignature>::KeyPairType: Unpin,
    <SCT as SignatureCollection>::SignatureType: Unpin,
{
    let (_, reload_handle) = tracing_subscriber::reload::Layer::new(EnvFilter::from_default_env());
    ParentExecutor {
        router: match config.router_config {
            RouterConfig::Local(router) => Updater::boxed(router),
            RouterConfig::RaptorCast(config) => Updater::boxed(RaptorCast::<
                ST,
                MonadMessage<ST, SCT, MockExecutionProtocol>,
                VerifiedMonadMessage<ST, SCT, MockExecutionProtocol>,
                MonadEvent<ST, SCT, MockExecutionProtocol>,
            >::new(config)),
        },
        timer: TokioTimer::default(),
        ledger: match config.ledger_config {
            LedgerConfig::Mock => MockLedger::new(state_backend.clone()),
        },
        checkpoint: MockCheckpoint::default(),
        state_root_hash: match config.state_root_hash_config {
            StateRootHashConfig::Mock {
                genesis_validator_data,
                val_set_update_interval,
            } => Updater::boxed(MockStateRootHashNop::new(
                genesis_validator_data,
                val_set_update_interval,
            )),
        },
        timestamp: TokioTimestamp::new(Duration::from_millis(5), 100, 10001),
        txpool: MockTxPoolExecutor::default(),
        control_panel: ControlPanelIpcReceiver::new(
            format!("./monad_controlpanel_{}.sock", index).into(),
            reload_handle,
            1000,
        )
        .expect("uds bind failed"),
        loopback: LoopbackExecutor::default(),
        state_sync: MockStateSyncExecutor::new(
            state_backend,
            // TODO do we test statesync in testground?
            Vec::new(),
        ),
        config_loader: MockConfigLoader::default(),
    }
}

type MonadStateType<ST, SCT> = MonadState<
    ST,
    SCT,
    MockExecutionProtocol,
    PassthruBlockPolicy,
    InMemoryState,
    ValidatorSetFactory<CertificateSignaturePubKey<ST>>,
    SimpleRoundRobin<CertificateSignaturePubKey<ST>>,
    MockValidator,
    MockChainConfig,
    MockChainRevision,
    NopCompression,
>;

pub struct StateConfig<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub key: ST::KeyPairType,

    pub cert_key: <SCT::SignatureType as CertificateSignature>::KeyPairType,

    pub val_set_update_interval: SeqNum,
    pub epoch_start_delay: Round,

    pub validators: ValidatorSetData<SCT>,
    pub consensus_config: ConsensusConfig<MockChainConfig, MockChainRevision>,
}

pub fn make_monad_state<ST, SCT>(
    state_backend: InMemoryState,
    config: StateConfig<ST, SCT>,
) -> (
    MonadStateType<ST, SCT>,
    Vec<
        Command<
            MonadEvent<ST, SCT, MockExecutionProtocol>,
            VerifiedMonadMessage<ST, SCT, MockExecutionProtocol>,
            ST,
            SCT,
            MockExecutionProtocol,
            PassthruBlockPolicy,
            InMemoryState,
        >,
    >,
)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    MonadStateBuilder {
        validator_set_factory: ValidatorSetFactory::default(),
        leader_election: SimpleRoundRobin::default(),
        block_validator: MockValidator {},
        block_policy: PassthruBlockPolicy {},
        state_backend,
        key: config.key,
        certkey: config.cert_key,
        val_set_update_interval: config.val_set_update_interval,
        epoch_start_delay: config.epoch_start_delay,
        beneficiary: Default::default(),
        forkpoint: Forkpoint::genesis(config.validators),
        block_sync_override_peers: Default::default(),
        consensus_config: config.consensus_config,
        compression_algo: NopCompression {},

        _phantom: PhantomData,
    }
    .build()
}

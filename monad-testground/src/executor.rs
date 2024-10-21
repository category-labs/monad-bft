use std::{marker::PhantomData, time::Duration};

use monad_async_state_verify::PeerAsyncStateVerify;
use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block::PassthruBlockPolicy, block_validator::MockValidator, payload::NopStateRoot,
    signature_collection::SignatureCollection, state_root_hash::StateRootHash, txpool::MockTxPool,
    validator_data::ValidatorSetData,
};
use monad_control_panel::ipc::ControlPanelIpcReceiver;
use monad_crypto::certificate_signature::{
    CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_discovery::NopDiscovery;
use monad_eth_types::EthAddress;
use monad_executor_glue::{Command, MonadEvent, RouterCommand, StateRootHashCommand};
use monad_gossip::{
    gossipsub::UnsafeGossipsubConfig,
    mock::MockGossipConfig,
    seeder::{Raptor, SeederConfig},
    Gossip,
};
use monad_ipc::IpcReceiver;
use monad_quic::{SafeQuinnConfig, Service, ServiceConfig};
use monad_raptorcast::{RaptorCast, RaptorCastConfig};
use monad_state::{
    Forkpoint, MonadMessage, MonadState, MonadStateBuilder, MonadVersion, VerifiedMonadMessage,
};
use monad_state_backend::InMemoryState;
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::{
    checkpoint::MockCheckpoint, ledger::MockLedger, local_router::LocalPeerRouter,
    loopback::LoopbackExecutor, parent::ParentExecutor, state_root_hash::MockStateRootHashNop,
    statesync::MockStateSyncExecutor, timer::TokioTimer, tokio_timestamp::TokioTimestamp,
    BoxUpdater, Updater,
};
use monad_validator::{
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory},
};
use rand::distributions::{Alphanumeric, DistString};
use tracing_subscriber::EnvFilter;

pub enum MonadP2PGossipConfig<ST: CertificateSignatureRecoverable> {
    Simple(MockGossipConfig<CertificateSignaturePubKey<ST>>),
    Gossipsub(UnsafeGossipsubConfig<CertificateSignaturePubKey<ST>>),
    Raptor(SeederConfig<'static, Raptor<'static, ST>>),
}

pub enum RouterConfig<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    Local(
        /// Must be passed ahead-of-time because they can't be instantiated individually
        LocalPeerRouter<MonadMessage<ST, SCT>, VerifiedMonadMessage<ST, SCT>>,
    ),
    MonadP2P {
        config: ServiceConfig<SafeQuinnConfig<ST>>,
        gossip_config: MonadP2PGossipConfig<ST>,
    },
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

pub struct ExecutorConfig<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub router_config: RouterConfig<ST, SCT>,
    pub ledger_config: LedgerConfig,
    pub state_root_hash_config: StateRootHashConfig<SCT>,
    pub nodeid: NodeId<SCT::NodeIdPubKey>,
}

pub fn make_monad_executor<ST, SCT>(
    state_backend: InMemoryState,
    config: ExecutorConfig<ST, SCT>,
) -> ParentExecutor<
    BoxUpdater<
        'static,
        RouterCommand<CertificateSignaturePubKey<ST>, VerifiedMonadMessage<ST, SCT>>,
        MonadEvent<ST, SCT>,
    >,
    TokioTimer<MonadEvent<ST, SCT>>,
    MockLedger<ST, SCT>,
    MockCheckpoint<SCT>,
    BoxUpdater<'static, StateRootHashCommand<SCT>, MonadEvent<ST, SCT>>,
    IpcReceiver<ST, SCT>,
    ControlPanelIpcReceiver<ST, SCT>,
    LoopbackExecutor<MonadEvent<ST, SCT>>,
    TokioTimestamp<ST, SCT>,
    MockStateSyncExecutor<ST, SCT>,
>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Unpin,
    <ST as CertificateSignature>::KeyPairType: Unpin,
    <SCT as SignatureCollection>::SignatureType: Unpin,
{
    let (_, reload_handle) = tracing_subscriber::reload::Layer::new(EnvFilter::from_default_env());
    let instance_id = Alphanumeric.sample_string(&mut rand::thread_rng(), 8);
    ParentExecutor {
        router: match config.router_config {
            RouterConfig::Local(router) => Updater::boxed(router),
            RouterConfig::MonadP2P {
                config,
                gossip_config,
            } => Updater::boxed(Service::<_, _, MonadMessage<ST, SCT>, _>::new(
                config,
                match gossip_config {
                    MonadP2PGossipConfig::Simple(mock_config) => Gossip::boxed(mock_config.build()),
                    MonadP2PGossipConfig::Gossipsub(gossipsub_config) => {
                        Gossip::boxed(gossipsub_config.build())
                    }
                    MonadP2PGossipConfig::Raptor(raptor_config) => {
                        Gossip::boxed(raptor_config.build())
                    }
                },
            )),
            RouterConfig::RaptorCast(config) => {
                Updater::boxed(RaptorCast::<
                    ST,
                    MonadMessage<ST, SCT>,
                    VerifiedMonadMessage<ST, SCT>,
                    _,
                >::new(config, NopDiscovery(PhantomData)))
            }
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
        ipc: IpcReceiver::new(
            format!("./monad_mempool_{}.sock", instance_id).into(),
            500, // tx_batch_size
            6,   // max_queued_batches
            3,   // queued_batches_watermark
        )
        .expect("uds bind failed"),
        control_panel: ControlPanelIpcReceiver::new(
            format!("./monad_controlpanel_{}.sock", instance_id).into(),
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
    }
}

type MonadStateType<ST, SCT> = MonadState<
    ST,
    SCT,
    PassthruBlockPolicy,
    InMemoryState,
    ValidatorSetFactory<CertificateSignaturePubKey<ST>>,
    SimpleRoundRobin<CertificateSignaturePubKey<ST>>,
    MockTxPool,
    MockValidator,
    NopStateRoot,
    PeerAsyncStateVerify<SCT, <ValidatorSetFactory<CertificateSignaturePubKey<ST>> as ValidatorSetTypeFactory>::ValidatorSetType>>;

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
    pub consensus_config: ConsensusConfig,
}

pub fn make_monad_state<ST, SCT>(
    state_backend: InMemoryState,
    config: StateConfig<ST, SCT>,
) -> (
    MonadStateType<ST, SCT>,
    Vec<Command<MonadEvent<ST, SCT>, VerifiedMonadMessage<ST, SCT>, SCT>>,
)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    MonadStateBuilder {
        version: MonadVersion::new("TESTGROUND"),
        validator_set_factory: ValidatorSetFactory::default(),
        leader_election: SimpleRoundRobin::default(),
        transaction_pool: MockTxPool::default(),
        block_validator: MockValidator {},
        block_policy: PassthruBlockPolicy {},
        state_backend,
        state_root_validator: NopStateRoot::default(),
        async_state_verify: PeerAsyncStateVerify::default(),
        key: config.key,
        certkey: config.cert_key,
        val_set_update_interval: config.val_set_update_interval,
        epoch_start_delay: config.epoch_start_delay,
        beneficiary: EthAddress::default(),
        forkpoint: Forkpoint::genesis(config.validators, StateRootHash::default()),
        consensus_config: config.consensus_config,
    }
    .build()
}

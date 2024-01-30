use monad_consensus_state::{command::Checkpoint, ConsensusConfig};
use monad_consensus_types::{
    block::Block, block_validator::MockValidator, payload::NopStateRoot,
    signature_collection::SignatureCollection, validator_data::ValidatorData,
};
use monad_crypto::certificate_signature::{
    CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_eth_txpool::EthTxPool;
use monad_eth_types::EthAddress;
use monad_executor::{BoxExecutor, Executor};
use monad_executor_glue::{
    Command, ExecutionLedgerCommand, MonadEvent, RouterCommand, StateRootHashCommand,
};
use monad_gossip::{gossipsub::UnsafeGossipsubConfig, mock::MockGossipConfig, Gossip};
use monad_ipc::{generate_uds_path, IpcReceiver};
use monad_ledger::MonadFileLedger;
use monad_mock_swarm::mock::MockExecutionLedger;
use monad_quic::{SafeQuinnConfig, Service, ServiceConfig};
use monad_state::{MonadMessage, MonadState, MonadStateBuilder, VerifiedMonadMessage};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::{
    checkpoint::MockCheckpoint, ledger::MockLedger, local_router::LocalPeerRouter,
    loopback::LoopbackExecutor, parent::ParentExecutor, state_root_hash::MockStateRootHashNop,
    timer::TokioTimer, BoxUpdater, Updater,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};

pub enum MonadP2PGossipConfig<PT: PubKey> {
    Simple(MockGossipConfig<PT>),
    Gossipsub(UnsafeGossipsubConfig<PT>),
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
        gossip_config: MonadP2PGossipConfig<CertificateSignaturePubKey<ST>>,
    },
}

pub enum ExecutionLedgerConfig {
    Mock,
    File,
}

pub enum StateRootHashConfig<SCT>
where
    SCT: SignatureCollection,
{
    Mock {
        genesis_validator_data: ValidatorData<SCT>,
        val_set_update_interval: SeqNum,
    },
}

pub struct ExecutorConfig<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub router_config: RouterConfig<ST, SCT>,
    pub execution_ledger_config: ExecutionLedgerConfig,
    pub state_root_hash_config: StateRootHashConfig<SCT>,
    pub nodeid: NodeId<SCT::NodeIdPubKey>,
}

pub async fn make_monad_executor<ST, SCT>(
    config: ExecutorConfig<ST, SCT>,
) -> ParentExecutor<
    BoxUpdater<
        'static,
        RouterCommand<CertificateSignaturePubKey<ST>, VerifiedMonadMessage<ST, SCT>>,
        MonadEvent<ST, SCT>,
    >,
    TokioTimer<MonadEvent<ST, SCT>>,
    MockLedger<CertificateSignaturePubKey<ST>, Block<SCT>, MonadEvent<ST, SCT>>,
    BoxExecutor<'static, ExecutionLedgerCommand<SCT>>,
    MockCheckpoint<Checkpoint<SCT>>,
    BoxUpdater<'static, StateRootHashCommand<Block<SCT>>, MonadEvent<ST, SCT>>,
    IpcReceiver<ST, SCT>,
    LoopbackExecutor<MonadEvent<ST, SCT>>,
>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Unpin,
    <SCT as SignatureCollection>::SignatureType: Unpin,
{
    ParentExecutor {
        router: match config.router_config {
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
                },
            )),
            RouterConfig::Local(router) => Updater::boxed(router),
        },
        timer: TokioTimer::default(),
        ledger: MockLedger::default(),
        execution_ledger: match config.execution_ledger_config {
            ExecutionLedgerConfig::Mock => Executor::boxed(MockExecutionLedger::default()),
            ExecutionLedgerConfig::File => Executor::boxed(MonadFileLedger::new(
                format!("{:?}-ledger", config.nodeid).into(),
            )),
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
        ipc: IpcReceiver::new(generate_uds_path()).expect("uds bind failed"),
        loopback: LoopbackExecutor::default(),
    }
}

type MonadStateType<ST, SCT> = MonadState<
    ST,
    SCT,
    ValidatorSetFactory<CertificateSignaturePubKey<ST>>,
    SimpleRoundRobin<CertificateSignaturePubKey<ST>>,
    EthTxPool,
    MockValidator,
    NopStateRoot,
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

    pub validators: ValidatorData<SCT>,
    pub consensus_config: ConsensusConfig,
}

pub fn make_monad_state<ST, SCT>(
    config: StateConfig<ST, SCT>,
) -> (
    MonadStateType<ST, SCT>,
    Vec<
        Command<
            MonadEvent<ST, SCT>,
            VerifiedMonadMessage<ST, SCT>,
            Block<SCT>,
            Checkpoint<SCT>,
            SCT,
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
        transaction_pool: EthTxPool::default(),
        block_validator: MockValidator {},
        state_root_validator: NopStateRoot::default(),
        validators: config.validators,
        key: config.key,
        certkey: config.cert_key,
        val_set_update_interval: config.val_set_update_interval,
        epoch_start_delay: config.epoch_start_delay,
        beneficiary: EthAddress::default(),
        consensus_config: config.consensus_config,
    }
    .build()
}

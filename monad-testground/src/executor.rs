use monad_consensus_state::{command::Checkpoint, ConsensusConfig, ConsensusState};
use monad_consensus_types::{
    block::FullBlock,
    block_validator::MockValidator,
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    message_signature::MessageSignature,
    payload::NopStateRoot,
    signature_collection::SignatureCollection,
};
use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_eth_types::EthAddress;
use monad_executor::{BoxExecutor, Executor, State};
use monad_executor_glue::{
    Command, ExecutionLedgerCommand, MempoolCommand, MonadEvent, RouterCommand,
};
use monad_gossip::{gossipsub::UnsafeGossipsubConfig, mock::MockGossipConfig, Gossip};
use monad_mock_swarm::mock::{MockExecutionLedger, MockMempool};
use monad_quic::{SafeQuinnConfig, Service, ServiceConfig};
use monad_state::{MonadConfig, MonadMessage, MonadState, VerifiedMonadMessage};
use monad_types::Stake;
use monad_updaters::{
    checkpoint::MockCheckpoint, execution_ledger::MonadFileLedger, ledger::MockLedger,
    local_router::LocalPeerRouter, parent::ParentExecutor, timer::TokioTimer, BoxUpdater, Updater,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};

pub enum MonadP2PGossipConfig {
    Simple(MockGossipConfig),
    Gossipsub(UnsafeGossipsubConfig),
}

pub enum RouterConfig<MessageSignatureType, SignatureCollectionType>
where
    MessageSignatureType: MessageSignature,
    SignatureCollectionType: SignatureCollection,
{
    Local(
        /// Must be passed ahead-of-time because they can't be instantiated individually
        LocalPeerRouter<
            MonadMessage<MessageSignatureType, SignatureCollectionType>,
            VerifiedMonadMessage<MessageSignatureType, SignatureCollectionType>,
        >,
    ),
    MonadP2P {
        config: ServiceConfig<SafeQuinnConfig>,
        gossip_config: MonadP2PGossipConfig,
    },
}

pub enum ExecutionLedgerConfig {
    Mock,
    File,
}

pub enum MempoolConfig {
    Mock,
}

pub struct ExecutorConfig<MessageSignatureType, SignatureCollectionType>
where
    MessageSignatureType: MessageSignature,
    SignatureCollectionType: SignatureCollection,
{
    pub router_config: RouterConfig<MessageSignatureType, SignatureCollectionType>,
    pub mempool_config: MempoolConfig,
    pub execution_ledger_config: ExecutionLedgerConfig,
}

pub async fn make_monad_executor<MessageSignatureType, SignatureCollectionType>(
    config: ExecutorConfig<MessageSignatureType, SignatureCollectionType>,
) -> ParentExecutor<
    BoxUpdater<
        'static,
        RouterCommand<VerifiedMonadMessage<MessageSignatureType, SignatureCollectionType>>,
        MonadEvent<MessageSignatureType, SignatureCollectionType>,
    >,
    TokioTimer<MonadEvent<MessageSignatureType, SignatureCollectionType>>,
    BoxUpdater<
        'static,
        MempoolCommand<SignatureCollectionType>,
        MonadEvent<MessageSignatureType, SignatureCollectionType>,
    >,
    MockLedger<
        FullBlock<SignatureCollectionType>,
        MonadEvent<MessageSignatureType, SignatureCollectionType>,
    >,
    BoxExecutor<'static, ExecutionLedgerCommand<SignatureCollectionType>>,
    MockCheckpoint<Checkpoint<SignatureCollectionType>>,
>
where
    MessageSignatureType: MessageSignature + Unpin,
    SignatureCollectionType: SignatureCollection + Unpin,
    <SignatureCollectionType as SignatureCollection>::SignatureType: Unpin,
{
    ParentExecutor {
        router: match config.router_config {
            RouterConfig::MonadP2P {
                config,
                gossip_config,
            } => Updater::boxed(Service::<
                _,
                _,
                MonadMessage<MessageSignatureType, SignatureCollectionType>,
                _,
            >::new(
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
        mempool: match config.mempool_config {
            MempoolConfig::Mock => Updater::boxed(MockMempool::default()),
        },
        ledger: MockLedger::default(),
        execution_ledger: match config.execution_ledger_config {
            ExecutionLedgerConfig::Mock => Executor::boxed(MockExecutionLedger::default()),
            ExecutionLedgerConfig::File => Executor::boxed(MonadFileLedger::default()),
        },
        checkpoint: MockCheckpoint::default(),
    }
}

type MonadStateType<MessageSignatureType, SignatureCollectionType> = MonadState<
    ConsensusState<SignatureCollectionType, MockValidator, NopStateRoot>,
    MessageSignatureType,
    SignatureCollectionType,
    ValidatorSet,
    SimpleRoundRobin,
>;

pub struct StateConfig<SignatureCollectionType: SignatureCollection> {
    pub key: KeyPair,

    pub cert_key: <SignatureCollectionType::SignatureType as CertificateSignature>::KeyPairType,

    pub genesis_peers: Vec<(
        PubKey,
        <<SignatureCollectionType::SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
    )>,
    pub consensus_config: ConsensusConfig,
}

pub fn make_monad_state<MessageSignatureType, SignatureCollectionType>(
    config: StateConfig<SignatureCollectionType>,
) -> (
    MonadStateType<MessageSignatureType, SignatureCollectionType>,
    Vec<
        Command<
            MonadEvent<MessageSignatureType, SignatureCollectionType>,
            VerifiedMonadMessage<MessageSignatureType, SignatureCollectionType>,
            FullBlock<SignatureCollectionType>,
            Checkpoint<SignatureCollectionType>,
            SignatureCollectionType,
        >,
    >,
)
where
    MessageSignatureType: MessageSignature,
    SignatureCollectionType: SignatureCollection,
{
    MonadStateType::init(MonadConfig {
        transaction_validator: MockValidator {},
        validators: config
            .genesis_peers
            .into_iter()
            .map(|(pk, cert_pk)| (pk, Stake(1), cert_pk))
            .collect(),
        key: config.key,
        certkey: config.cert_key,
        beneficiary: EthAddress::default(),
        consensus_config: config.consensus_config,
    })
}

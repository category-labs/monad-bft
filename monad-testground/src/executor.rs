use monad_consensus_state::{command::Checkpoint, ConsensusConfig, ConsensusState};
use monad_consensus_types::{
    block::Block,
    block_validator::MockValidator,
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    message_signature::MessageSignature,
    payload::NopStateRoot,
    signature_collection::SignatureCollection,
    txpool::EthTxPool,
    validator_data::ValidatorData,
};
use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_eth_types::EthAddress;
use monad_executor::{BoxExecutor, Executor, State};
use monad_executor_glue::{
    Command, ExecutionLedgerCommand, MonadEvent, RouterCommand, StateRootHashCommand,
};
use monad_gossip::{gossipsub::UnsafeGossipsubConfig, mock::MockGossipConfig, Gossip};
use monad_mock_swarm::mock::MockExecutionLedger;
use monad_quic::{SafeQuinnConfig, Service, ServiceConfig};
use monad_state::{MonadConfig, MonadMessage, MonadState, VerifiedMonadMessage};
use monad_types::{Round, SeqNum, Stake};
use monad_updaters::{
    checkpoint::MockCheckpoint,
    execution_ledger::MonadFileLedger,
    ledger::MockLedger,
    local_router::LocalPeerRouter,
    parent::ParentExecutor,
    state_root_hash::{MockStateRootHashNop, MockableStateRootHash},
    timer::TokioTimer,
    BoxUpdater, Updater,
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

pub enum StateRootHashConfig<SignatureCollectionType>
where
    SignatureCollectionType: SignatureCollection,
{
    Mock {
        genesis_validator_data: ValidatorData<SignatureCollectionType>,
        val_set_update_interval: SeqNum,
    },
}

pub struct ExecutorConfig<MessageSignatureType, SignatureCollectionType>
where
    MessageSignatureType: MessageSignature,
    SignatureCollectionType: SignatureCollection,
{
    pub router_config: RouterConfig<MessageSignatureType, SignatureCollectionType>,
    pub execution_ledger_config: ExecutionLedgerConfig,
    pub state_root_hash_config: StateRootHashConfig<SignatureCollectionType>,
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
    MockLedger<
        Block<SignatureCollectionType>,
        MonadEvent<MessageSignatureType, SignatureCollectionType>,
    >,
    BoxExecutor<'static, ExecutionLedgerCommand<SignatureCollectionType>>,
    MockCheckpoint<Checkpoint<SignatureCollectionType>>,
    BoxUpdater<
        'static,
        StateRootHashCommand<Block<SignatureCollectionType>>,
        MonadEvent<MessageSignatureType, SignatureCollectionType>,
    >,
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
        ledger: MockLedger::default(),
        execution_ledger: match config.execution_ledger_config {
            ExecutionLedgerConfig::Mock => Executor::boxed(MockExecutionLedger::default()),
            ExecutionLedgerConfig::File => Executor::boxed(MonadFileLedger::default()),
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
    }
}

type MonadStateType<MessageSignatureType, SignatureCollectionType> = MonadState<
    ConsensusState<SignatureCollectionType, MockValidator, NopStateRoot>,
    MessageSignatureType,
    SignatureCollectionType,
    ValidatorSet,
    SimpleRoundRobin,
    EthTxPool,
>;

pub struct StateConfig<SignatureCollectionType: SignatureCollection> {
    pub key: KeyPair,

    pub cert_key: <SignatureCollectionType::SignatureType as CertificateSignature>::KeyPairType,

    pub val_set_update_interval: SeqNum,
    pub epoch_start_delay: Round,

    pub genesis_peers: Vec<(
        PubKey,
        Stake,
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
            Block<SignatureCollectionType>,
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
        validators: config.genesis_peers,
        key: config.key,
        certkey: config.cert_key,
        val_set_update_interval: config.val_set_update_interval,
        epoch_start_delay: config.epoch_start_delay,
        beneficiary: EthAddress::default(),
        consensus_config: config.consensus_config,
    })
}

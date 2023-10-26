use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    block::FullBlock,
    message_signature::MessageSignature,
    multi_sig::MultiSig,
    payload::StateRoot,
    signature_collection::SignatureCollection,
    transaction_validator::{MockValidator, TransactionValidator},
};
use monad_crypto::NopSignature;
use monad_executor::{timed_event::TimedEvent, State};
use monad_executor_glue::MonadEvent;
use monad_state::{MonadConfig, MonadMessage, MonadState, VerifiedMonadMessage};
use monad_types::{Deserializable, Serializable};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::{
    mock::{MockWALogger, MockWALoggerConfig},
    PersistenceLogger,
};

use crate::{
    mock::{
        MockMempool, MockMempoolConfig, MockableExecutor, NoSerRouterConfig, NoSerRouterScheduler,
        RouterScheduler,
    },
    transformer::{GenericTransformerPipeline, Pipeline},
};

pub trait SwarmRelation {
    type STATE: State<
        Event = MonadEvent<Self::ST, Self::SCT>,
        SignatureCollection = Self::SCT,
        Message = Self::StateMessage,
        OutboundMessage = Self::OutboundStateMessage,
        Block = FullBlock<Self::SCT>,
        Config = MonadConfig<Self::SCT, Self::TVT>,
    >;
    type ST: MessageSignature + Unpin;
    type SCT: SignatureCollection + Unpin;
    type RS: RouterScheduler<M = Self::Message, Serialized = Self::Message, Config = Self::RSCFG>;
    type P: Pipeline<Self::Message> + Clone;
    type LGR: PersistenceLogger<
        Config = Self::LGRCFG,
        Event = TimedEvent<MonadEvent<Self::ST, Self::SCT>>,
    >;
    type ME: MockableExecutor<
        Event = MonadEvent<Self::ST, Self::SCT>,
        SignatureCollection = Self::SCT,
        Config = Self::MPCFG,
    >;

    type TVT: TransactionValidator + Clone;
    type StateMessage: Deserializable<Self::Message>;
    type OutboundStateMessage: Serializable<Self::Message>;
    type Message: Clone + PartialEq + Eq + Send;
    type LGRCFG: Clone;
    type RSCFG;
    type MPCFG: Copy;
}
// default swarm relation impl
pub struct NoSerSwarm;

impl SwarmRelation for NoSerSwarm {
    type STATE = SwarmStateType<Self>;
    type ST = NopSignature;
    type SCT = MultiSig<Self::ST>;
    type RS = NoSerRouterScheduler<MonadMessage<Self::ST, Self::SCT>>;
    type P = GenericTransformerPipeline<MonadMessage<Self::ST, Self::SCT>>;
    type LGR = MockWALogger<TimedEvent<MonadEvent<Self::ST, Self::SCT>>>;
    type ME = MockMempool<Self::ST, Self::SCT>;
    type TVT = MockValidator;
    type LGRCFG = MockWALoggerConfig;
    type RSCFG = NoSerRouterConfig;
    type MPCFG = MockMempoolConfig;
    type StateMessage = MonadMessage<Self::ST, Self::SCT>;
    type OutboundStateMessage = VerifiedMonadMessage<Self::ST, Self::SCT>;
    type Message = MonadMessage<Self::ST, Self::SCT>;
}

pub type SwarmStateType<S> = MonadState<
    ConsensusState<<S as SwarmRelation>::SCT, <S as SwarmRelation>::TVT, StateRoot>,
    <S as SwarmRelation>::ST,
    <S as SwarmRelation>::SCT,
    ValidatorSet,
    SimpleRoundRobin,
    BlockSyncState,
>;

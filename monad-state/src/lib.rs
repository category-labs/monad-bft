use std::{fmt::Debug, time::Duration};

use message::MessageState;
use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    messages::{consensus_message::ConsensusMessage, message::ProposalMessage},
    pacemaker::PacemakerTimerExpire,
    validation::signing::{Unverified, Verified},
};
use monad_consensus_state::{
    command::{Checkpoint, ConsensusCommand, FetchedFullTxs, FetchedTxs},
    ConsensusProcess,
};
use monad_consensus_types::{
    block::Block, quorum_certificate::QuorumCertificate, signature::SignatureCollection,
    validation::Sha256Hash, voting::VoteInfo,
};
use monad_crypto::{
    secp256k1::{KeyPair, PubKey},
    Signature,
};
use monad_executor::{
    CheckpointCommand, Command, LedgerCommand, MempoolCommand, Message, PeerId, RouterCommand,
    RouterTarget, State, TimerCommand,
};
use monad_types::{Epoch, NodeId, Stake};
use monad_validator::{
    leader_election::LeaderElection,
    validator_set::{ValidatorData, ValidatorSetType},
};
use ref_cast::RefCast;

#[cfg(feature = "proto")]
pub mod convert;

mod message;

type HasherType = Sha256Hash;

pub struct MonadState<CT, ST, SCT, VT, LT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    message_state: MessageState<MonadMessage<ST, SCT>, VerifiedMonadMessage<ST, SCT>>,

    consensus: CT,
    epoch: Epoch,
    leader_election: LT,
    validator_set: VT,
    upcoming_validator_set: Option<VT>,
}

impl<CT, ST, SCT, VT, LT> MonadState<CT, ST, SCT, VT, LT>
where
    CT: ConsensusProcess<ST, SCT>,
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
    VT: ValidatorSetType,
{
    pub fn pubkey(&self) -> PubKey {
        self.consensus.get_pubkey()
    }

    pub fn blocktree(&self) -> &BlockTree<SCT> {
        self.consensus.blocktree()
    }

    fn advance_epoch(&mut self, val_set: Option<ValidatorData>) {
        self.epoch = self.epoch + Epoch(1);

        if let Some(vs) = self.upcoming_validator_set.take() {
            self.validator_set = vs;
        }

        // FIXME testnet_panic when that's implemented. TODO, decide
        // error handling behaviour for prod
        self.upcoming_validator_set = val_set.map(|v| {
            VT::new(v.0).expect("ValidatorData should not have duplicates or invalid entries")
        });
    }

    fn load_epoch(
        &mut self,
        epoch: Epoch,
        val_set: ValidatorData,
        upcoming_val_set: ValidatorData,
    ) {
        self.epoch = epoch;
        // FIXME testnet_panic when that's implemented. TODO, decide
        // error handling behaviour for prod
        self.validator_set = VT::new(val_set.0)
            .expect("ValidatorData should not have duplicates or invalid entries");
        self.upcoming_validator_set = Some(
            VT::new(upcoming_val_set.0)
                .expect("ValidatorData should not have duplicates or invalid entries"),
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonadEvent<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    ConsensusEvent(ConsensusEvent<ST, SCT>),
}

impl monad_types::Deserializable
    for MonadEvent<
        monad_crypto::NopSignature,
        monad_consensus_types::multi_sig::MultiSig<monad_crypto::NopSignature>,
    >
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        crate::convert::interface::deserialize_event(data)
    }
}

impl monad_types::Serializable
    for MonadEvent<
        monad_crypto::NopSignature,
        monad_consensus_types::multi_sig::MultiSig<monad_crypto::NopSignature>,
    >
{
    fn serialize(&self) -> Vec<u8> {
        crate::convert::interface::serialize_event(self)
    }
}

#[cfg(feature = "proto")]
impl monad_types::Deserializable
    for MonadEvent<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus_types::multi_sig::MultiSig<monad_crypto::secp256k1::SecpSignature>,
    >
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        crate::convert::interface::deserialize_event(data)
    }
}

#[cfg(feature = "proto")]
impl monad_types::Serializable
    for MonadEvent<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus_types::multi_sig::MultiSig<monad_crypto::secp256k1::SecpSignature>,
    >
{
    fn serialize(&self) -> Vec<u8> {
        crate::convert::interface::serialize_event(self)
    }
}

#[derive(Debug, Clone)]
pub struct VerifiedMonadMessage<ST, SCT>(Verified<ST, ConsensusMessage<ST, SCT>>);

#[derive(RefCast)]
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct MonadMessage<ST, SCT>(Unverified<ST, ConsensusMessage<ST, SCT>>);

#[cfg(feature = "proto")]
impl<S: Signature> monad_types::Serializable
    for VerifiedMonadMessage<S, monad_consensus_types::multi_sig::MultiSig<S>>
{
    fn serialize(&self) -> Vec<u8> {
        monad_consensus::convert::interface::serialize_verified_consensus_message(&self.0)
    }
}

#[cfg(feature = "proto")]
impl<S: Signature> monad_types::Deserializable
    for MonadMessage<S, monad_consensus_types::multi_sig::MultiSig<S>>
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError> {
        Ok(MonadMessage(
            monad_consensus::convert::interface::deserialize_unverified_consensus_message(message)?,
        ))
    }
}

impl<ST, SCT> From<VerifiedMonadMessage<ST, SCT>> for MonadMessage<ST, SCT> {
    fn from(value: VerifiedMonadMessage<ST, SCT>) -> Self {
        MonadMessage(value.0.into())
    }
}

impl<ST, SCT> AsRef<MonadMessage<ST, SCT>> for VerifiedMonadMessage<ST, SCT> {
    fn as_ref(&self) -> &MonadMessage<ST, SCT> {
        MonadMessage::ref_cast(self.0.as_ref())
    }
}

impl<ST, SCT> Message for MonadMessage<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    type Event = MonadEvent<ST, SCT>;
    type Id = ST;

    fn id(&self) -> Self::Id {
        *self.0.author_signature()
    }

    fn event(self, from: PeerId) -> Self::Event {
        // MUST assert that output is valid and came from the `from` PeerId
        // `from` must somehow be guaranteed to be staked at this point so that subsequent
        // malformed stuff (that gets added to event log) can be slashed? TODO

        Self::Event::ConsensusEvent(ConsensusEvent::Message {
            sender: from.0,
            unverified_message: self.0,
        })
    }
}

pub struct MonadConfig<SCT, TV> {
    pub transaction_validator: TV,
    pub validators: Vec<PubKey>,
    pub key: KeyPair,

    pub delta: Duration,
    pub genesis_block: Block<SCT>,
    pub genesis_vote_info: VoteInfo,
    pub genesis_signatures: SCT,
}

impl<CT, ST, SCT, VT, LT> State for MonadState<CT, ST, SCT, VT, LT>
where
    CT: ConsensusProcess<ST, SCT>,
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
    VT: ValidatorSetType,
    LT: LeaderElection,
{
    type Config = MonadConfig<SCT, CT::TransactionValidatorType>;
    type Event = MonadEvent<ST, SCT>;
    type Message = MonadMessage<ST, SCT>;
    type OutboundMessage = VerifiedMonadMessage<ST, SCT>;
    type Block = Block<SCT>;
    type Checkpoint = Checkpoint<SCT>;

    fn init(
        config: Self::Config,
    ) -> (
        Self,
        Vec<Command<Self::Message, Self::OutboundMessage, Self::Block, Self::Checkpoint>>,
    ) {
        // create my keys and validator structs
        // FIXME stake should be configurable
        let validator_list = config
            .validators
            .into_iter()
            .map(|pubkey| (NodeId(pubkey), Stake(1)))
            .collect::<Vec<_>>();

        // create the initial validator set
        let val_set = VT::new(validator_list.clone()).expect("initial validator set init failed");
        let election = LT::new();

        let genesis_qc = QuorumCertificate::genesis_qc::<HasherType>(
            config.genesis_vote_info,
            config.genesis_signatures,
        );

        let mut monad_state: MonadState<CT, ST, SCT, VT, LT> = Self {
            message_state: MessageState::new(
                10,
                validator_list
                    .into_iter()
                    .map(|(NodeId(pubkey), _)| PeerId(pubkey))
                    .collect(),
            ),
            validator_set: val_set,
            upcoming_validator_set: None,
            leader_election: election,
            epoch: Epoch(0),
            consensus: CT::new(
                config.transaction_validator,
                config.key.pubkey(),
                config.genesis_block,
                genesis_qc,
                config.delta,
                config.key,
            ),
        };

        let init_cmds = monad_state.update(MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(
            PacemakerTimerExpire,
        )));

        (monad_state, init_cmds)
    }

    fn update(
        &mut self,
        event: Self::Event,
    ) -> Vec<Command<Self::Message, Self::OutboundMessage, Self::Block, Self::Checkpoint>> {
        match event {
            MonadEvent::ConsensusEvent(consensus_event) => {
                let consensus_commands: Vec<ConsensusCommand<ST, SCT>> = match consensus_event {
                    ConsensusEvent::Timeout(pacemaker_expire) => self
                        .consensus
                        .handle_timeout_expiry(pacemaker_expire)
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                    ConsensusEvent::FetchedTxs(fetched) => {
                        assert_eq!(fetched.node_id, self.consensus.get_nodeid());

                        let mut cmds = vec![ConsensusCommand::FetchTxsReset];

                        // make sure we're still in same round
                        if fetched.round == self.consensus.get_current_round() {
                            let b = Block::new::<HasherType>(
                                fetched.node_id,
                                fetched.round,
                                &fetched.txns,
                                &fetched.high_qc,
                            );

                            let p = ProposalMessage {
                                block: b,
                                last_round_tc: fetched.last_round_tc,
                            };

                            cmds.push(ConsensusCommand::Publish {
                                target: RouterTarget::Broadcast,
                                message: ConsensusMessage::Proposal(p),
                            })
                        }

                        cmds
                    }
                    ConsensusEvent::FetchedFullTxs(fetched_txs) => {
                        let mut cmds = vec![ConsensusCommand::FetchFullTxsReset];

                        cmds.extend(
                            self.consensus
                                .handle_proposal_message_full::<HasherType, _, _>(
                                    fetched_txs.author,
                                    fetched_txs.p,
                                    fetched_txs.txns,
                                    &self.validator_set,
                                    &self.leader_election,
                                ),
                        );

                        cmds
                    }
                    ConsensusEvent::Message {
                        sender,
                        unverified_message,
                    } => {
                        let verified_message = match unverified_message
                            .verify::<HasherType>(self.validator_set.get_members(), &sender)
                        {
                            Ok(m) => m,
                            Err(e) => todo!("{e:?}"),
                        };
                        let (author, signature, verified_message) = verified_message.destructure();
                        match verified_message {
                            ConsensusMessage::Proposal(msg) => self
                                .consensus
                                .handle_proposal_message::<HasherType>(author, msg),
                            ConsensusMessage::Vote(msg) => {
                                self.consensus.handle_vote_message::<HasherType, _, _>(
                                    author,
                                    signature,
                                    msg,
                                    &self.validator_set,
                                    &self.leader_election,
                                )
                            }
                            ConsensusMessage::Timeout(msg) => {
                                self.consensus.handle_timeout_message::<_, _>(
                                    author,
                                    signature,
                                    msg,
                                    &self.validator_set,
                                    &self.leader_election,
                                )
                            }
                        }
                    }
                    ConsensusEvent::LoadEpoch(epoch, current_val_set, next_val_set) => {
                        self.load_epoch(epoch, current_val_set, next_val_set);
                        Vec::new()
                    }
                    ConsensusEvent::AdvanceEpoch(valset) => {
                        self.advance_epoch(valset);
                        Vec::new()
                    }
                };

                let mut cmds = Vec::new();
                for consensus_command in consensus_commands {
                    match consensus_command {
                        ConsensusCommand::Publish { target, message } => {
                            let message = VerifiedMonadMessage(
                                message.sign::<HasherType>(self.consensus.get_keypair()),
                            );
                            let publish_action = self.message_state.send(target, message);
                            let id = Self::Message::from(publish_action.message.clone()).id();
                            cmds.push(Command::RouterCommand(RouterCommand::Publish {
                                target: publish_action.target,
                                message: publish_action.message,
                            }))
                        }
                        ConsensusCommand::Schedule {
                            duration,
                            on_timeout,
                        } => cmds.push(Command::TimerCommand(TimerCommand::Schedule {
                            duration,
                            on_timeout: Self::Event::ConsensusEvent(ConsensusEvent::Timeout(
                                on_timeout,
                            )),
                        })),
                        ConsensusCommand::ScheduleReset => {
                            cmds.push(Command::TimerCommand(TimerCommand::ScheduleReset))
                        }
                        ConsensusCommand::FetchTxs(cb) => cmds.push(Command::MempoolCommand(
                            MempoolCommand::FetchTxs(Box::new(|txs| {
                                Self::Event::ConsensusEvent(ConsensusEvent::FetchedTxs(cb(txs)))
                            })),
                        )),
                        ConsensusCommand::FetchTxsReset => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::FetchReset))
                        }
                        ConsensusCommand::FetchFullTxs(cb) => cmds.push(Command::MempoolCommand(
                            MempoolCommand::FetchFullTxs(Box::new(|txs| {
                                Self::Event::ConsensusEvent(ConsensusEvent::FetchedFullTxs(cb(txs)))
                            })),
                        )),
                        ConsensusCommand::FetchFullTxsReset => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::FetchFullReset))
                        }
                        ConsensusCommand::RequestSync { blockid: _ } => {
                            // TODO: respond with the Proposal matching the blockID.
                            //       right now, all 'missed' proposals are actually in-flight
                            //       so we don't need to implement this yet for things to work
                        }
                        ConsensusCommand::LedgerCommit(block) => {
                            cmds.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(block)))
                        }
                        ConsensusCommand::CheckpointSave(checkpoint) => cmds.push(
                            Command::CheckpointCommand(CheckpointCommand::Save(checkpoint)),
                        ),
                    }
                }
                cmds
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum ConsensusEvent<ST, SCT> {
    Message {
        sender: PubKey,
        unverified_message: Unverified<ST, ConsensusMessage<ST, SCT>>,
    },
    Timeout(PacemakerTimerExpire),
    FetchedTxs(FetchedTxs<ST, SCT>),
    FetchedFullTxs(FetchedFullTxs<ST, SCT>),
    LoadEpoch(Epoch, ValidatorData, ValidatorData),
    AdvanceEpoch(Option<ValidatorData>),
}

impl<S: Debug, SC: Debug> Debug for ConsensusEvent<S, SC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusEvent::Message {
                sender,
                unverified_message,
            } => f
                .debug_struct("Message")
                .field("sender", &sender)
                .field("msg", &unverified_message)
                .finish(),
            ConsensusEvent::Timeout(p) => p.fmt(f),
            ConsensusEvent::FetchedTxs(p) => p.fmt(f),
            ConsensusEvent::FetchedFullTxs(p) => f
                .debug_struct("FetchedFullTxs")
                .field("author", &p.author)
                .field("proposal", &p.p)
                .field("txns", &p.txns)
                .finish(),
            ConsensusEvent::LoadEpoch(e, _, _) => e.fmt(f),
            ConsensusEvent::AdvanceEpoch(e) => e.fmt(f),
        }
    }
}

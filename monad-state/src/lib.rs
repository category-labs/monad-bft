use std::{fmt::Debug, time::Duration};

use message::MessageState;
use monad_block_sync::BlockSyncProcess;
use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncMessage, ProposalMessage},
    },
    pacemaker::PacemakerTimerExpire,
    validation::signing::{Unverified, Verified},
};
use monad_consensus_state::{
    command::{Checkpoint, ConsensusCommand, FetchedBlock, FetchedFullTxs, FetchedTxs},
    ConsensusProcess,
};
use monad_consensus_types::{
    block::Block,
    certificate_signature::CertificateSignatureRecoverable,
    message_signature::MessageSignature,
    payload::{ExecutionArtifacts, Payload},
    quorum_certificate::QuorumCertificate,
    signature_collection::{
        SignatureCollection, SignatureCollectionKeyPairType, SignatureCollectionPubKeyType,
    },
    validation::Sha256Hash,
    voting::{ValidatorMapping, VoteInfo},
};
use monad_crypto::secp256k1::{KeyPair, PubKey};
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

pub mod convert;

mod message;

type HasherType = Sha256Hash;

pub struct MonadState<CT, ST, SCT, VT, LT, BST>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    message_state: MessageState<MonadMessage<ST, SCT>, VerifiedMonadMessage<ST, SCT>>,

    consensus: CT,
    epoch: Epoch,
    leader_election: LT,
    validator_set: VT,
    validator_mapping: ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    upcoming_validator_set: Option<VT>,
    block_sync: BST,
}

impl<CT, ST, SCT, VT, LT, BST> MonadState<CT, ST, SCT, VT, LT, BST>
where
    CT: ConsensusProcess<ST, SCT>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
    BST: BlockSyncProcess<ST, SCT, VT>,
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
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    ConsensusEvent(ConsensusEvent<ST, SCT>),
}

impl monad_types::Deserializable<[u8]>
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

impl monad_types::Serializable<Vec<u8>>
    for MonadEvent<
        monad_crypto::NopSignature,
        monad_consensus_types::multi_sig::MultiSig<monad_crypto::NopSignature>,
    >
{
    fn serialize(&self) -> Vec<u8> {
        crate::convert::interface::serialize_event(self)
    }
}

impl monad_types::Deserializable<[u8]>
    for MonadEvent<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus_types::bls::BlsSignatureCollection,
    >
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        crate::convert::interface::deserialize_event(data)
    }
}

impl monad_types::Serializable<Vec<u8>>
    for MonadEvent<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus_types::bls::BlsSignatureCollection,
    >
{
    fn serialize(&self) -> Vec<u8> {
        crate::convert::interface::serialize_event(self)
    }
}

impl monad_types::Deserializable<[u8]>
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

impl monad_types::Serializable<Vec<u8>>
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
pub struct VerifiedMonadMessage<ST, SCT: SignatureCollection>(
    Verified<ST, ConsensusMessage<ST, SCT>>,
);

#[derive(RefCast)]
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonadMessage<ST, SCT: SignatureCollection>(Unverified<ST, ConsensusMessage<ST, SCT>>);

impl<MS: MessageSignature, SCT: SignatureCollection> monad_types::Serializable<Vec<u8>>
    for VerifiedMonadMessage<MS, SCT>
{
    fn serialize(&self) -> Vec<u8> {
        monad_consensus::convert::interface::serialize_verified_consensus_message(&self.0)
    }
}

impl<MS: MessageSignature, SCT: SignatureCollection>
    monad_types::Serializable<MonadMessage<MS, SCT>> for VerifiedMonadMessage<MS, SCT>
{
    fn serialize(&self) -> MonadMessage<MS, SCT> {
        MonadMessage(self.0.clone().into())
    }
}

impl<MS: MessageSignature, SCT: SignatureCollection> monad_types::Deserializable<[u8]>
    for MonadMessage<MS, SCT>
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError> {
        Ok(MonadMessage(
            monad_consensus::convert::interface::deserialize_unverified_consensus_message(message)?,
        ))
    }
}

impl<MS: MessageSignature, CS: CertificateSignatureRecoverable> monad_types::Deserializable<Vec<u8>>
    for MonadMessage<MS, monad_consensus_types::multi_sig::MultiSig<CS>>
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(message: &Vec<u8>) -> Result<Self, Self::ReadError> {
        Ok(MonadMessage(
            monad_consensus::convert::interface::deserialize_unverified_consensus_message(
                message.as_ref(),
            )?,
        ))
    }
}

impl<ST, SCT: SignatureCollection> From<VerifiedMonadMessage<ST, SCT>> for MonadMessage<ST, SCT> {
    fn from(value: VerifiedMonadMessage<ST, SCT>) -> Self {
        MonadMessage(value.0.into())
    }
}

impl<ST, SCT: SignatureCollection> AsRef<MonadMessage<ST, SCT>> for VerifiedMonadMessage<ST, SCT> {
    fn as_ref(&self) -> &MonadMessage<ST, SCT> {
        MonadMessage::ref_cast(self.0.as_ref())
    }
}

impl<ST, SCT> Message for MonadMessage<ST, SCT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
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

pub struct MonadConfig<SCT: SignatureCollection, TV> {
    pub transaction_validator: TV,
    pub validators: Vec<(PubKey, SignatureCollectionPubKeyType<SCT>)>,
    pub key: KeyPair,
    pub certkey: SignatureCollectionKeyPairType<SCT>,

    pub delta: Duration,
    pub state_root_delay: u64,
    pub genesis_block: Block<SCT>,
    pub genesis_vote_info: VoteInfo,
    pub genesis_signatures: SCT,
}

impl<CT, ST, SCT, VT, LT, BST> State for MonadState<CT, ST, SCT, VT, LT, BST>
where
    CT: ConsensusProcess<ST, SCT>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
    LT: LeaderElection,
    BST: BlockSyncProcess<ST, SCT, VT>,
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
        // FIXME stake should be configurable
        let staking_list = config
            .validators
            .iter()
            .map(|(pubkey, _)| (NodeId(*pubkey), Stake(1)))
            .collect::<Vec<_>>();

        let voting_identities = config
            .validators
            .into_iter()
            .map(|(pubkey, certpubkey)| (NodeId(pubkey), certpubkey))
            .collect::<Vec<_>>();

        // create the initial validator set
        let val_set = VT::new(staking_list.clone()).expect("initial validator set init failed");
        let val_mapping = ValidatorMapping::new(voting_identities);
        let election = LT::new();

        let genesis_qc = QuorumCertificate::genesis_qc::<HasherType>(
            config.genesis_vote_info,
            config.genesis_signatures,
        );

        let mut monad_state: MonadState<CT, ST, SCT, VT, LT, BST> = Self {
            message_state: MessageState::new(
                10,
                staking_list
                    .into_iter()
                    .map(|(NodeId(pubkey), _)| PeerId(pubkey))
                    .collect(),
            ),
            validator_set: val_set,
            validator_mapping: val_mapping,
            upcoming_validator_set: None,
            leader_election: election,
            epoch: Epoch(0),
            consensus: CT::new(
                config.transaction_validator,
                config.key.pubkey(),
                config.genesis_block,
                genesis_qc,
                config.delta,
                config.state_root_delay,
                config.key,
                config.certkey,
            ),
            block_sync: BST::new(),
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
                                &Payload {
                                    txns: fetched.txns,
                                    header: ExecutionArtifacts::zero(), // TODO this needs to be fetched
                                    seq_num: fetched.seq_num,
                                },
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

                        if let Some(txns) = fetched_txs.txns {
                            cmds.extend(
                                self.consensus
                                    .handle_proposal_message_full::<HasherType, _, _>(
                                        fetched_txs.author,
                                        fetched_txs.p,
                                        txns,
                                        &self.validator_set,
                                        &self.leader_election,
                                    ),
                            );
                        }

                        cmds
                    }
                    ConsensusEvent::FetchedBlock(fetched_b) => {
                        let mut cmds = vec![ConsensusCommand::LedgerFetchReset];
                        if let Some(b) = fetched_b.block {
                            let m = BlockSyncMessage { block: b };
                            cmds.push(ConsensusCommand::Publish {
                                target: RouterTarget::PointToPoint(PeerId(fetched_b.requester.0)),
                                message: ConsensusMessage::BlockSync(m),
                            })
                        }
                        cmds
                    }
                    ConsensusEvent::Message {
                        sender,
                        unverified_message,
                    } => {
                        let verified_message = match unverified_message.verify::<HasherType, _>(
                            &self.validator_set,
                            &self.validator_mapping,
                            &sender,
                        ) {
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
                                    msg,
                                    &self.validator_set,
                                    &self.validator_mapping,
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
                            ConsensusMessage::RequestBlockSync(msg) => self
                                .block_sync
                                .handle_request_block_sync_message(author, msg),
                            ConsensusMessage::BlockSync(msg) => {
                                self.consensus.handle_block_sync_message(msg)
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

                let mut prepare_router_message =
                    |target: RouterTarget, message: ConsensusMessage<ST, SCT>| {
                        let message = VerifiedMonadMessage(
                            message.sign::<HasherType>(self.consensus.get_keypair()),
                        );
                        let publish_action = self.message_state.send(target, message);
                        Command::RouterCommand(RouterCommand::Publish {
                            target: publish_action.target,
                            message: publish_action.message,
                        })
                    };

                let mut cmds = Vec::new();
                for consensus_command in consensus_commands {
                    match consensus_command {
                        ConsensusCommand::Publish { target, message } => {
                            cmds.push(prepare_router_message(target, message));
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
                        ConsensusCommand::FetchTxs(max_txns, cb) => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::FetchTxs(
                                max_txns,
                                Box::new(|txs| {
                                    Self::Event::ConsensusEvent(ConsensusEvent::FetchedTxs(cb(txs)))
                                }),
                            )))
                        }
                        ConsensusCommand::FetchTxsReset => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::FetchReset))
                        }
                        ConsensusCommand::FetchFullTxs(txs, cb) => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::FetchFullTxs(
                                txs,
                                Box::new(|full_txs| {
                                    Self::Event::ConsensusEvent(ConsensusEvent::FetchedFullTxs(cb(
                                        full_txs,
                                    )))
                                }),
                            )))
                        }
                        ConsensusCommand::FetchFullTxsReset => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::FetchFullReset))
                        }

                        ConsensusCommand::RequestSync { blockid } => {
                            let (target, message) = self
                                .block_sync
                                .request_block_sync(blockid, &self.validator_set);
                            cmds.push(prepare_router_message(target, message));
                        }
                        ConsensusCommand::LedgerCommit(block) => {
                            cmds.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(block)))
                        }
                        ConsensusCommand::LedgerFetch(b_id, cb) => {
                            cmds.push(Command::LedgerCommand(LedgerCommand::LedgerFetch(
                                b_id,
                                Box::new(|block: Option<Block<_>>| {
                                    Self::Event::ConsensusEvent(ConsensusEvent::FetchedBlock(cb(
                                        block,
                                    )))
                                }),
                            )))
                        }

                        ConsensusCommand::LedgerFetchReset => {
                            cmds.push(Command::LedgerCommand(LedgerCommand::LedgerFetchReset))
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
pub enum ConsensusEvent<ST, SCT: SignatureCollection> {
    Message {
        sender: PubKey,
        unverified_message: Unverified<ST, ConsensusMessage<ST, SCT>>,
    },
    Timeout(PacemakerTimerExpire),
    FetchedTxs(FetchedTxs<ST, SCT>),
    FetchedFullTxs(FetchedFullTxs<ST, SCT>),
    FetchedBlock(FetchedBlock<SCT>),
    LoadEpoch(Epoch, ValidatorData, ValidatorData),
    AdvanceEpoch(Option<ValidatorData>),
}

impl<S: Debug, SCT: Debug + SignatureCollection> Debug for ConsensusEvent<S, SCT> {
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
            ConsensusEvent::FetchedBlock(b) => f
                .debug_struct("FetchedBlock")
                .field("block", &b.block)
                .finish(),
            ConsensusEvent::LoadEpoch(e, _, _) => e.fmt(f),
            ConsensusEvent::AdvanceEpoch(e) => e.fmt(f),
        }
    }
}

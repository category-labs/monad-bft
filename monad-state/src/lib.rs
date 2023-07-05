use std::fmt::Debug;
use std::time::Duration;

use monad_blocktree::blocktree::BlockTree;
use monad_consensus::types::consensus_message::ConsensusMessage;
use monad_consensus::validation::signing::Unverified;
use monad_consensus::{
    pacemaker::{Pacemaker, PacemakerCommand, PacemakerTimerExpire},
    types::{
        block::{Block, TransactionList},
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
        quorum_certificate::{QuorumCertificate, Rank},
        signature::SignatureCollection,
        timeout::TimeoutCertificate,
        voting::VoteInfo,
    },
    validation::{
        hashing::{Hasher, Sha256Hash},
        safety::Safety,
        signing::Verified,
    },
    vote_state::VoteState,
};
use monad_crypto::{
    secp256k1::{KeyPair, PubKey},
    Signature,
};
use monad_executor::{
    Command, LedgerCommand, MempoolCommand, Message, PeerId, RouterCommand, RouterTarget, State,
    TimerCommand,
};
use monad_types::{BlockId, NodeId, Round, Stake};
use monad_validator::{
    leader_election::LeaderElection, validator_set::ValidatorSet,
    weighted_round_robin::WeightedRoundRobin,
};

use message::MessageState;
use ref_cast::RefCast;

#[cfg(feature = "proto")]
pub mod convert;

mod message;

type LeaderElectionType = WeightedRoundRobin;
type HasherType = Sha256Hash;

pub struct MonadState<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    message_state: MessageState<MonadMessage<ST, SCT>, VerifiedMonadMessage<ST, SCT>>,

    consensus_state: ConsensusState<ST, SCT>,
    validator_set: ValidatorSet<LeaderElectionType>,
}

impl<ST, SCT> MonadState<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    pub fn pubkey(&self) -> PubKey {
        self.consensus_state.nodeid.0
    }

    pub fn blocktree(&self) -> &BlockTree<SCT> {
        &self.consensus_state.pending_block_tree
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

#[cfg(feature = "proto")]
impl monad_types::Deserializable
    for MonadEvent<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus::signatures::multi_sig::MultiSig<monad_crypto::secp256k1::SecpSignature>,
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
        monad_consensus::signatures::multi_sig::MultiSig<monad_crypto::secp256k1::SecpSignature>,
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
    for VerifiedMonadMessage<S, monad_consensus::signatures::multi_sig::MultiSig<S>>
{
    fn serialize(&self) -> Vec<u8> {
        monad_consensus::convert::interface::serialize_verified_consensus_message(&self.0)
    }
}

#[cfg(feature = "proto")]
impl<S: Signature> monad_types::Deserializable
    for MonadMessage<S, monad_consensus::signatures::multi_sig::MultiSig<S>>
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

pub struct MonadConfig<SCT> {
    pub validators: Vec<PubKey>,
    pub key: KeyPair,

    pub delta: Duration,
    pub genesis_block: Block<SCT>,
    pub genesis_vote_info: VoteInfo,
    pub genesis_signatures: SCT,
}

impl<ST, SCT> State for MonadState<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    type Config = MonadConfig<SCT>;
    type Event = MonadEvent<ST, SCT>;
    type Message = MonadMessage<ST, SCT>;
    type OutboundMessage = VerifiedMonadMessage<ST, SCT>;
    type Block = Block<SCT>;

    fn init(
        config: Self::Config,
    ) -> (
        Self,
        Vec<Command<Self::Message, Self::OutboundMessage, Self::Block>>,
    ) {
        // create my keys and validator structs
        // FIXME stake should be configurable
        let validator_list = config
            .validators
            .into_iter()
            .map(|pubkey| (NodeId(pubkey), Stake(1)))
            .collect::<Vec<_>>();

        // create the initial validator set
        let val_set =
            ValidatorSet::new(validator_list.clone()).expect("initial validator set init failed");

        let genesis_qc = QuorumCertificate::genesis_qc::<HasherType>(
            config.genesis_vote_info,
            config.genesis_signatures,
        );

        let mut monad_state: MonadState<ST, SCT> = Self {
            message_state: MessageState::new(
                10,
                validator_list
                    .into_iter()
                    .map(|(NodeId(pubkey), _)| PeerId(pubkey))
                    .collect(),
            ),
            validator_set: val_set,
            consensus_state: ConsensusState::new(
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
    ) -> Vec<Command<Self::Message, Self::OutboundMessage, Self::Block>> {
        match event {
            MonadEvent::ConsensusEvent(consensus_event) => {
                let consensus_commands: Vec<ConsensusCommand<ST, SCT>> = match consensus_event {
                    ConsensusEvent::Timeout(pacemaker_expire) => self
                        .consensus_state
                        .pacemaker
                        .handle_event(
                            &mut self.consensus_state.safety,
                            &self.consensus_state.high_qc,
                            pacemaker_expire,
                        )
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                    ConsensusEvent::FetchedTxs(fetched) => {
                        assert_eq!(fetched.node_id, self.consensus_state.nodeid);

                        let mut cmds = vec![ConsensusCommand::FetchTxsReset];

                        // make sure we're still in same round
                        if fetched.round == self.consensus_state.pacemaker.get_current_round() {
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
                                .consensus_state
                                .handle_proposal_message::<HasherType, _>(
                                    author,
                                    msg,
                                    &mut self.validator_set,
                                ),
                            ConsensusMessage::Vote(msg) => self
                                .consensus_state
                                .handle_vote_message::<HasherType, LeaderElectionType>(
                                    author,
                                    signature,
                                    msg,
                                    &mut self.validator_set,
                                ),
                            ConsensusMessage::Timeout(msg) => {
                                self.consensus_state.handle_timeout_message::<_>(
                                    author,
                                    signature,
                                    msg,
                                    &mut self.validator_set,
                                )
                            }
                        }
                    }
                };
                let mut cmds = Vec::new();
                for consensus_command in consensus_commands {
                    match consensus_command {
                        ConsensusCommand::Publish { target, message } => {
                            let message = VerifiedMonadMessage(
                                message.sign::<HasherType>(&self.consensus_state.keypair),
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
                        ConsensusCommand::RequestSync { blockid: _ } => {
                            // TODO: respond with the Proposal matching the blockID.
                            //       right now, all 'missed' proposals are actually in-flight
                            //       so we don't need to implement this yet for things to work
                        }
                        ConsensusCommand::LedgerCommit(block) => {
                            cmds.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(block)))
                        }
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
}

impl<S: Debug, T: Debug> Debug for ConsensusEvent<S, T> {
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedTxs<ST, SCT> {
    // some of this stuff is probably not strictly necessary
    // they're included here just to be extra safe
    node_id: NodeId,
    round: Round,
    high_qc: QuorumCertificate<SCT>,
    last_round_tc: Option<TimeoutCertificate<ST>>,

    txns: TransactionList,
}

pub enum ConsensusCommand<ST, SCT: SignatureCollection> {
    Publish {
        target: RouterTarget,
        message: ConsensusMessage<ST, SCT>,
    },
    Schedule {
        duration: Duration,
        on_timeout: PacemakerTimerExpire,
    },
    ScheduleReset,
    FetchTxs(Box<dyn (FnOnce(Vec<u8>) -> FetchedTxs<ST, SCT>) + Send + Sync>),
    FetchTxsReset,
    LedgerCommit(Block<SCT>),
    RequestSync {
        blockid: BlockId,
    },
    // TODO add command for updating validator_set/round
    // - to handle this command, we need to call message_state.set_round()
}

struct ConsensusState<S, T> {
    pending_block_tree: BlockTree<T>,
    vote_state: VoteState<T>,
    high_qc: QuorumCertificate<T>,

    pacemaker: Pacemaker<S, T>,
    safety: Safety,

    nodeid: NodeId,

    // TODO deprecate
    keypair: KeyPair,
}

impl<S, T> ConsensusState<S, T>
where
    S: Signature,
    T: SignatureCollection,
{
    pub fn new(
        my_pubkey: PubKey,
        genesis_block: Block<T>,
        genesis_qc: QuorumCertificate<T>,
        delta: Duration,

        // TODO deprecate
        keypair: KeyPair,
    ) -> Self {
        ConsensusState {
            pending_block_tree: BlockTree::new(genesis_block),
            vote_state: VoteState::default(),
            high_qc: genesis_qc,
            pacemaker: Pacemaker::new(delta, Round(1), None),
            safety: Safety::default(),
            nodeid: NodeId(my_pubkey),

            keypair,
        }
    }

    fn handle_proposal_message<H: Hasher, V: LeaderElection>(
        &mut self,
        author: NodeId,
        p: ProposalMessage<S, T>,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<S, T>> {
        let mut cmds = Vec::new();

        let process_certificate_cmds = self.process_certificate_qc(&p.block.qc);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = p.last_round_tc.as_ref() {
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(last_round_tc)
                .map(Into::into)
                .into_iter();
            cmds.extend(advance_round_cmds);
        }

        let round = self.pacemaker.get_current_round();
        let leader = *validators.get_leader(round);

        self.pending_block_tree
            .add(p.block.clone())
            .expect("Failed to add block to blocktree");

        if !self.pending_block_tree.has_parent(&p.block.qc) {
            cmds.push(ConsensusCommand::RequestSync {
                blockid: p.block.get_parent_id(),
            });
        }

        if p.block.round != round || author != leader || p.block.author != leader {
            return cmds;
        }

        let vote_msg = self.safety.make_vote::<S, T, H>(&p.block, &p.last_round_tc);

        if let Some(v) = vote_msg {
            let next_leader = validators.get_leader(round + Round(1));
            let send_cmd = ConsensusCommand::Publish {
                target: RouterTarget::PointToPoint(PeerId(next_leader.0)),
                message: ConsensusMessage::Vote(v),
            };
            cmds.push(send_cmd);
        }

        cmds
    }

    fn handle_vote_message<H: Hasher, V: LeaderElection>(
        &mut self,
        author: NodeId,
        signature: T::SignatureType,
        v: VoteMessage,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<S, T>> {
        if v.vote_info.round < self.pacemaker.get_current_round() {
            return Default::default();
        }

        let qc: Option<QuorumCertificate<T>> = self
            .vote_state
            .process_vote::<V, H>(&author, &signature, &v, validators);

        let mut cmds = Vec::new();
        if let Some(qc) = qc {
            cmds.extend(self.process_certificate_qc(&qc));

            if self.nodeid == *validators.get_leader(self.pacemaker.get_current_round()) {
                cmds.extend(self.process_new_round_event(None));
            }
        }
        cmds
    }

    fn handle_timeout_message<V: LeaderElection>(
        &mut self,
        author: NodeId,
        signature: S,
        tm: TimeoutMessage<S, T>,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<S, T>> {
        let mut cmds = Vec::new();
        if tm.tminfo.round < self.pacemaker.get_current_round() {
            return cmds;
        }

        let process_certificate_cmds = self.process_certificate_qc(&tm.tminfo.high_qc);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = tm.last_round_tc.as_ref() {
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(last_round_tc)
                .map(Into::into)
                .into_iter();
            cmds.extend(advance_round_cmds);
        }

        let (tc, remote_timeout_cmds) = self.pacemaker.process_remote_timeout(
            validators,
            &mut self.safety,
            &self.high_qc,
            author,
            signature,
            tm,
        );
        cmds.extend(remote_timeout_cmds.into_iter().map(Into::into));
        if let Some(tc) = tc {
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(&tc)
                .into_iter()
                .map(Into::into);
            cmds.extend(advance_round_cmds);

            if self.nodeid == *validators.get_leader(self.pacemaker.get_current_round()) {
                cmds.extend(self.process_new_round_event(Some(tc)));
            }
        }

        cmds
    }

    // If the qc has a commit_state_hash, commit the parent block and prune the
    // block tree
    // Update our highest seen qc (high_qc) if the incoming qc is of higher rank
    #[must_use]
    fn process_qc(&mut self, qc: &QuorumCertificate<T>) -> Vec<ConsensusCommand<S, T>> {
        if Rank(qc.info) <= Rank(self.high_qc.info) {
            return Vec::new();
        }

        self.high_qc = qc.clone();
        let mut cmds = Vec::new();
        if qc.info.ledger_commit.commit_state_hash.is_some()
            && self
                .pending_block_tree
                .path_to_root(&qc.info.vote.parent_id)
        {
            let blocks_to_commit = self
                .pending_block_tree
                .prune(&qc.info.vote.parent_id)
                .unwrap_or_else(|_| panic!("\n{:?}", self.pending_block_tree));

            if !blocks_to_commit.is_empty() {
                cmds.extend(
                    blocks_to_commit
                        .into_iter()
                        .map(ConsensusCommand::<S, T>::LedgerCommit),
                );
            }
        }
        cmds
    }

    // TODO consider changing return type to Option<T>
    #[must_use]
    fn process_certificate_qc(&mut self, qc: &QuorumCertificate<T>) -> Vec<ConsensusCommand<S, T>> {
        let mut cmds = Vec::new();
        cmds.extend(self.process_qc(qc));

        cmds.extend(
            self.pacemaker
                .advance_round_qc(qc)
                .map(Into::into)
                .into_iter(),
        );
        cmds
    }

    // TODO consider changing return type to Option<T>
    #[must_use]
    fn process_new_round_event(
        &mut self,
        last_round_tc: Option<TimeoutCertificate<S>>,
    ) -> Vec<ConsensusCommand<S, T>> {
        self.vote_state
            .start_new_round(self.pacemaker.get_current_round());

        let node_id = self.nodeid;
        let round = self.pacemaker.get_current_round();
        let high_qc = self.high_qc.clone();

        vec![ConsensusCommand::FetchTxs(Box::new(move |txns| {
            FetchedTxs {
                node_id,
                round,
                high_qc,
                last_round_tc,
                txns: TransactionList(txns),
            }
        }))]
    }
}

impl<S: Signature, T: SignatureCollection> From<PacemakerCommand<S, T>> for ConsensusCommand<S, T> {
    fn from(cmd: PacemakerCommand<S, T>) -> Self {
        match cmd {
            PacemakerCommand::Broadcast(message) => ConsensusCommand::Publish {
                target: RouterTarget::Broadcast,
                message: ConsensusMessage::Timeout(message),
            },
            PacemakerCommand::Schedule {
                duration,
                on_timeout,
            } => ConsensusCommand::Schedule {
                duration,
                on_timeout,
            },
            PacemakerCommand::ScheduleReset => ConsensusCommand::ScheduleReset,
        }
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use monad_consensus::types::block::TransactionList;
    use std::time::Duration;

    use monad_consensus::pacemaker::PacemakerTimerExpire;
    use monad_consensus::signatures::multi_sig::MultiSig;
    use monad_consensus::types::ledger::LedgerCommitInfo;
    use monad_consensus::types::message::{TimeoutMessage, VoteMessage};
    use monad_consensus::types::quorum_certificate::{genesis_vote_info, QuorumCertificate};
    use monad_consensus::types::signature::SignatureCollection;
    use monad_consensus::types::timeout::TimeoutInfo;
    use monad_consensus::types::voting::VoteInfo;
    use monad_consensus::validation::hashing::Sha256Hash;
    use monad_consensus::validation::signing::Verified;
    use monad_crypto::secp256k1::KeyPair;
    use monad_crypto::{NopSignature, Signature};
    use monad_executor::{PeerId, RouterTarget};
    use monad_testutil::proposal::ProposalGen;
    use monad_testutil::signing::{create_keys, get_genesis_config};
    use monad_types::{BlockId, Hash, NodeId, Round, Stake};
    use monad_validator::{validator_set::ValidatorSet, weighted_round_robin::WeightedRoundRobin};

    use crate::{ConsensusCommand, ConsensusMessage, ConsensusState};

    fn setup<ST: Signature, SCT: SignatureCollection<SignatureType = ST>>(
        num_states: u32,
    ) -> (
        Vec<KeyPair>,
        ValidatorSet<WeightedRoundRobin>,
        Vec<ConsensusState<ST, SCT>>,
    ) {
        let keys = create_keys(num_states);
        let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();
        let (genesis_block, genesis_sigs) = get_genesis_config::<Sha256Hash, SCT>(keys.iter());

        let validator_list = pubkeys
            .into_iter()
            .map(|pubkey| (NodeId(pubkey), Stake(1)))
            .collect::<Vec<_>>();

        let val_set: ValidatorSet<WeightedRoundRobin> =
            ValidatorSet::new(validator_list).expect("initial validator set init failed");

        let genesis_qc = QuorumCertificate::genesis_qc::<Sha256Hash>(
            genesis_vote_info(genesis_block.get_id()),
            genesis_sigs,
        );

        let mut dupkeys = create_keys(num_states);
        let consensus_states = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let default_key = KeyPair::from_bytes([127; 32]).unwrap();
                ConsensusState::<ST, SCT>::new(
                    k.pubkey(),
                    genesis_block.clone(),
                    genesis_qc.clone(),
                    Duration::from_secs(1),
                    std::mem::replace(&mut dupkeys[i], default_key),
                )
            })
            .collect::<Vec<_>>();

        (keys, val_set, consensus_states)
    }

    // 2f+1 votes for a VoteInfo leads to a QC locking -- ie, high_qc is set to that QC.
    #[test]
    fn lock_qc_high() {
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);

        let state = &mut states[0];
        assert_eq!(state.high_qc.info.vote.round, Round(0));

        let expected_qc_high_round = Round(5);

        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: expected_qc_high_round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: expected_qc_high_round - Round(1),
        };
        let vm = VoteMessage {
            vote_info: vi,
            ledger_commit_info: LedgerCommitInfo::new::<Sha256Hash>(None, &vi),
        };

        let v1 = Verified::<_, VoteMessage>::new::<Sha256Hash>(vm, &keys[1]);
        let v2 = Verified::<_, VoteMessage>::new::<Sha256Hash>(vm, &keys[2]);
        let v3 = Verified::<_, VoteMessage>::new::<Sha256Hash>(vm, &keys[3]);

        state.handle_vote_message::<Sha256Hash, _>(
            *v1.author(),
            *v1.author_signature(),
            *v1,
            &mut valset,
        );
        state.handle_vote_message::<Sha256Hash, _>(
            *v2.author(),
            *v2.author_signature(),
            *v2,
            &mut valset,
        );

        // less than 2f+1, so expect not locked
        assert_eq!(state.high_qc.info.vote.round, Round(0));

        state.handle_vote_message::<Sha256Hash, _>(
            *v3.author(),
            *v3.author_signature(),
            *v3,
            &mut valset,
        );
        assert_eq!(state.high_qc.info.vote.round, expected_qc_high_round);
    }

    // When a node locally timesout on a round, it no longer produces votes in that round
    #[test]
    fn timeout_stops_voting() {
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());
        let p1 = propgen.next_proposal(&keys, &mut valset, &Default::default());

        // local timeout for state in Round 1
        assert_eq!(state.pacemaker.get_current_round(), Round(1));
        let _ =
            state
                .pacemaker
                .handle_event(&mut state.safety, &state.high_qc, PacemakerTimerExpire);

        // check no vote commands result from receiving the proposal for round 1

        let (author, _, verified_message) = p1.destructure();
        let cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);
        let result = cmds.iter().find(|&c| {
            matches!(
                c,
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(_),
                }
            )
        });

        assert!(result.is_none());
    }

    #[test]
    fn enter_proposalmsg_round() {
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);

        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        let p1 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p1.destructure();
        let cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);
        let result = cmds.iter().find(|&c| {
            matches!(
                c,
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(_),
                }
            )
        });

        assert_eq!(state.pacemaker.get_current_round(), Round(1));
        assert!(result.is_some());

        let p2 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p2.destructure();
        let cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);
        let result = cmds.iter().find(|&c| {
            matches!(
                c,
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(_),
                }
            )
        });

        assert_eq!(state.pacemaker.get_current_round(), Round(2));
        assert!(result.is_some());

        for _ in 0..4 {
            propgen.next_proposal(&keys, &mut valset, &Default::default());
        }
        let p7 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p7.destructure();
        state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);

        assert_eq!(state.pacemaker.get_current_round(), Round(7));
    }

    #[test]
    fn old_qc_in_timeout_message() {
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        let mut qc2 = state.high_qc.clone();

        for i in 1..5 {
            let p = propgen.next_proposal(&keys, &mut valset, &Default::default());
            let (author, _, verified_message) = p.clone().destructure();
            let cmds = state.handle_proposal_message::<Sha256Hash, _>(
                author,
                verified_message,
                &mut valset,
            );
            let result = cmds.iter().find(|&c| {
                matches!(
                    c,
                    ConsensusCommand::Publish {
                        target: RouterTarget::PointToPoint(_),
                        message: ConsensusMessage::Vote(_),
                    }
                )
            });

            if i == 3 {
                qc2 = p.block.qc.clone();
                assert_eq!(qc2.info.vote.round, Round(2));
            }

            assert_eq!(state.pacemaker.get_current_round(), Round(i));
            assert!(result.is_some());
        }

        let byzantine_tm = TimeoutMessage {
            tminfo: TimeoutInfo {
                round: state.pacemaker.get_current_round(),
                high_qc: qc2,
            },
            last_round_tc: None,
        };
        let signed_byzantine_tm = Verified::new::<Sha256Hash>(byzantine_tm, &keys[1]);
        let (author, signature, tm) = signed_byzantine_tm.destructure();
        state.handle_timeout_message::<_>(author, signature, tm, &mut valset);
    }

    #[test]
    fn duplicate_proposals() {
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        let p1 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p1.clone().destructure();
        let cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);
        let result = cmds.iter().find(|&c| {
            matches!(
                c,
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(_),
                }
            )
        });
        assert!(result.is_some());

        // send duplicate of p1, expect it to be ignored and no output commands
        let (author, _, verified_message) = p1.destructure();
        let cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_out_of_order_proposals() {
        let perms = (0..4).permutations(4).collect::<Vec<_>>();

        for perm in perms {
            out_of_order_proposals(perm);
        }
    }

    fn out_of_order_proposals(perms: Vec<usize>) {
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        // first proposal
        let p1 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p1.destructure();
        let cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);
        let result = cmds.iter().find(|&c| {
            matches!(
                c,
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(_),
                }
            )
        });

        assert_eq!(state.pacemaker.get_current_round(), Round(1));
        assert!(result.is_some());

        // second proposal
        let p2 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p2.destructure();
        let cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);
        let result = cmds.iter().find(|&c| {
            matches!(
                c,
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(_),
                }
            )
        });

        assert_eq!(state.pacemaker.get_current_round(), Round(2));
        assert!(result.is_some());

        let mut missing_proposals = Vec::new();
        for _ in 0..perms.len() {
            missing_proposals.push(propgen.next_proposal(&keys, &mut valset, &Default::default()));
        }

        // last proposal arrvies
        let p_fut = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p_fut.destructure();
        state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);

        // confirm the size of the pending_block_tree (genesis, p1, p2, p_fut)
        assert_eq!(state.pending_block_tree.size(), 4);

        // missed proposals now arrive
        let mut cmds = Vec::new();
        for i in &perms {
            let (author, _, verified_message) = missing_proposals[*i].clone().destructure();
            cmds.extend(state.handle_proposal_message::<Sha256Hash, _>(
                author,
                verified_message,
                &mut valset,
            ));
        }

        let _self_id = PeerId(state.nodeid.0);
        let mut more_proposals = true;

        while more_proposals {
            cmds = cmds
                .into_iter()
                .filter(|m| {
                    matches!(
                        m,
                        ConsensusCommand::Publish {
                            target: RouterTarget::PointToPoint(_),
                            message: ConsensusMessage::Proposal(_),
                        }
                    )
                })
                .collect::<Vec<_>>();

            let mut proposals = Vec::new();
            let c = if cmds.is_empty() {
                break;
            } else {
                cmds.remove(0)
            };

            match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_self_id),
                    message: ConsensusMessage::Proposal(m),
                } => {
                    proposals.extend(state.handle_proposal_message::<Sha256Hash, _>(
                        m.block.author,
                        m.clone(),
                        &mut valset,
                    ));
                }
                _ => more_proposals = false,
            }
            cmds.extend(proposals);
        }

        // next proposal will trigger everything to be committed if there is
        // a consecutive chain as expected
        // last proposal arrvies
        let p_last = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p_last.destructure();
        state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);

        assert_eq!(state.pending_block_tree.size(), 3);
        assert_eq!(
            state.pacemaker.get_current_round(),
            Round(perms.len() as u64 + 4),
            "order of proposals {:?}",
            perms
        );
    }

    #[test]
    fn test_commit_rule_consecutive() {
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);

        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        // round 1 proposal
        let p1 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p1.destructure();
        let p1_cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);

        let p1_votes = p1_cmds
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(p1_votes.len() == 1);
        assert!(p1_votes[0].ledger_commit_info.commit_state_hash.is_some());

        // round 2 proposal
        let p2 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p2.destructure();
        let p2_cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);

        let lc = p2_cmds
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(lc.is_none());

        let p2_votes = p2_cmds
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(p2_votes.len() == 1);
        // csh is some: the proposal and qc have consecutive rounds
        assert!(p2_votes[0].ledger_commit_info.commit_state_hash.is_some());

        let p3 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p3.destructure();

        let p2_cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);
        let lc = p2_cmds
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(lc.is_some());
    }

    #[test]
    fn test_commit_rule_non_consecutive() {
        use monad_consensus::pacemaker::PacemakerCommand::Broadcast;
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);

        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        // round 1 proposal
        let p1 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p1.destructure();
        state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);

        // round 2 proposal
        let p2 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p2.destructure();
        let p2_cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);

        let p2_votes = p2_cmds
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(p2_votes.len() == 1);
        assert!(p2_votes[0].ledger_commit_info.commit_state_hash.is_some());

        // round 2 timeout
        let pacemaker_cmds =
            state
                .pacemaker
                .handle_event(&mut state.safety, &state.high_qc, PacemakerTimerExpire);

        let broadcast_cmd = pacemaker_cmds
            .iter()
            .find(|cmd| matches!(cmd, Broadcast(_)))
            .unwrap();

        let tmo_msg = if let Broadcast(tmo_msg) = broadcast_cmd {
            tmo_msg
        } else {
            panic!()
        };
        assert_eq!(tmo_msg.tminfo.round, Round(2));

        let _ = propgen.next_tc(&keys, &mut valset);

        // round 3 proposal, has qc(1)
        let p3 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        assert_eq!(p3.block.qc.info.vote.round, Round(1));
        assert_eq!(p3.block.round, Round(3));
        let (author, _, verified_message) = p3.destructure();
        let p3_cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);

        let p3_votes = p3_cmds
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(p3_votes.len() == 1);
        // proposal and qc have non-consecutive rounds
        assert!(p3_votes[0].ledger_commit_info.commit_state_hash.is_none());
    }

    // this test checks that a malicious proposal sent only to the next leader is
    // not incorrectly committed
    #[test]
    fn test_malicious_proposal_to_next_leader() {
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);

        let (first_state, xs) = states.split_first_mut().unwrap();
        let (second_state, xs) = xs.split_first_mut().unwrap();
        let (third_state, xs) = xs.split_first_mut().unwrap();
        let fourth_state = &mut xs[0];

        // first_state will send 2 different proposals, A and B. A will be sent to
        // the next leader, all other nodes get B.
        // effect is that nodes send votes for B to the next leader who thinks that
        // the "correct" proposal is A.
        let mut correct_proposal_gen = ProposalGen::new(first_state.high_qc.clone());
        let mut mal_proposal_gen = ProposalGen::new(first_state.high_qc.clone());

        let cp1 = correct_proposal_gen.next_proposal(&keys, &mut valset, &Default::default());
        let mp1 = mal_proposal_gen.next_proposal(&keys, &mut valset, &TransactionList(vec![5]));

        let (author, _, verified_message) = cp1.destructure();
        let cmds1 = first_state.handle_proposal_message::<Sha256Hash, _>(
            author,
            verified_message.clone(),
            &mut valset,
        );
        let p1_votes = cmds1
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>()[0];

        let cmds3 = third_state.handle_proposal_message::<Sha256Hash, _>(
            author,
            verified_message.clone(),
            &mut valset,
        );
        let p3_votes = cmds3
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>()[0];

        let cmds4 = fourth_state.handle_proposal_message::<Sha256Hash, _>(
            author,
            verified_message,
            &mut valset,
        );
        let p4_votes = cmds4
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>()[0];

        let (author, _, verified_message) = mp1.destructure();
        let cmds2 = second_state.handle_proposal_message::<Sha256Hash, _>(
            author,
            verified_message,
            &mut valset,
        );
        let p2_votes = cmds2
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>()[0];

        assert_eq!(p1_votes, p3_votes);
        assert_eq!(p1_votes, p4_votes);
        assert_ne!(p1_votes, p2_votes);

        let votes = vec![p1_votes, p2_votes, p3_votes, p4_votes];

        // Deliver all the votes to second_state, who is the leader for the next round
        for i in 0..4 {
            let v = Verified::<NopSignature, VoteMessage>::new::<Sha256Hash>(votes[i], &keys[i]);
            second_state.handle_vote_message::<Sha256Hash, _>(
                *v.author(),
                *v.author_signature(),
                *v,
                &mut valset,
            );
        }

        // confirm that the votes lead to a QC forming (which leads to high_qc update)
        assert_eq!(second_state.high_qc.info.vote, votes[0].vote_info);

        // use the correct proposal gen to make next proposal and send it to second_state
        // this should cause it to emit the RequestSync command because the the QC parent
        // points to a different proposal (second_state has the malicious proposal in its blocktree)
        let cp2 = correct_proposal_gen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = cp2.destructure();
        let cmds2 = second_state.handle_proposal_message::<Sha256Hash, _>(
            author,
            verified_message.clone(),
            &mut valset,
        );
        let res = cmds2.into_iter().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    blockid: BlockId(_),
                }
            )
        });
        assert!(res.is_some());

        // first_state has the correct block in its blocktree, so it should not request anything
        let cmds1 = first_state.handle_proposal_message::<Sha256Hash, _>(
            author,
            verified_message,
            &mut valset,
        );
        let res = cmds1.into_iter().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    blockid: BlockId(_),
                }
            )
        });
        assert!(res.is_none());

        // next correct proposal is created and we send it to the first two states.
        let cp3 = correct_proposal_gen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = cp3.destructure();
        let cmds2 = second_state.handle_proposal_message::<Sha256Hash, _>(
            author,
            verified_message.clone(),
            &mut valset,
        );
        let cmds1 = first_state.handle_proposal_message::<Sha256Hash, _>(
            author,
            verified_message,
            &mut valset,
        );

        // second_state has the malicious block in the blocktree, so it will not be able to
        // commit anything
        assert_eq!(second_state.pending_block_tree.size(), 4);
        let res = cmds2
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(res.is_none());

        // first_state has the correct blocks, so expect to see a commit
        assert_eq!(first_state.pending_block_tree.size(), 3);
        let res = cmds1
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(res.is_some());
    }
}

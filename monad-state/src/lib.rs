use std::{fmt::Debug, marker::PhantomData, ops::Deref, time::Duration};

use bytes::Bytes;
use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncResponseMessage, ProposalMessage, RequestBlockSyncMessage},
    },
    validation::signing::{Unverified, Verified},
};
use monad_consensus_state::{
    command::{Checkpoint, ConsensusCommand},
    ConsensusConfig, ConsensusProcess,
};
use monad_consensus_types::{
    block::{Block, FullBlock},
    message_signature::MessageSignature,
    payload::{ExecutionArtifacts, Payload, RandaoReveal},
    signature_collection::{
        SignatureCollection, SignatureCollectionKeyPairType, SignatureCollectionPubKeyType,
    },
    validation,
    validator_data::ValidatorData,
    voting::ValidatorMapping,
};
use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_eth_types::EthAddress;
use monad_executor::State;
use monad_executor_glue::{
    CheckpointCommand, Command, ConsensusEvent, ExecutionLedgerCommand, Identifiable,
    LedgerCommand, MempoolCommand, Message, MonadEvent, RouterCommand, StateRootHashCommand,
    TimerCommand,
};
use monad_tracing_counter::inc_count;
use monad_types::{Epoch, NodeId, RouterTarget, Stake, TimeoutVariant};
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSetType};
use ref_cast::RefCast;

use crate::blocksync::BlockSyncResponder;

pub mod blocksync;
pub mod convert;

pub struct MonadState<CT, ST, SCT, VT, LT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    /// Core consensus algorithm state machine
    consensus: CT,
    /// Handle responses to block sync requests from other nodes
    block_sync_respond: BlockSyncResponder,

    /// Algorithm for choosing leaders for the consensus algorithm
    leader_election: LT,
    /// Track the current epoch (time for which a validator set is valid)
    epoch: Epoch,
    /// The current epoch's validator set
    validator_set: VT,
    /// Maps the NodeId to the Certificate PubKey
    validator_mapping: ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    /// Validator set for next epoch
    upcoming_validator_set: Option<VT>,

    _pd: PhantomData<ST>,
}

impl<CT, ST, SCT, VT, LT> MonadState<CT, ST, SCT, VT, LT>
where
    CT: ConsensusProcess<SCT>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
{
    pub fn consensus(&self) -> &CT {
        &self.consensus
    }

    pub fn pubkey(&self) -> PubKey {
        self.consensus.get_pubkey()
    }

    pub fn blocktree(&self) -> &BlockTree<SCT> {
        self.consensus.blocktree()
    }

    fn advance_epoch(&mut self, val_set: Option<ValidatorData<SCT>>) {
        self.epoch = self.epoch + Epoch(1);

        if let Some(vs) = self.upcoming_validator_set.take() {
            self.validator_set = vs;
        }

        // FIXME-2 testnet_panic when that's implemented.
        // TODO-3 decide error handling behaviour for prod
        self.upcoming_validator_set = val_set.map(|v| {
            VT::new(v.get_stakes())
                .expect("ValidatorData should not have duplicates or invalid entries")
        });
    }

    fn load_epoch(
        &mut self,
        epoch: Epoch,
        val_set: ValidatorData<SCT>,
        upcoming_val_set: ValidatorData<SCT>,
    ) {
        self.epoch = epoch;
        // FIXME-2 testnet_panic when that's implemented.
        // TODO-3 decide error handling behaviour for prod
        self.validator_set = VT::new(val_set.get_stakes())
            .expect("ValidatorData should not have duplicates or invalid entries");
        self.upcoming_validator_set = Some(
            VT::new(upcoming_val_set.get_stakes())
                .expect("ValidatorData should not have duplicates or invalid entries"),
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedMonadMessage<ST, SCT: SignatureCollection>(Verified<ST, ConsensusMessage<SCT>>);

impl<ST, SCT: SignatureCollection> From<Verified<ST, ConsensusMessage<SCT>>>
    for VerifiedMonadMessage<ST, SCT>
{
    fn from(value: Verified<ST, ConsensusMessage<SCT>>) -> Self {
        Self(value)
    }
}

#[derive(RefCast)]
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonadMessage<ST, SCT: SignatureCollection>(Unverified<ST, ConsensusMessage<SCT>>);

impl<MS: MessageSignature, SCT: SignatureCollection> monad_types::Serializable<Bytes>
    for VerifiedMonadMessage<MS, SCT>
{
    fn serialize(&self) -> Bytes {
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

impl<MS: MessageSignature, SCT: SignatureCollection> monad_types::Deserializable<Bytes>
    for MonadMessage<MS, SCT>
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        Ok(MonadMessage(
            monad_consensus::convert::interface::deserialize_unverified_consensus_message(
                message.clone(),
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

impl<ST, SCT: SignatureCollection> Deref for VerifiedMonadMessage<ST, SCT> {
    type Target = ConsensusMessage<SCT>;
    fn deref(&self) -> &ConsensusMessage<SCT> {
        self.0.deref()
    }
}

impl<ST, SCT> Identifiable for MonadMessage<ST, SCT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    type Id = ST;

    fn id(&self) -> Self::Id {
        *self.0.author_signature()
    }
}

impl<ST, SCT> Message for MonadMessage<ST, SCT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    type Event = MonadEvent<ST, SCT>;

    fn event(self, from: NodeId) -> Self::Event {
        // MUST assert that output is valid and came from the `from` NodeId
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
    pub validators: Vec<(PubKey, Stake, SignatureCollectionPubKeyType<SCT>)>,
    pub key: KeyPair,
    pub certkey: SignatureCollectionKeyPairType<SCT>,
    pub beneficiary: EthAddress,

    pub delta: Duration,
    pub consensus_config: ConsensusConfig,
}

impl<CT, ST, SCT, VT, LT> State for MonadState<CT, ST, SCT, VT, LT>
where
    CT: ConsensusProcess<SCT>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
    LT: LeaderElection,
{
    type Config = MonadConfig<SCT, CT::TransactionValidatorType>;
    type Event = MonadEvent<ST, SCT>;
    type Message = MonadMessage<ST, SCT>;
    type OutboundMessage = VerifiedMonadMessage<ST, SCT>;
    type Block = FullBlock<SCT>;
    type Checkpoint = Checkpoint<SCT>;
    type SignatureCollection = SCT;

    fn init(
        config: Self::Config,
    ) -> (
        Self,
        Vec<
            Command<
                Self::Event,
                Self::OutboundMessage,
                Self::Block,
                Self::Checkpoint,
                Self::SignatureCollection,
            >,
        >,
    ) {
        let staking_list = config
            .validators
            .iter()
            .map(|(pubkey, stake, _)| (NodeId(*pubkey), *stake))
            .collect::<Vec<_>>();

        let voting_identities = config
            .validators
            .into_iter()
            .map(|(pubkey, _, certpubkey)| (NodeId(pubkey), certpubkey))
            .collect::<Vec<_>>();

        // create the initial validator set
        let val_set = VT::new(staking_list).expect("initial validator set init failed");
        let val_mapping = ValidatorMapping::new(voting_identities);
        let election = LT::new();

        let mut monad_state: MonadState<CT, ST, SCT, VT, LT> = Self {
            validator_set: val_set,
            validator_mapping: val_mapping,
            upcoming_validator_set: None,
            leader_election: election,
            epoch: Epoch(0),
            consensus: CT::new(
                config.transaction_validator,
                config.key.pubkey(),
                config.delta,
                config.consensus_config,
                config.key,
                config.certkey,
                config.beneficiary,
            ),
            block_sync_respond: BlockSyncResponder {},

            _pd: PhantomData,
        };

        let init_cmds = monad_state.update(MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(
            TimeoutVariant::Pacemaker,
        )));

        (monad_state, init_cmds)
    }

    fn update(
        &mut self,
        event: Self::Event,
    ) -> Vec<
        Command<
            Self::Event,
            Self::OutboundMessage,
            Self::Block,
            Self::Checkpoint,
            Self::SignatureCollection,
        >,
    > {
        let _event_span = tracing::info_span!("event_span", ?event).entered();
        match event {
            MonadEvent::ConsensusEvent(consensus_event) => {
                let consensus_commands: Vec<ConsensusCommand<SCT>> = match consensus_event {
                    ConsensusEvent::Timeout(tmo_event) => match tmo_event {
                        TimeoutVariant::Pacemaker => self
                            .consensus
                            .handle_timeout_expiry()
                            .into_iter()
                            .map(|cmd| {
                                ConsensusCommand::from_pacemaker_command(
                                    self.consensus.get_cert_keypair(),
                                    cmd,
                                )
                            })
                            .collect(),
                        TimeoutVariant::BlockSync(bid) => self
                            .consensus
                            .handle_block_sync_tmo(bid, &self.validator_set),
                    },

                    ConsensusEvent::FetchedTxs(fetched, txns) => {
                        assert_eq!(fetched.node_id, self.consensus.get_nodeid());

                        let mut cmds = vec![ConsensusCommand::FetchTxsReset];

                        // make sure we're still in same round
                        if fetched.round == self.consensus.get_current_round() {
                            let mut header = ExecutionArtifacts::zero();
                            header.state_root = fetched.state_root_hash;
                            let b = Block::new(
                                fetched.node_id,
                                fetched.round,
                                &Payload {
                                    txns,
                                    header,
                                    seq_num: fetched.seq_num,
                                    beneficiary: self.consensus.get_beneficiary(),
                                    randao_reveal: RandaoReveal::new::<SCT::SignatureType>(
                                        fetched.round,
                                        self.consensus.get_cert_keypair(),
                                    ),
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
                            });
                        }

                        cmds
                    }
                    ConsensusEvent::FetchedFullTxs(fetched_txs, txns) => {
                        let mut cmds = vec![ConsensusCommand::FetchFullTxsReset];

                        if let Some(txns) = txns {
                            let proposal_msg = ProposalMessage {
                                block: fetched_txs.p_block,
                                last_round_tc: fetched_txs.p_last_round_tc,
                            };
                            cmds.extend(self.consensus.handle_proposal_message_full(
                                fetched_txs.author,
                                proposal_msg,
                                txns,
                                &self.validator_set,
                                &self.validator_mapping,
                                &self.leader_election,
                            ));
                        }

                        cmds
                    }
                    ConsensusEvent::FetchedBlock(fetched_b) => {
                        vec![
                            ConsensusCommand::LedgerFetchReset(
                                fetched_b.requester,
                                fetched_b.block_id,
                            ),
                            ConsensusCommand::Publish {
                                target: RouterTarget::PointToPoint(NodeId(fetched_b.requester.0)),
                                message: ConsensusMessage::BlockSync(
                                    match fetched_b.unverified_full_block {
                                        Some(b) => BlockSyncResponseMessage::BlockFound(b),
                                        None => BlockSyncResponseMessage::NotAvailable(
                                            fetched_b.block_id,
                                        ),
                                    },
                                ),
                            },
                        ]
                    }
                    ConsensusEvent::Message {
                        sender,
                        unverified_message,
                    } => {
                        let verified_message = match unverified_message.verify(
                            &self.validator_set,
                            &self.validator_mapping,
                            &sender,
                        ) {
                            Ok(m) => m,
                            Err(e) => {
                                // TODO-2: collect evidence
                                let evidence_cmds = vec![];
                                match e {
                                    validation::Error::InvalidAuthor => {
                                        inc_count!(invalid_author)
                                    }
                                    validation::Error::NotWellFormed => {
                                        inc_count!(not_well_formed_sig)
                                    }
                                    validation::Error::InvalidSignature => {
                                        inc_count!(invalid_signature)
                                    }
                                    validation::Error::AuthorNotSender => {
                                        inc_count!(author_not_sender)
                                    }
                                    validation::Error::InvalidTcRound => {
                                        inc_count!(invalid_tc_round)
                                    }
                                    validation::Error::InsufficientStake => {
                                        inc_count!(insufficient_stake)
                                    }
                                    validation::Error::InvalidSeqNum => {
                                        inc_count!(invalid_seq_num)
                                    }
                                };
                                return evidence_cmds;
                            }
                        };
                        let (author, _, verified_message) = verified_message.destructure();
                        match verified_message {
                            ConsensusMessage::Proposal(msg) => {
                                self.consensus.handle_proposal_message(author, msg)
                            }
                            ConsensusMessage::Vote(msg) => self.consensus.handle_vote_message(
                                author,
                                msg,
                                &self.validator_set,
                                &self.validator_mapping,
                                &self.leader_election,
                            ),
                            ConsensusMessage::Timeout(msg) => {
                                self.consensus.handle_timeout_message(
                                    author,
                                    msg,
                                    &self.validator_set,
                                    &self.validator_mapping,
                                    &self.leader_election,
                                )
                            }
                            ConsensusMessage::RequestBlockSync(msg) => {
                                if let Some(block) =
                                    self.consensus.fetch_uncommitted_block(&msg.block_id)
                                {
                                    // retrieve if currently cached in pending block tree
                                    vec![ConsensusCommand::Publish {
                                        target: RouterTarget::PointToPoint(author),
                                        message: ConsensusMessage::BlockSync(
                                            BlockSyncResponseMessage::BlockFound(
                                                block.clone().into(),
                                            ),
                                        ),
                                    }]
                                } else {
                                    // else ask ledger
                                    self.block_sync_respond
                                        .handle_request_block_sync_message(author, msg)
                                }
                            }
                            ConsensusMessage::BlockSync(msg) => {
                                self.consensus
                                    .handle_block_sync(author, msg, &self.validator_set)
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
                    ConsensusEvent::StateUpdate((seq_num, root_hash)) => {
                        self.consensus.handle_state_root_update(seq_num, root_hash);
                        Vec::new()
                    }
                };

                let prepare_router_message =
                    |target: RouterTarget, message: ConsensusMessage<SCT>| {
                        let message =
                            VerifiedMonadMessage(message.sign(self.consensus.get_keypair()));
                        Command::RouterCommand(RouterCommand::Publish { target, message })
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
                            variant: on_timeout,
                            on_timeout: Self::Event::ConsensusEvent(ConsensusEvent::Timeout(
                                on_timeout,
                            )),
                        })),
                        ConsensusCommand::ScheduleReset(on_timeout) => cmds.push(
                            Command::TimerCommand(TimerCommand::ScheduleReset(on_timeout)),
                        ),
                        ConsensusCommand::FetchTxs(fetch_criteria) => cmds.push(
                            Command::MempoolCommand(MempoolCommand::FetchTxs(fetch_criteria)),
                        ),
                        ConsensusCommand::FetchTxsReset => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::FetchReset))
                        }
                        ConsensusCommand::FetchFullTxs(txs, fetch_params) => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::FetchFullTxs(
                                txs,
                                fetch_params,
                            )))
                        }
                        ConsensusCommand::FetchFullTxsReset => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::FetchFullReset))
                        }
                        ConsensusCommand::DrainTxs(txs) => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::DrainTxs(txs)))
                        }

                        ConsensusCommand::RequestSync { peer, block_id } => {
                            cmds.push(prepare_router_message(
                                RouterTarget::PointToPoint(peer),
                                ConsensusMessage::RequestBlockSync(RequestBlockSyncMessage {
                                    block_id,
                                }),
                            ));
                        }
                        ConsensusCommand::LedgerCommit(block) => {
                            cmds.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(
                                block.clone(),
                            )));
                            cmds.push(Command::ExecutionLedgerCommand(
                                ExecutionLedgerCommand::LedgerCommit(block),
                            ))
                        }
                        ConsensusCommand::LedgerFetch(n_id, b_id, cb) => {
                            cmds.push(Command::LedgerCommand(LedgerCommand::LedgerFetch(
                                n_id,
                                b_id,
                                Box::new(|full_block: Option<FullBlock<_>>| {
                                    Self::Event::ConsensusEvent(ConsensusEvent::FetchedBlock(cb(
                                        full_block.map(|b| b.into()),
                                    )))
                                }),
                            )))
                        }
                        ConsensusCommand::LedgerFetchReset(node_id, block_id) => {
                            cmds.push(Command::LedgerCommand(LedgerCommand::LedgerFetchReset(
                                node_id, block_id,
                            )))
                        }
                        ConsensusCommand::CheckpointSave(checkpoint) => cmds.push(
                            Command::CheckpointCommand(CheckpointCommand::Save(checkpoint)),
                        ),
                        ConsensusCommand::StateRootHash(full_block) => {
                            cmds.push(Command::StateRootHashCommand(
                                StateRootHashCommand::LedgerCommit(full_block),
                            ))
                        }
                    }
                }

                cmds
            }
        }
    }
}

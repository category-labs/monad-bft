use std::{fmt::Debug, marker::PhantomData, time::Duration};

use monad_block_sync::BlockSyncProcess;
use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncMessage, ProposalMessage, RequestBlockSyncMessage},
    },
    pacemaker::PacemakerTimerExpire,
    validation::signing::{Unverified, Verified},
};
use monad_consensus_state::{
    command::{Checkpoint, ConsensusCommand},
    ConsensusConfig, ConsensusProcess,
};
use monad_consensus_types::{
    block::{Block, FullBlock},
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
use monad_eth_types::EthAddress;
use monad_executor::State;
use monad_executor_glue::{
    CheckpointCommand, Command, ConsensusEvent, Identifiable, LedgerCommand, MempoolCommand,
    Message, MonadEvent, PeerId, RouterCommand, RouterTarget, StateRootHashCommand, TimerCommand,
};
use monad_types::{Epoch, NodeId, Stake, ValidatorData};
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSetType};
use ref_cast::RefCast;

pub mod convert;

type HasherType = Sha256Hash;

pub struct MonadState<CT, ST, SCT, VT, LT, BST>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    consensus: CT,
    epoch: Epoch,
    leader_election: LT,
    validator_set: VT,
    validator_mapping: ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    upcoming_validator_set: Option<VT>,
    block_sync: BST,

    _pd: PhantomData<ST>,
}

impl<CT, ST, SCT, VT, LT, BST> MonadState<CT, ST, SCT, VT, LT, BST>
where
    CT: ConsensusProcess<SCT>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
    BST: BlockSyncProcess<SCT, VT>,
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

#[derive(Debug, Clone)]
pub struct VerifiedMonadMessage<ST, SCT: SignatureCollection>(Verified<ST, ConsensusMessage<SCT>>);

#[derive(RefCast)]
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonadMessage<ST, SCT: SignatureCollection>(Unverified<ST, ConsensusMessage<SCT>>);

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
    pub beneficiary: EthAddress,

    pub delta: Duration,
    pub consensus_config: ConsensusConfig,
    pub genesis_block: FullBlock<SCT>,
    pub genesis_vote_info: VoteInfo,
    pub genesis_signatures: SCT,
}

impl<
        #[cfg(feature = "monad_test")] CT: ConsensusProcess<SCT> + Eq,
        #[cfg(not(feature = "monad_test"))] CT: ConsensusProcess<SCT>,
        ST,
        SCT,
        VT,
        LT,
        BST,
    > State for MonadState<CT, ST, SCT, VT, LT, BST>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
    LT: LeaderElection,
    BST: BlockSyncProcess<SCT, VT>,
{
    type Config = MonadConfig<SCT, CT::TransactionValidatorType>;
    type Event = MonadEvent<ST, SCT>;
    type Message = MonadMessage<ST, SCT>;
    type OutboundMessage = VerifiedMonadMessage<ST, SCT>;
    type Block = FullBlock<SCT>;
    type Checkpoint = Checkpoint<SCT>;
    type SignatureCollection = SCT;
    #[cfg(feature = "monad_test")]
    type ConsensusState = CT;

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
        let val_set = VT::new(staking_list).expect("initial validator set init failed");
        let val_mapping = ValidatorMapping::new(voting_identities);
        let election = LT::new();

        let genesis_qc = QuorumCertificate::genesis_qc::<HasherType>(
            config.genesis_vote_info,
            config.genesis_signatures,
        );

        let mut monad_state: MonadState<CT, ST, SCT, VT, LT, BST> = Self {
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
                config.consensus_config,
                config.key,
                config.certkey,
                config.beneficiary,
            ),
            block_sync: BST::new(),

            _pd: PhantomData,
        };

        let init_cmds = monad_state.update(MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(
            PacemakerTimerExpire,
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
        match event {
            MonadEvent::ConsensusEvent(consensus_event) => {
                let consensus_commands: Vec<ConsensusCommand<SCT>> = match consensus_event {
                    ConsensusEvent::Timeout(pacemaker_expire) => self
                        .consensus
                        .handle_timeout_expiry::<HasherType>(pacemaker_expire)
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                    ConsensusEvent::FetchedTxs(fetched, txns) => {
                        assert_eq!(fetched.node_id, self.consensus.get_nodeid());

                        let mut cmds = vec![ConsensusCommand::FetchTxsReset];

                        // make sure we're still in same round
                        if fetched.round == self.consensus.get_current_round() {
                            let mut header = ExecutionArtifacts::zero();
                            header.state_root = fetched.state_root_hash;
                            let b = Block::new::<HasherType>(
                                fetched.node_id,
                                fetched.round,
                                &Payload {
                                    txns,
                                    header,
                                    seq_num: fetched.seq_num,
                                    beneficiary: self.consensus.get_beneficiary(),
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
                            cmds.extend(
                                self.consensus
                                    .handle_proposal_message_full::<HasherType, _, _>(
                                        fetched_txs.author,
                                        proposal_msg,
                                        txns,
                                        &self.validator_set,
                                        &self.leader_election,
                                    ),
                            );
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
                                target: RouterTarget::PointToPoint(PeerId(fetched_b.requester.0)),
                                message: ConsensusMessage::BlockSync(
                                    match fetched_b.unverified_full_block {
                                        Some(b) => BlockSyncMessage::BlockFound(b),
                                        None => BlockSyncMessage::NotAvailable(fetched_b.block_id),
                                    },
                                ),
                            },
                        ]
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
                        let (author, _, verified_message) = verified_message.destructure();
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
                                self.consensus.handle_timeout_message::<HasherType, _, _>(
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
                                        target: RouterTarget::PointToPoint((&author).into()),
                                        message: ConsensusMessage::BlockSync(
                                            BlockSyncMessage::BlockFound(block.clone().into()),
                                        ),
                                    }]
                                } else {
                                    // else ask ledger
                                    self.block_sync
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
                        let message = VerifiedMonadMessage(
                            message.sign::<HasherType, ST>(self.consensus.get_keypair()),
                        );
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
                            on_timeout: MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(
                                on_timeout,
                            )),
                        })),
                        ConsensusCommand::ScheduleReset => {
                            cmds.push(Command::TimerCommand(TimerCommand::ScheduleReset))
                        }
                        ConsensusCommand::FetchTxs(max_txns, pending_txs, fetch_params) => cmds
                            .push(Command::MempoolCommand(MempoolCommand::FetchTxs(
                                max_txns,
                                pending_txs,
                                fetch_params,
                            ))),
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
                                RouterTarget::PointToPoint((&peer).into()),
                                ConsensusMessage::RequestBlockSync(RequestBlockSyncMessage {
                                    block_id,
                                }),
                            ));
                        }
                        ConsensusCommand::LedgerCommit(block) => {
                            cmds.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(block)))
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
    #[cfg(feature = "monad_test")]
    fn consensus(&self) -> &Self::ConsensusState {
        &self.consensus
    }
}

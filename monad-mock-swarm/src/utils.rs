// test utils for mock swarm unit test
#[cfg(test)]
pub mod test_tool {
    use std::time::Duration;

    use monad_consensus::messages::{
        consensus_message::ConsensusMessage,
        message::{
            BlockSyncMessage, ProposalMessage, RequestBlockSyncMessage, TimeoutMessage, VoteMessage,
        },
    };
    use monad_consensus_types::{
        block::Block,
        certificate_signature::CertificateSignature,
        ledger::LedgerCommitInfo,
        multi_sig::MultiSig,
        payload::{ExecutionArtifacts, Payload, RandaoReveal, TransactionHashList},
        quorum_certificate::{QcInfo, QuorumCertificate},
        timeout::{Timeout, TimeoutInfo},
        voting::{Vote, VoteInfo},
    };
    use monad_crypto::{
        hasher::{Hash, HasherType, Sha256Hash},
        secp256k1::KeyPair,
        NopSignature,
    };
    use monad_eth_types::EthAddress;
    use monad_state::VerifiedMonadMessage;
    use monad_testutil::signing::create_keys;
    use monad_transformer::{LinkMessage, ID};
    use monad_types::{BlockId, NodeId, Round, SeqNum};

    type ST = NopSignature;
    type SC = MultiSig<NopSignature>;
    type H = Sha256Hash;
    type QC = QuorumCertificate<SC>;

    /// FIXME these should take in from/to/from_tick as params, not have defaults
    pub fn get_mock_message() -> LinkMessage<String> {
        let keys = create_keys(2);
        LinkMessage {
            from: ID::new(NodeId(keys[0].pubkey())),
            to: ID::new(NodeId(keys[1].pubkey())),
            message: "Dummy Message".to_string(),
            from_tick: Duration::from_millis(10),
        }
    }

    pub fn fake_node_id() -> NodeId {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();
        NodeId(keypair.pubkey())
    }

    pub fn fake_qc() -> QuorumCertificate<SC> {
        QC::new::<H>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x00_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x00_u8; 32])),
                    parent_round: Round(0),
                    seq_num: SeqNum(0),
                },
                ledger_commit: LedgerCommitInfo::default(),
            },
            MultiSig { sigs: vec![] },
        )
    }

    pub fn fake_block(round: Round) -> Block<SC> {
        let payload = Payload {
            txns: TransactionHashList::default(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };

        Block::new::<H>(fake_node_id(), round, &payload, &fake_qc())
    }

    pub fn fake_proposal_message(kp: &KeyPair, round: Round) -> VerifiedMonadMessage<ST, SC> {
        let internal_msg = ProposalMessage {
            block: fake_block(round),
            last_round_tc: None,
        };
        ConsensusMessage::Proposal(internal_msg)
            .sign::<HasherType, NopSignature>(kp)
            .into()
    }

    pub fn fake_vote_message(kp: &KeyPair, round: Round) -> VerifiedMonadMessage<ST, SC> {
        let vote_info = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };
        let internal_msg = VoteMessage {
            vote: Vote {
                vote_info,
                ledger_commit_info: LedgerCommitInfo::new::<H>(None, &vote_info),
            },
            sig: NopSignature::sign(&[0x00_u8, 32], kp),
        };
        ConsensusMessage::Vote(internal_msg)
            .sign::<HasherType, NopSignature>(kp)
            .into()
    }

    pub fn fake_timeout_message(kp: &KeyPair) -> VerifiedMonadMessage<ST, SC> {
        let timeout_info = TimeoutInfo {
            round: Round(0),
            high_qc: fake_qc(),
        };
        let internal_msg = TimeoutMessage {
            timeout: Timeout {
                tminfo: timeout_info,
                last_round_tc: None,
            },
            sig: NopSignature::sign(&[0x00_u8, 32], kp),
        };
        ConsensusMessage::Timeout(internal_msg)
            .sign::<HasherType, NopSignature>(kp)
            .into()
    }

    pub fn fake_request_block_sync(kp: &KeyPair) -> VerifiedMonadMessage<ST, SC> {
        let internal_msg = RequestBlockSyncMessage {
            block_id: BlockId(Hash([0x00_u8; 32])),
        };
        ConsensusMessage::RequestBlockSync(internal_msg)
            .sign::<HasherType, NopSignature>(kp)
            .into()
    }

    pub fn fake_block_sync(kp: &KeyPair) -> VerifiedMonadMessage<ST, SC> {
        let internal_msg = BlockSyncMessage::NotAvailable(BlockId(Hash([0x00_u8; 32])));
        ConsensusMessage::BlockSync(internal_msg)
            .sign::<HasherType, NopSignature>(kp)
            .into()
    }
}

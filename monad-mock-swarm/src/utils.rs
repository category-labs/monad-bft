// test utils for mock swarm unit test
#[cfg(test)]
pub mod test_tool {
    use std::time::Duration;

    use monad_blocksync::messages::message::{
        BlockSyncHeadersResponse, BlockSyncRequestMessage, BlockSyncResponseMessage,
    };
    use monad_consensus::messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
    };
    use monad_consensus_types::{
        block::{Block, BlockKind, BlockRange},
        payload::{
            ExecutionProtocol, FullTransactionList, Payload, RandaoReveal, TransactionPayload,
        },
        quorum_certificate::QuorumCertificate,
        state_root_hash::StateRootHash,
        timeout::{Timeout, TimeoutInfo},
        voting::Vote,
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        },
        hasher::Hash,
        NopSignature,
    };
    use monad_eth_types::EthAddress;
    use monad_multi_sig::MultiSig;
    use monad_state::VerifiedMonadMessage;
    use monad_testutil::signing::create_keys;
    use monad_transformer::{LinkMessage, ID};
    use monad_types::{BlockId, DontCare, Epoch, NodeId, Round, SeqNum};

    type ST = NopSignature;
    type KeyPairType = <ST as CertificateSignature>::KeyPairType;
    type PubKeyType = CertificateSignaturePubKey<ST>;
    type SC = MultiSig<NopSignature>;
    type QC = QuorumCertificate<SC>;

    /// FIXME-3 these should take in from/to/from_tick as params, not have defaults
    pub fn get_mock_message() -> LinkMessage<PubKeyType, String> {
        let keys = create_keys::<ST>(2);
        LinkMessage {
            from: ID::new(NodeId::new(keys[0].pubkey())),
            to: ID::new(NodeId::new(keys[1].pubkey())),
            message: "Dummy Message".to_string(),
            from_tick: Duration::from_millis(10),
        }
    }

    pub fn fake_node_id() -> NodeId<PubKeyType> {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPairType::from_bytes(&mut privkey).unwrap();
        NodeId::new(keypair.pubkey())
    }

    pub fn fake_qc() -> QuorumCertificate<SC> {
        QC::new(
            Vote {
                ..DontCare::dont_care()
            },
            MultiSig { sigs: vec![] },
        )
    }

    pub fn fake_block(round: Round) -> (Block<SC>, Payload) {
        let execution = ExecutionProtocol {
            state_root: StateRootHash::default(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let payload = Payload {
            txns: TransactionPayload::List(FullTransactionList::empty()),
        };

        (
            Block::new(
                fake_node_id(),
                0,
                Epoch(1),
                round,
                &execution,
                payload.get_id(),
                BlockKind::Executable,
                &fake_qc(),
            ),
            payload,
        )
    }

    pub fn fake_proposal_message(kp: &KeyPairType, round: Round) -> VerifiedMonadMessage<ST, SC> {
        let (block, payload) = fake_block(round);
        let internal_msg = ProposalMessage {
            block,
            payload,
            last_round_tc: None,
        };
        ConsensusMessage {
            version: "TEST".into(),
            message: ProtocolMessage::Proposal(internal_msg),
        }
        .sign(kp)
        .into()
    }

    pub fn fake_vote_message(kp: &KeyPairType, round: Round) -> VerifiedMonadMessage<ST, SC> {
        let vote = Vote {
            round,
            ..DontCare::dont_care()
        };
        let internal_msg = VoteMessage {
            vote,
            sig: NopSignature::sign(&[0x00_u8, 32], kp),
        };
        ConsensusMessage {
            version: "TEST".into(),
            message: ProtocolMessage::Vote(internal_msg),
        }
        .sign(kp)
        .into()
    }

    pub fn fake_timeout_message(kp: &KeyPairType) -> VerifiedMonadMessage<ST, SC> {
        let timeout_info = TimeoutInfo {
            epoch: Epoch(1),
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
        ConsensusMessage {
            version: "TEST".into(),
            message: ProtocolMessage::Timeout(internal_msg),
        }
        .sign(kp)
        .into()
    }

    pub fn fake_request_block_sync() -> VerifiedMonadMessage<ST, SC> {
        let internal_msg = BlockSyncRequestMessage::Headers(BlockRange {
            last_block_id: BlockId(Hash([0x01_u8; 32])),
            root_seq_num: SeqNum(1),
        });
        VerifiedMonadMessage::BlockSyncRequest(internal_msg)
    }

    pub fn fake_block_sync() -> VerifiedMonadMessage<ST, SC> {
        let internal_msg = BlockSyncResponseMessage::HeadersResponse(
            BlockSyncHeadersResponse::NotAvailable(BlockRange {
                last_block_id: BlockId(Hash([0x01_u8; 32])),
                root_seq_num: SeqNum(1),
            }),
        );
        VerifiedMonadMessage::BlockSyncResponse(internal_msg)
    }
}

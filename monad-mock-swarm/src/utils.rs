// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// test utils for mock swarm unit test
#[cfg(test)]
pub mod test_tool {
    use std::time::Duration;

    use bytes::Bytes;
    use monad_blocksync::messages::message::{
        BlockSyncHeadersResponse, BlockSyncRequestMessage, BlockSyncResponseMessage,
    };
    use monad_consensus::messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
    };
    use monad_consensus_types::{
        block::{
            BlockRange, ConsensusBlockHeader, MockExecutionBody, MockExecutionProposedHeader,
            MockExecutionProtocol,
        },
        payload::{ConsensusBlockBody, ConsensusBlockBodyInner, RoundSignature},
        quorum_certificate::QuorumCertificate,
        timeout::{HighExtendVote, Timeout, TimeoutInfo},
        tip::ConsensusTip,
        voting::Vote,
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        },
        hasher::Hash,
        signing_domain, NopKeyPair, NopSignature,
    };
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
    type EP = MockExecutionProtocol;

    /// FIXME-3 these should take in from/to/from_tick as params, not have defaults
    pub fn get_mock_message() -> LinkMessage<PubKeyType, String> {
        let keys = create_keys::<ST>(2);
        LinkMessage {
            from: ID::new(NodeId::new(keys[0].pubkey())),
            to: ID::new(NodeId::new(keys[1].pubkey())),
            message: "Dummy Message".to_string(),
            from_tick: Duration::from_millis(10),
            nonce: rand::random(),
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

    pub fn fake_block(round: Round) -> (ConsensusBlockHeader<ST, SC, EP>, ConsensusBlockBody<EP>) {
        let body = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: MockExecutionBody {
                data: Bytes::default(),
            },
        });

        (
            ConsensusBlockHeader::new(
                fake_node_id(),
                Epoch(1),
                round,
                Vec::new(), // delayed_results
                MockExecutionProposedHeader {},
                body.get_id(),
                fake_qc(),
                SeqNum(0),
                0,
                RoundSignature::new(round, &NopKeyPair::from_bytes(&mut [127; 32]).unwrap()),
                Some(monad_tfm::base_fee::MIN_BASE_FEE),
                Some(monad_tfm::base_fee::GENESIS_BASE_FEE_TREND),
                Some(monad_tfm::base_fee::GENESIS_BASE_FEE_MOMENT),
            ),
            body,
        )
    }

    pub fn fake_proposal_message(
        kp: &KeyPairType,
        round: Round,
    ) -> VerifiedMonadMessage<ST, SC, EP> {
        let (block_header, block_body) = fake_block(round);
        let internal_msg = ProposalMessage {
            proposal_epoch: block_header.epoch,
            proposal_round: block_header.block_round,
            tip: ConsensusTip::new(kp, block_header, None),
            block_body,
            last_round_tc: None,
        };
        ConsensusMessage {
            version: 1,
            message: ProtocolMessage::Proposal(internal_msg),
        }
        .sign(kp)
        .into()
    }

    pub fn fake_vote_message(kp: &KeyPairType, round: Round) -> VerifiedMonadMessage<ST, SC, EP> {
        let vote = Vote {
            round,
            ..DontCare::dont_care()
        };
        let internal_msg = VoteMessage {
            vote,
            sig: NopSignature::sign::<signing_domain::Vote>(&[0x00_u8, 32], kp),
        };
        ConsensusMessage {
            version: 1,
            message: ProtocolMessage::Vote(internal_msg),
        }
        .sign(kp)
        .into()
    }

    pub fn fake_timeout_message(kp: &KeyPairType) -> VerifiedMonadMessage<ST, SC, EP> {
        let timeout_info = TimeoutInfo {
            epoch: Epoch(1),
            round: Round(0),
            high_qc_round: Round(0),
            high_tip_round: Round(0),
        };
        let internal_msg = TimeoutMessage(Timeout {
            tminfo: timeout_info,
            high_extend: HighExtendVote::Qc(QuorumCertificate::genesis_qc()),
            last_round_certificate: None,
            timeout_signature: NopSignature::sign::<signing_domain::Timeout>(&[0x00_u8, 32], kp),
        });
        ConsensusMessage {
            version: 1,
            message: ProtocolMessage::Timeout(internal_msg),
        }
        .sign(kp)
        .into()
    }

    pub fn fake_request_block_sync() -> VerifiedMonadMessage<ST, SC, EP> {
        let internal_msg = BlockSyncRequestMessage::Headers(BlockRange {
            last_block_id: BlockId(Hash([0x01_u8; 32])),
            num_blocks: SeqNum(1),
        });
        VerifiedMonadMessage::BlockSyncRequest(internal_msg)
    }

    pub fn fake_block_sync() -> VerifiedMonadMessage<ST, SC, EP> {
        let internal_msg = BlockSyncResponseMessage::HeadersResponse(
            BlockSyncHeadersResponse::NotAvailable(BlockRange {
                last_block_id: BlockId(Hash([0x01_u8; 32])),
                num_blocks: SeqNum(1),
            }),
        );
        VerifiedMonadMessage::BlockSyncResponse(internal_msg)
    }
}

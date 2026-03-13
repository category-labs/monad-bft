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

use std::fmt::Debug;

use alloy_rlp::{encode_list, Decodable, Encodable, Header, RlpDecodable, RlpEncodable};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{ExecutionProtocol, Round};
use monad_validator::signature_collection::SignatureCollection;
use serde::Serialize;

use crate::{
    messages::message::{
        AdvanceRoundMessage, NoEndorsementMessage, ProposalMessage, RoundRecoveryMessage,
        TimeoutMessage, VoteMessage,
    },
    validation::signing::{Validated, Verified},
};

const PROTOCOL_MESSAGE_NAME: &str = "ProtocolMessage";

/// Consensus protocol messages
#[derive(Clone, PartialEq, Eq, Serialize)]
pub enum ProtocolMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// Consensus protocol proposal message
    Proposal(ProposalMessage<ST, SCT, EPT>),

    /// Consensus protocol vote message
    Vote(VoteMessage<SCT>),

    /// Consensus protocol timeout message
    Timeout(TimeoutMessage<ST, SCT, EPT>),

    RoundRecovery(RoundRecoveryMessage<ST, SCT, EPT>),
    NoEndorsement(NoEndorsementMessage<SCT>),

    /// This message is broadcasted upon locally constructing QC(r)
    /// This helps other nodes advance their round faster
    AdvanceRound(AdvanceRoundMessage<ST, SCT, EPT>),
}

impl<ST, SCT, EPT> Debug for ProtocolMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolMessage::Proposal(p) => f.debug_tuple("").field(&p).finish(),
            ProtocolMessage::Vote(v) => f.debug_tuple("").field(&v).finish(),
            ProtocolMessage::Timeout(t) => f.debug_tuple("").field(&t).finish(),
            ProtocolMessage::RoundRecovery(r) => f.debug_tuple("").field(&r).finish(),
            ProtocolMessage::NoEndorsement(n) => f.debug_tuple("").field(&n).finish(),
            ProtocolMessage::AdvanceRound(l) => f.debug_tuple("").field(&l).finish(),
        }
    }
}

// FIXME-2:
// it can be confusing as we are hashing only part of the message
// in the signature refactoring, we might want a clean split between:
//      integrity sig: sign over the entire serialized struct
//      protocol sig: signatures outlined in the protocol
impl<ST, SCT, EPT> Encodable for ProtocolMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let name = PROTOCOL_MESSAGE_NAME;
        match self {
            ProtocolMessage::Proposal(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &1u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            ProtocolMessage::Vote(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            ProtocolMessage::Timeout(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &3u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            ProtocolMessage::RoundRecovery(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &4u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            ProtocolMessage::NoEndorsement(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &5u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            ProtocolMessage::AdvanceRound(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &6u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for ProtocolMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != PROTOCOL_MESSAGE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type ProtocolMessage",
            ));
        }

        let result = match u8::decode(&mut payload)? {
            1 => ProtocolMessage::Proposal(ProposalMessage::decode(&mut payload)?),
            2 => ProtocolMessage::Vote(VoteMessage::decode(&mut payload)?),
            3 => ProtocolMessage::Timeout(TimeoutMessage::decode(&mut payload)?),
            4 => ProtocolMessage::RoundRecovery(RoundRecoveryMessage::decode(&mut payload)?),
            5 => ProtocolMessage::NoEndorsement(NoEndorsementMessage::decode(&mut payload)?),
            6 => ProtocolMessage::AdvanceRound(AdvanceRoundMessage::decode(&mut payload)?),
            _ => {
                return Err(alloy_rlp::Error::Custom(
                    "failed to decode unknown ProtocolMessage",
                ))
            }
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct ConsensusMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub version: u32,
    pub message: ProtocolMessage<ST, SCT, EPT>,
}

impl<ST, SCT, EPT> ConsensusMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    #[tracing::instrument(
        level = "debug", 
        name = "consesnsus_sign"
        skip_all,
    )]
    pub fn sign(
        self,
        keypair: &ST::KeyPairType,
    ) -> Verified<ST, Validated<ConsensusMessage<ST, SCT, EPT>>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        Verified::new(Validated::new(self), keypair)
    }

    pub fn get_round(&self) -> Round {
        match &self.message {
            ProtocolMessage::Proposal(p) => p.proposal_round,
            ProtocolMessage::Vote(v) => v.vote.round,
            ProtocolMessage::Timeout(t) => t.0.tminfo.round,
            ProtocolMessage::RoundRecovery(r) => r.round,
            ProtocolMessage::NoEndorsement(n) => n.msg.round,
            ProtocolMessage::AdvanceRound(n) => n.last_round_certificate.round(),
        }
    }
}

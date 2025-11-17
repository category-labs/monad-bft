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

use std::{ops::Deref, sync::Arc};

use alloy_rlp::{RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};
use monad_crypto::{
    certificate_signature::{CertificateSignature, CertificateSignaturePubKey},
    hasher::{Hash, Hasher, HasherType},
    signing_domain,
};
use monad_types::{ExecutionProtocol, Round};
use serde::{Deserialize, Serialize};

/// randao_reveal uses a proposer's public key to contribute randomness
#[derive(
    Debug, Clone, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper, Serialize, Deserialize,
)]
#[serde(bound(serialize = "", deserialize = ""))]
pub struct RoundSignature<CST: CertificateSignature>(CST);

impl<CST: CertificateSignature> RoundSignature<CST> {
    /// TODO should this incorporate parent_block_id to increase "randomness"?
    pub fn new(round: Round, keypair: &CST::KeyPairType) -> Self {
        let encoded_round = alloy_rlp::encode(round);
        Self(CST::sign::<signing_domain::RoundSignature>(
            &encoded_round,
            keypair,
        ))
    }

    pub fn verify(
        &self,
        round: Round,
        pubkey: &CertificateSignaturePubKey<CST>,
    ) -> Result<(), CST::Error> {
        let encoded_round = alloy_rlp::encode(round);
        self.0
            .verify::<signing_domain::RoundSignature>(&encoded_round, pubkey)
    }

    pub fn get_hash(&self) -> Hash {
        let mut hasher = HasherType::new();
        hasher.update(alloy_rlp::encode(self));
        hasher.hash()
    }
}

/// Contents of a proposal that are part of the Monad protocol
/// but not in the core bft consensus protocol
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper, Serialize)]
pub struct ConsensusBlockBody<EPT>(Arc<ConsensusBlockBodyInner<EPT>>)
where
    EPT: ExecutionProtocol;

impl<EPT> ConsensusBlockBody<EPT>
where
    EPT: ExecutionProtocol,
{
    pub fn new(body: ConsensusBlockBodyInner<EPT>) -> Self {
        Self(body.into())
    }

    pub fn get_id(&self) -> ConsensusBlockBodyId {
        let mut hasher = HasherType::new();
        hasher.update(alloy_rlp::encode(self));
        ConsensusBlockBodyId(hasher.hash())
    }
}
impl<EPT> Deref for ConsensusBlockBody<EPT>
where
    EPT: ExecutionProtocol,
{
    type Target = ConsensusBlockBodyInner<EPT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct ConsensusBlockBodyInner<EPT>
where
    EPT: ExecutionProtocol,
{
    pub execution_body: EPT::Body,
}

#[repr(transparent)]
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct ConsensusBlockBodyId(pub Hash);

impl std::fmt::Debug for ConsensusBlockBodyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:>02x}{:>02x}..{:>02x}{:>02x}",
            self.0[0], self.0[1], self.0[30], self.0[31]
        )
    }
}

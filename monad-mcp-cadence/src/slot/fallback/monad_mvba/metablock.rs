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

use alloy_rlp::Decodable;

use super::{
    super::{
        super::{
            fast::{CertifiedEntry, Entry},
            types::{HeaderAuth, Lane, Lanes, ProposalScope, Slot, ValidatorData},
        },
        Metablock, ValidateInput, Votable,
    },
    ValidationContext,
};

impl Metablock {
    /// One entry per proposer, each bound to this slot and its own lane with
    /// valid signatures. Availability behind a positive root is not checked
    pub(crate) fn is_valid(
        &self,
        slot: Slot,
        num_lanes: usize,
        header_auth: &HeaderAuth,
        validator_data: &ValidatorData,
    ) -> bool {
        if self.0.size() != num_lanes || self.slot() != slot {
            return false;
        }

        let mut certs = self.0.as_ref().into_iter();
        certs.all(|cert| cert.verify(header_auth, validator_data))
    }

    /// The paper's *fast metablock*: every entry a `FastQc`
    pub(crate) fn is_fast(&self) -> bool {
        self.0
            .as_ref()
            .into_iter()
            .all(|cert| matches!(cert, CertifiedEntry::FastQc(_)))
    }

    /// `entries(B)`, in increasing order of lane
    pub(crate) fn entries(&self) -> Lanes<Entry> {
        self.0.as_ref().map(CertifiedEntry::entry).into_owned()
    }

    pub(crate) fn certified_entries(&self) -> Lanes<&CertifiedEntry> {
        self.0.as_ref()
    }
}

impl Decodable for Metablock {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let block = Self(Lanes::decode(buf)?);
        if !block.is_in_place() {
            return Err(alloy_rlp::Error::Custom("metablock entries out of place"));
        }
        Ok(block)
    }
}

impl Metablock {
    // a block has at least one lane, and all its entries name one slot
    pub(crate) fn slot(&self) -> Slot {
        self.0[Lane::new_unchecked(0)].scope().slot
    }

    // every entry names the block's slot and its own lane
    fn is_in_place(&self) -> bool {
        let slot = self.slot();
        let mut certs = self.0.as_ref().into_indexed_iter();
        certs.all(|(j, cert)| cert.scope() == ProposalScope::new(slot, j))
    }
}

impl ValidateInput for Metablock {
    type Context = ValidationContext;

    fn validate(&self, context: &Self::Context) -> bool {
        self.is_valid(
            context.slot,
            context.num_lanes,
            &context.header_auth,
            &context.validator_data,
        )
    }

    fn cert_optional(&self) -> bool {
        self.is_fast()
    }
}

impl Votable for Metablock {
    type Entries = Lanes<Entry>;

    fn entries(&self) -> Self::Entries {
        Metablock::entries(self)
    }
}

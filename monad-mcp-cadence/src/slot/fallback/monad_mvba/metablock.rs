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

use super::super::{
    super::{
        context::ChorusContext,
        fast::{CertifiedEntry, Entry},
        types::{Lane, Lanes, ProposalScope, Slot},
    },
    Metablock, ValidateInput, Votable,
};

impl Metablock {
    /// One entry per proposer, each bound to this slot and its own lane with
    /// valid signatures. Availability behind a positive root is not checked
    pub(crate) fn is_valid(&self, context: &ChorusContext) -> bool {
        if self.0.size() != context.num_lanes() || self.slot() != context.slot {
            return false;
        }

        let mut certs = self.0.as_ref().into_iter();
        certs.all(|cert| cert.verify(context))
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
    fn slot(&self) -> Slot {
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
    fn validate(&self, context: &ChorusContext) -> bool {
        self.is_valid(context)
    }

    fn fbcert_optional(&self) -> bool {
        self.is_fast()
    }
}

impl Votable for Metablock {
    type Entries = Lanes<Entry>;

    fn entries(&self) -> Self::Entries {
        Metablock::entries(self)
    }
}

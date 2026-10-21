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

use super::{
    super::{
        super::{
            fast::{CertifiedEntry, Entry},
            types::{HeaderAuth, Lanes, ProposalScope, Slot, ValidatorData},
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
        if self.0.size() != num_lanes {
            return false;
        }

        self.0.as_ref().into_indexed_iter().all(|(j, cert)| {
            let scope = ProposalScope::new(slot, j);
            cert.verify(scope, header_auth, validator_data)
        })
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
        self.0.as_ref().map(CertifiedEntry::entry)
    }

    pub(crate) fn certified_entries(&self) -> Lanes<&CertifiedEntry> {
        self.0.as_ref()
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

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

use std::sync::Arc;

use super::{
    fast::{CertifiedEntry, EnterFallbackCert},
    types::{KeyPair, Slot, TotalProposalMap, ValidatorData},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct FallbackRound(u64);

#[derive(Clone)]
pub(crate) struct FallbackPath {
    slot: Slot,
    round: FallbackRound,

    input: MVBAInputs,

    // using Arc to avoid lifetime issues.
    key: Arc<KeyPair>,
    validator_data: Arc<ValidatorData>,
}

impl FallbackPath {
    pub(crate) fn new(
        slot: Slot,
        key: Arc<KeyPair>,
        validator_data: Arc<ValidatorData>,
        input: MVBAInputs,
    ) -> Self {
        Self {
            slot,
            round: FallbackRound(0),
            key,
            validator_data,
            input,
        }
    }

    pub(crate) fn on_tick(&mut self) {
        todo!()
    }
}

pub(crate) type PartialBlock = TotalProposalMap<CertifiedEntry>;

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct MVBAInputs {
    pub enter_fallback_cert: EnterFallbackCert,
    pub block: PartialBlock,
}

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

use monad_mcp_common::SlotLifecycle;

use super::{election::Election, env::ValidatorData, handle::EpochHandle};

// Decides the epochs of the chain and gives out their handles.
pub struct EpochManager {
    genesis: EpochHandle,
}

impl EpochManager {
    // A manager with one epoch that never ends.
    pub fn new_fixed(
        validators: ValidatorData,
        election: impl Election + Send + Sync + 'static,
    ) -> Self {
        Self {
            genesis: EpochHandle::new(validators, election),
        }
    }

    pub fn genesis(&self) -> EpochHandle {
        self.genesis.clone()
    }

    // todo: seal the next epoch when the slots near the cap of the newest one
    pub fn handle_lifecycle(&mut self, _event: SlotLifecycle) {}

    // The next sealed epoch. None always, because this manager never rotates.
    pub fn poll(&mut self) -> Option<EpochHandle> {
        None
    }
}

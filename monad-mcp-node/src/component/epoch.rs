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

use monad_mcp_common::Timestamp;

use super::Component;
use crate::{
    cadence::SlotLifecycle,
    epoch::{EpochHandle, EpochManager},
};

impl Component for EpochManager {
    type Input = SlotLifecycle;
    // the next sealed epoch
    type Output = EpochHandle;

    fn handle(&mut self, _now: Timestamp, event: SlotLifecycle) {
        self.handle_lifecycle(event);
    }

    fn poll(&mut self) -> Option<EpochHandle> {
        EpochManager::poll(self)
    }
}

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

pub mod mempool;
pub mod random;

use bytes::Bytes;
use monad_mcp_common::{ProposalScope, SlotLifecycle, Timestamp};

pub use self::{
    mempool::{Mempool, MempoolConfig, MempoolInput, MempoolSource},
    random::RandomSource,
};

pub trait ProposalSource {
    // an input that only this source reads
    type LocalIn;

    fn handle_lifecycle(&mut self, event: SlotLifecycle);
    fn handle_input(&mut self, input: Self::LocalIn, now: Timestamp);

    fn seal(&mut self, scope: ProposalScope, now: Timestamp) -> Bytes;
}

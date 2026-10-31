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

//! The container types of the Cadence crates. Each type documents its
//! iteration order as part of its contract, so what a node sends never
//! depends on a hasher (see `monad-mcp-node/docs/determinism.md`).
//!
//! Which to use:
//!
//! - Validators of one epoch: [`ValidatorIndex`], resolved once from a
//!   `NodeId` where a sender is checked against the validator set, then
//!   [`ValidatorSet`] (a fixed-size bitset: signers, voters, holders) and
//!   [`ValidatorMap`] (a value for some validators: votes). Both iterate in
//!   index order.
//! - Dense state over another small index domain, such as a chunk
//!   assignment's nodes: [`IndexVec`], typed by the index, so indices of
//!   different domains cannot be mixed up.
//! - Lookups by any other key: [`FastHashMap`] and [`FastHashSet`]
//!   (rapidhash), or [`SipHashMap`] and [`SipHashSet`] where an adversary
//!   chooses the keys. The type name states the choice; iteration is only
//!   offered as `*_unordered`, for uses whose result does not depend on
//!   order. The std types are disallowed in these crates by clippy.
//! - A list that is only iterated: a `Vec`, in the order it is built.
//! - A meaningful order or range queries: a `BTreeMap`/`BTreeSet`, which
//!   cost more than the above.
//! - A pseudo-random order (who is served or asked first): [`OrderKey`], an
//!   explicit keyed permutation. Never a hash table's order: it is not
//!   portable between x86 and arm, nor across versions, even with a fixed
//!   seed.

mod hash;
mod index_vec;
mod order;
mod validators;

pub use self::{
    hash::{
        FastHashMap, FastHashSet, FastHashState, LookupMap, LookupSet, SipHashMap, SipHashSet,
        SipHashState,
    },
    index_vec::{Idx, IndexVec},
    order::OrderKey,
    validators::{MAX_VALIDATORS, ValidatorIndex, ValidatorMap, ValidatorSet},
};

pub mod validator_set {
    pub use super::validators::Iter;
}

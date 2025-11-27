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

use std::collections::BTreeMap;

use monad_crypto::certificate_signature::{CertificateKeyPair, PubKey};
use monad_types::NodeId;

/// Map validator NodeId to its Certificate PubKey
#[derive(Clone)]
pub struct ValidatorMapping<PT: PubKey, VKT: CertificateKeyPair> {
    pub map: BTreeMap<NodeId<PT>, VKT::PubKeyType>,
}

impl<PT: PubKey, VKT: CertificateKeyPair> ValidatorMapping<PT, VKT> {
    pub fn new(iter: impl IntoIterator<Item = (NodeId<PT>, VKT::PubKeyType)>) -> Self {
        Self {
            map: iter.into_iter().collect(),
        }
    }
}

impl<PT: PubKey, VKT: CertificateKeyPair> IntoIterator for ValidatorMapping<PT, VKT> {
    type Item = (NodeId<PT>, VKT::PubKeyType);
    type IntoIter = std::collections::btree_map::IntoIter<NodeId<PT>, VKT::PubKeyType>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

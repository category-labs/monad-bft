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

use monad_crypto::certificate_signature::PubKey;
use monad_types::{deserialize_pubkey, serialize_pubkey};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct BlockSyncPeersConfig<P: PubKey> {
    #[serde(bound = "P:PubKey")]
    pub peers: Vec<SyncPeerIdentityConfig<P>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct StateSyncPeersConfig<P: PubKey> {
    pub expand_to_group: bool,
    #[serde(bound = "P:PubKey")]
    pub init_peers: Vec<SyncPeerIdentityConfig<P>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SyncPeerIdentityConfig<P: PubKey> {
    #[serde(serialize_with = "serialize_pubkey::<_, P>")]
    #[serde(deserialize_with = "deserialize_pubkey::<_, P>")]
    #[serde(bound = "P:PubKey")]
    pub secp256k1_pubkey: P,
}

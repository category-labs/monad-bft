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

//! Wire protocol and client for the SEV-SNP enclave signer.
//!
//! The node holds private keys as `Local | Remote` enums; the `Remote` variant
//! carries a [`EnclaveSigner`] that talks to a signer service inside a
//! confidential VM over a [`Transport`] (AF_VSOCK in production, a unix socket
//! for laptop development).
//!
//! This crate intentionally knows nothing about the concrete key types — it
//! moves opaque byte arrays (digests, signatures, pubkeys) so that `monad-secp`
//! and `monad-bls` can depend on it without a dependency cycle.

pub mod client;
pub mod protocol;
pub mod transport;

pub use client::{ProvisionBundle, ProvisionRequest, Pubkeys, EnclaveSigner};
pub use protocol::{Op, ProtoError, BLS_PREFIXED_MAX, SECP_DIGEST_LEN, SECP_SIG_LEN};
pub use transport::{Transport, TransportConfig};

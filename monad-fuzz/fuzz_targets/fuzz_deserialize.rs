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

// Fuzz runner config:
//
// CORPUS_FILTER=*.deserialize.bin
// TIMEOUT_QUICK=5m
//
// Environments:
//
// AFL_HANG_TMOUT=100
// AFL_EXIT_ON_TIME=300000

use bytes::Bytes;
use monad_bls::BlsSignatureCollection;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::EthExecutionProtocol;
use monad_raptorcast::message::InboundRouterMessage;
use monad_secp::SecpSignature;
use monad_state::MonadMessage;

type SignatureType = SecpSignature;
type SignatureCollection = BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
type ExecutionProtocol = EthExecutionProtocol;
type Message = MonadMessage<SignatureType, SignatureCollection, ExecutionProtocol>;

fn main() {
    afl::fuzz!(|data: &[u8]| {
        let app_message = Bytes::copy_from_slice(data);
        let _ = InboundRouterMessage::<Message, SignatureType>::try_deserialize(&app_message);
    });
}

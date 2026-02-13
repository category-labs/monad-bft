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
// CORPUS_FILTER=*.parse_message.bin
// TIMEOUT_QUICK=5m
//
// Environments:
//
// AFL_HANG_TMOUT=100
// AFL_EXIT_ON_TIME=300000
// AFL_INPUT_LEN_MAX=1500

use bytes::Bytes;
use monad_raptorcast::{
    parser::legacy::{
        parse_message as old_parse_message, ChunkSignatureVerifier as OldSigVerifier,
    },
    udp::{
        parse_message as new_parse_message, ChunkSignatureVerifier as NewSigVerifier,
        MessageValidationError,
    },
};
use monad_secp::mock::MockSecpSignature;

fn main() {
    use MessageValidationError::*;

    afl::fuzz!(|data: &[u8]| {
        let payload = Bytes::copy_from_slice(data);

        let mut old_sig_cache = OldSigVerifier::<MockSecpSignature>::new().with_cache(1);
        let mut new_sig_cache = NewSigVerifier::<MockSecpSignature>::new().with_cache(1);

        let old = old_parse_message::<MockSecpSignature, _>(
            &mut old_sig_cache,
            payload.clone(),
            u64::MAX,
            |_| true,
        );

        let new = new_parse_message::<MockSecpSignature, _>(
            &mut new_sig_cache,
            payload,
            u64::MAX,
            |_| true,
        );

        match (old, new) {
            // known discrepancies due to different ordering of checks
            (Err(InvalidSignature), Err(TooShort))
            | (Err(InvalidSignature), Err(InvalidMerkleProof))
            | (Err(InvalidSignature), Err(InvalidChunkId))
            | (Err(InvalidTreeDepth), Err(TooShort))
            | (Err(TooLong), Err(TooShort))
            | (Err(InvalidMerkleProof), Err(InvalidChunkId))
            | (Err(InvalidMerkleProof), Err(TooShort))
            | (Err(InvalidBroadcastBits(_)), Err(TooShort))
            | (Err(InvalidBroadcastBits(_)), Err(TooLong))
            | (Err(InvalidBroadcastBits(_)), Err(InvalidTreeDepth)) => return,

            (old, new) => {
                // otherwise the two implementations should agree
                assert_eq!(old, new);
            }
        }
    });
}

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
use monad_raptorcast::udp::{parse_message, ChunkSignatureVerifier};
use monad_secp::mock::MockSecpSignature;

fn main() {
    afl::fuzz!(|data: &[u8]| {
        let mut sig_cache = ChunkSignatureVerifier::<MockSecpSignature>::new().with_cache(1);
        let payload = Bytes::copy_from_slice(data);
        let _ = parse_message::<MockSecpSignature, _>(&mut sig_cache, payload, u64::MAX, |_| true);
    });
}

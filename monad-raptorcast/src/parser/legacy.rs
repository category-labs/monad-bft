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

// Legacy packet parser implementation.

use bytes::Bytes;
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    hasher::{Hasher, HasherType},
    signing_domain,
};
use monad_merkle::{MerkleHash, MerkleProof};
use monad_types::{Epoch, Round};

use crate::{
    message::MAX_MESSAGE_SIZE,
    packet::assembler::HEADER_LEN,
    parser::signature_verifier,
    udp::{
        GroupId, MessageValidationError, ValidatedMessage, MAX_MERKLE_TREE_DEPTH, MAX_REDUNDANCY,
        MAX_VALIDATOR_SET_SIZE, MIN_MERKLE_TREE_DEPTH,
    },
    util::{BroadcastMode, HexBytes},
    SIGNATURE_SIZE,
};

type SignatureCacheKey = [u8; HEADER_LEN + 20];
pub type ChunkSignatureVerifier<ST> =
    signature_verifier::SignatureVerifier<ST, SignatureCacheKey, signing_domain::RaptorcastChunk>;

pub fn parse_message<ST, F>(
    signature_verifier: &mut ChunkSignatureVerifier<ST>,
    message: Bytes,
    max_age_ms: u64,
    bypass_rate_limiter: F,
) -> Result<ValidatedMessage<CertificateSignaturePubKey<ST>>, MessageValidationError>
where
    ST: CertificateSignatureRecoverable,
    F: FnOnce(Epoch) -> bool,
{
    let mut cursor: Bytes = message.clone();
    let mut split_off = |mid| {
        if mid > cursor.len() {
            Err(MessageValidationError::TooShort)
        } else {
            Ok(cursor.split_to(mid))
        }
    };
    let cursor_signature = split_off(SIGNATURE_SIZE)?;
    let signature = <ST as CertificateSignature>::deserialize(&cursor_signature)
        .map_err(|_| MessageValidationError::InvalidSignature)?;

    let cursor_version = split_off(2)?;
    let version = u16::from_le_bytes(cursor_version.as_ref().try_into().expect("u16 is 2 bytes"));
    if version != 0 {
        return Err(MessageValidationError::UnknownVersion(version));
    }

    let cursor_broadcast_tree_depth = split_off(1)?[0];
    let broadcast = (cursor_broadcast_tree_depth & (1 << 7)) != 0;
    let secondary_broadcast = (cursor_broadcast_tree_depth & (1 << 6)) != 0;
    let tree_depth = cursor_broadcast_tree_depth & 0b0000_1111; // bottom 4 bits

    let broadcast_mode = match (broadcast, secondary_broadcast) {
        (true, false) => BroadcastMode::Primary,
        (false, true) => BroadcastMode::Secondary,
        (false, false) => BroadcastMode::Unspecified, // unicast or broadcast
        (true, true) => {
            return Err(MessageValidationError::InvalidBroadcastBits(0b11));
        }
    };

    if !(MIN_MERKLE_TREE_DEPTH..=MAX_MERKLE_TREE_DEPTH).contains(&tree_depth) {
        return Err(MessageValidationError::InvalidTreeDepth);
    }

    let cursor_group_id = split_off(8)?;
    let group_id = u64::from_le_bytes(cursor_group_id.as_ref().try_into().expect("u64 is 8 bytes"));
    let group_id = match broadcast_mode {
        BroadcastMode::Primary | BroadcastMode::Unspecified => GroupId::Primary(Epoch(group_id)),
        BroadcastMode::Secondary => GroupId::Secondary(Round(group_id)),
    };

    let cursor_unix_ts_ms = split_off(8)?;
    let unix_ts_ms = u64::from_le_bytes(
        cursor_unix_ts_ms
            .as_ref()
            .try_into()
            .expect("u64 is 8 bytes"),
    );

    ensure_valid_timestamp(unix_ts_ms, max_age_ms)?;

    let cursor_app_message_hash = split_off(20)?;
    let app_message_hash: HexBytes<20> = HexBytes(
        cursor_app_message_hash
            .as_ref()
            .try_into()
            .expect("Hash is 20 bytes"),
    );

    let cursor_app_message_len = split_off(4)?;
    let app_message_len = u32::from_le_bytes(
        cursor_app_message_len
            .as_ref()
            .try_into()
            .expect("u32 is 4 bytes"),
    ) as usize;

    if app_message_len > MAX_MESSAGE_SIZE {
        return Err(MessageValidationError::TooLong);
    };

    let proof_size: u16 = 20 * (u16::from(tree_depth) - 1);

    let mut merkle_proof = Vec::new();
    for _ in 0..tree_depth - 1 {
        let cursor_sibling = split_off(20)?;
        let sibling =
            MerkleHash::try_from(cursor_sibling.as_ref()).expect("MerkleHash is 20 bytes");
        merkle_proof.push(sibling);
    }

    let cursor_recipient = split_off(20)?;
    let recipient_hash: HexBytes<20> = HexBytes(
        cursor_recipient
            .as_ref()
            .try_into()
            .expect("Hash is 20 bytes"),
    );

    let cursor_merkle_idx = split_off(1)?[0];
    let merkle_proof = MerkleProof::new_from_leaf_idx(merkle_proof, cursor_merkle_idx)
        .ok_or(MessageValidationError::InvalidMerkleProof)?;

    let _cursor_reserved = split_off(1)?;

    let cursor_chunk_id = split_off(2)?;
    let chunk_id = u16::from_le_bytes(cursor_chunk_id.as_ref().try_into().expect("u16 is 2 bytes"));

    let cursor_payload = cursor;
    let symbol_len = cursor_payload.len();
    if symbol_len == 0 {
        // handle the degenerate case
        return Err(MessageValidationError::TooShort);
    }

    let chunk_id_range = match broadcast_mode {
        BroadcastMode::Unspecified | BroadcastMode::Secondary => {
            valid_chunk_id_range(app_message_len, symbol_len)?
        }
        BroadcastMode::Primary => {
            // only perform a basic sanity check here. more precise
            // check of chunk_id is in decoding.rs when the validator
            // set is available.
            valid_chunk_id_range_raptorcast(app_message_len, symbol_len, MAX_VALIDATOR_SET_SIZE)?
        }
    };
    if !chunk_id_range.contains(&(chunk_id as usize)) {
        return Err(MessageValidationError::InvalidChunkId);
    }

    let leaf_hash = {
        let mut hasher = HasherType::new();
        hasher.update(
            &message[HEADER_LEN + proof_size as usize..
                // HEADER_LEN as usize
                //     + proof_size as usize
                //     + CHUNK_HEADER_LEN as usize
                //     + payload_len as usize
            ],
        );
        hasher.hash()
    };
    let root = merkle_proof
        .compute_root(&leaf_hash)
        .ok_or(MessageValidationError::InvalidMerkleProof)?;
    let mut signed_over: SignatureCacheKey = [0_u8; HEADER_LEN + 20];
    // TODO can avoid this copy if necessary
    signed_over[..HEADER_LEN].copy_from_slice(&message[..HEADER_LEN]);
    signed_over[HEADER_LEN..].copy_from_slice(&root);

    let author = if let Some(author) = signature_verifier.load_cached(&signed_over) {
        author
    } else {
        let new_author = match group_id {
            GroupId::Primary(epoch) if bypass_rate_limiter(epoch) => {
                signature_verifier.verify_force(signature, &signed_over[SIGNATURE_SIZE..])?
            }
            _ => signature_verifier.verify(signature, &signed_over[SIGNATURE_SIZE..])?,
        };
        signature_verifier.save_cache(signed_over, new_author);
        new_author
    };

    Ok(ValidatedMessage {
        message,
        author,
        group_id,
        unix_ts_ms,
        app_message_hash,
        app_message_len: app_message_len as u32,
        broadcast_mode,
        recipient_hash,
        chunk_id,
        chunk: cursor_payload,
    })
}

fn ensure_valid_timestamp(unix_ts_ms: u64, max_age_ms: u64) -> Result<(), MessageValidationError> {
    let current_time_ms = if let Ok(current_time_elapsed) = std::time::UNIX_EPOCH.elapsed() {
        current_time_elapsed.as_millis() as u64
    } else {
        tracing::warn!("system time is before unix epoch, ignoring timestamp");
        return Ok(());
    };
    let delta = (current_time_ms as i64).saturating_sub(unix_ts_ms as i64);
    if delta.unsigned_abs() > max_age_ms {
        Err(MessageValidationError::InvalidTimestamp {
            timestamp: unix_ts_ms,
            max: max_age_ms,
            delta,
        })
    } else {
        Ok(())
    }
}

fn valid_chunk_id_range_raptorcast(
    app_message_len: usize,
    symbol_len: usize,
    num_validators: usize,
) -> Result<std::ops::Range<usize>, MessageValidationError> {
    if symbol_len == 0 {
        return Err(MessageValidationError::TooShort);
    }
    let base_chunks = app_message_len.div_ceil(symbol_len);
    let rounding_chunks = num_validators;
    let num_chunks = MAX_REDUNDANCY
        .scale(base_chunks)
        .ok_or(MessageValidationError::TooLong)?
        + rounding_chunks;
    Ok(0..num_chunks)
}

fn valid_chunk_id_range(
    app_message_len: usize,
    symbol_len: usize,
) -> Result<std::ops::Range<usize>, MessageValidationError> {
    if symbol_len == 0 {
        return Err(MessageValidationError::TooShort);
    }
    let base_chunks = app_message_len.div_ceil(symbol_len);
    let num_chunks = MAX_REDUNDANCY
        .scale(base_chunks)
        .ok_or(MessageValidationError::TooLong)?;
    Ok(0..num_chunks)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, net::SocketAddr};

    use monad_crypto::{
        certificate_signature::CertificateSignaturePubKey, hasher::Hasher as HasherTrait,
    };
    use monad_dataplane::udp::DEFAULT_SEGMENT_SIZE;
    use monad_secp::{KeyPair, SecpSignature};
    use monad_types::{NodeId, Stake};
    use monad_validator::validator_set::ValidatorSet;

    use super::*;
    use crate::{
        packet::build_messages,
        udp::{GroupId, SIGNATURE_CACHE_SIZE},
        util::{BuildTarget, EpochValidators, Redundancy},
    };

    type SignatureType = SecpSignature;
    type KeyPairType = KeyPair;
    type TestSignatureVerifier = crate::udp::ChunkSignatureVerifier<SignatureType>;
    type LegacySignatureVerifier = super::ChunkSignatureVerifier<SignatureType>;

    fn validator_set() -> (
        KeyPairType,
        EpochValidators<CertificateSignaturePubKey<SignatureType>>,
        HashMap<NodeId<CertificateSignaturePubKey<SignatureType>>, SocketAddr>,
    ) {
        const NUM_KEYS: u8 = 100;
        let mut keys = (0_u8..NUM_KEYS)
            .map(|n| {
                let mut hasher = HasherType::new();
                hasher.update(n.to_le_bytes());
                let mut hash = hasher.hash();
                KeyPairType::from_bytes(&mut hash.0).unwrap()
            })
            .collect::<Vec<_>>();

        let valset = keys
            .iter()
            .map(|key| (NodeId::new(key.pubkey()), Stake::ONE))
            .collect();
        let validators = EpochValidators {
            validators: ValidatorSet::new_unchecked(valset),
        };

        let known_addresses = HashMap::new();
        (keys.pop().unwrap(), validators, known_addresses)
    }

    const EPOCH: Epoch = Epoch(5);
    const UNIX_TS_MS: u64 = 5;

    #[test]
    fn test_legacy_vs_new_parser_equivalence() {
        let (key, validators, known_addresses) = validator_set();
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);

        let app_message: Bytes = vec![1_u8; 1024 * 64].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE,
            app_message,
            Redundancy::from_u8(2),
            GroupId::Primary(EPOCH),
            UNIX_TS_MS,
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        );

        let mut legacy_verifier = LegacySignatureVerifier::new().with_cache(SIGNATURE_CACHE_SIZE);
        let mut new_verifier = TestSignatureVerifier::new().with_cache(SIGNATURE_CACHE_SIZE);

        for (_to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                let legacy_result =
                    super::parse_message(&mut legacy_verifier, message.clone(), u64::MAX, |_| true);
                let new_result =
                    crate::udp::parse_message(&mut new_verifier, message.clone(), u64::MAX, |_| {
                        true
                    });

                assert!(new_result.is_ok() && legacy_result.is_ok());
                assert_eq!(legacy_result, new_result);
            }
        }
    }
}

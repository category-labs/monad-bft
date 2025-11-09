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

use super::{common::*, errors::CryptoError};

pub const CONSTRUCTION: &[u8] = b"Noise_IKpsk2_secp256k1_AEGIS128L_BLAKE3";
pub const IDENTIFIER: &[u8] = b"authenticated udp v1 -- monad";
pub const LABEL_MAC1: &[u8] = b"mac1----";
pub const LABEL_COOKIE: &[u8] = b"cookie--";

pub fn encrypt_in_place(
    key: &CipherKey,
    nonce: &CipherNonce,
    data: &mut [u8],
    ad: &[u8],
) -> [u8; 16] {
    let cipher = aegis::aegis128l::Aegis128L::<16>::new(key.as_ref(), nonce.as_ref());
    cipher.encrypt_in_place(data, ad)
}

pub fn decrypt_in_place(
    key: &CipherKey,
    nonce: &CipherNonce,
    data: &mut [u8],
    tag: &[u8; 16],
    ad: &[u8],
) -> Result<(), CryptoError> {
    let cipher = aegis::aegis128l::Aegis128L::<16>::new(key.as_ref(), nonce.as_ref());
    cipher
        .decrypt_in_place(data, tag, ad)
        .map_err(|_| CryptoError::MacVerificationFailed)
}

#[macro_export]
macro_rules! hash {
    ($data:expr) => {{
        use $crate::protocol::common::HashOutput;
        HashOutput(blake3::hash($data).into())
    }};
    ($data1:expr, $data2:expr) => {{
        use $crate::protocol::common::HashOutput;
        let mut hasher = blake3::Hasher::new();
        hasher.update($data1);
        hasher.update($data2);
        HashOutput(hasher.finalize().into())
    }};
    ($data1:expr, $data2:expr, $data3:expr) => {{
        use $crate::protocol::common::HashOutput;
        let mut hasher = blake3::Hasher::new();
        hasher.update($data1);
        hasher.update($data2);
        hasher.update($data3);
        HashOutput(hasher.finalize().into())
    }};
}

#[macro_export]
macro_rules! keyed_hash {
    ($key:expr, $data:expr) => {{
        use $crate::protocol::common::HashOutput;
        HashOutput(blake3::keyed_hash($key, $data).into())
    }};
    ($key:expr, $data1:expr, $data2:expr) => {{
        use $crate::protocol::common::HashOutput;
        let mut hasher = blake3::Hasher::new_keyed($key);
        hasher.update($data1);
        hasher.update($data2);
        HashOutput(hasher.finalize().into())
    }};
    ($key:expr, $data1:expr, $data2:expr, $data3:expr) => {{
        use $crate::protocol::common::HashOutput;
        let mut hasher = blake3::Hasher::new_keyed($key);
        hasher.update($data1);
        hasher.update($data2);
        hasher.update($data3);
        HashOutput(hasher.finalize().into())
    }};
}

pub fn verify_keyed_hash(key: &HashOutput, data: &[u8], tag: &[u8; 16]) -> Result<(), CryptoError> {
    let computed: MacTag = keyed_hash!(key.as_ref(), data).into();
    if computed == *tag {
        Ok(())
    } else {
        Err(CryptoError::MacVerificationFailed)
    }
}

pub fn ecdh(private_key: &monad_secp::KeyPair, public_key: &monad_secp::PubKey) -> SharedSecret {
    SharedSecret(private_key.ecdh(public_key))
}

pub fn verify_mac1<M: crate::messages::MacMessage>(
    message: &M,
    static_public: &monad_secp::PubKey,
) -> Result<(), CryptoError> {
    let mac_key = hash!(LABEL_MAC1, &static_public.bytes_compressed());
    verify_keyed_hash(&mac_key, message.mac1_input(), message.mac1().as_ref())
}

pub fn verify_mac2<M: crate::messages::MacMessage>(
    message: &M,
    static_public: &monad_secp::PubKey,
    cookie: &[u8; 16],
) -> Result<(), CryptoError> {
    let cookie_key = hash!(LABEL_COOKIE, &static_public.bytes_compressed());
    let expected_mac2: MacTag =
        keyed_hash!(cookie_key.as_ref(), message.mac2_input(), cookie).into();
    if message.mac2() == &expected_mac2 {
        Ok(())
    } else {
        Err(CryptoError::MacVerificationFailed)
    }
}

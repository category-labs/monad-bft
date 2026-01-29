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

use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

use monad_bls::BlsKeyPair;
use monad_secp::KeyPair;
use rand::{rngs::OsRng, CryptoRng, RngCore};
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::{
    checksum_module::{ChecksumError, ChecksumHash},
    cipher_module::{Aes128Params, CipherModule, CipherParams},
    hex_string::{deserialize_bytes_from_hex_string, serialize_bytes_to_hex_string},
    kdf_module::{KDFError, KDFModule, KDFParams, ScryptParams},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(try_from = "u32", into = "u32")]
pub enum KeystoreVersion {
    #[default]
    Legacy = 1,
    DirectIkm = 2,
}

impl TryFrom<u32> for KeystoreVersion {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(KeystoreVersion::Legacy),
            2 => Ok(KeystoreVersion::DirectIkm),
            _ => Err(format!("Unknown keystore version: {}", value)),
        }
    }
}

impl From<KeystoreVersion> for u32 {
    fn from(version: KeystoreVersion) -> Self {
        version as u32
    }
}

impl std::fmt::Display for KeystoreVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeystoreVersion::Legacy => write!(f, "v1 (Legacy)"),
            KeystoreVersion::DirectIkm => write!(f, "v2 (Direct IKM)"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Keystore {
    #[serde(default)]
    pub version: KeystoreVersion,
    #[serde(
        serialize_with = "serialize_bytes_to_hex_string",
        deserialize_with = "deserialize_bytes_from_hex_string"
    )]
    pub ciphertext: Vec<u8>,
    #[serde(
        serialize_with = "serialize_bytes_to_hex_string",
        deserialize_with = "deserialize_bytes_from_hex_string"
    )]
    pub checksum: Vec<u8>,
    #[serde(flatten)]
    pub crypto: CryptoModules,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CryptoModules {
    pub cipher: CipherModule,
    pub kdf: KDFModule,
    pub hash: ChecksumHash,
}

#[derive(Debug)]
pub enum KeystoreError {
    InvalidJSONFormat,
    KDFError(KDFError),
    ChecksumError(ChecksumError),
    FileIOError(std::io::Error),
}

impl From<serde_json::Error> for KeystoreError {
    fn from(_err: serde_json::Error) -> KeystoreError {
        KeystoreError::InvalidJSONFormat
    }
}

impl From<std::io::Error> for KeystoreError {
    fn from(err: std::io::Error) -> KeystoreError {
        KeystoreError::FileIOError(err)
    }
}

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct KeystoreSecret(Vec<u8>);

impl KeystoreSecret {
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn to_bls(mut self, version: KeystoreVersion) -> Result<BlsKeyPair, KeystoreError> {
        match version {
            KeystoreVersion::Legacy => {
                BlsKeyPair::from_bytes(self.as_mut()).map_err(|_| KeystoreError::InvalidJSONFormat)
            }
            KeystoreVersion::DirectIkm => {
                BlsKeyPair::from_ikm(self.as_mut()).map_err(|_| KeystoreError::InvalidJSONFormat)
            }
        }
    }

    pub fn to_secp(mut self, version: KeystoreVersion) -> Result<KeyPair, KeystoreError> {
        match version {
            KeystoreVersion::Legacy => {
                KeyPair::from_bytes(self.as_mut()).map_err(|_| KeystoreError::InvalidJSONFormat)
            }
            KeystoreVersion::DirectIkm => {
                KeyPair::from_ikm(self.as_mut()).map_err(|_| KeystoreError::InvalidJSONFormat)
            }
        }
    }
}

impl From<Vec<u8>> for KeystoreSecret {
    fn from(data: Vec<u8>) -> Self {
        Self::new(data)
    }
}

impl AsRef<[u8]> for KeystoreSecret {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsMut<[u8]> for KeystoreSecret {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl CryptoModules {
    pub fn new(str: &str) -> Result<Self, KeystoreError> {
        serde_json::from_str(str).map_err(|_| KeystoreError::InvalidJSONFormat)
    }

    // Default parameters if cryptographic modules not provided
    pub fn create_default<R: RngCore + CryptoRng>(rng: &mut R) -> Self {
        // create parameters
        let mut iv = vec![0u8; 16];
        rng.fill_bytes(&mut iv);
        let mut salt = vec![0u8; 32];
        rng.fill_bytes(&mut salt);

        CryptoModules {
            cipher: CipherModule {
                cipher_function: String::from("AES_128_CTR"),
                params: CipherParams::Aes128Params(Aes128Params { iv }),
            },
            kdf: KDFModule {
                kdf_name: String::from("scrypt"),
                params: KDFParams::Scrypt(ScryptParams {
                    salt,
                    key_len: 32,
                    n: 262144,
                    r: 8,
                    p: 1,
                }),
            },
            hash: ChecksumHash::SHA256,
        }
    }

    /// Encrypt the secret with a passphrase Returns the corresponding
    /// ciphertext and checksum
    ///
    /// Password normalization
    /// https://eips.ethereum.org/EIPS/eip-2335#password-requirements
    /// > The password is a string of arbitrary unicode characters. The password
    /// > is first converted to its NFKD representation, then the control codes
    /// > (specified below) are stripped from the password and finally it is
    /// > UTF-8 encoded.
    pub fn encrypt(
        &self,
        secret: &[u8],
        password: &str,
    ) -> Result<(Vec<u8>, Vec<u8>), KeystoreError> {
        let password_nfkd = password
            .nfkd()
            .filter(|&c| !c.is_control())
            .collect::<String>();

        let mut encryption_key = self
            .kdf
            .derive_key(password_nfkd.as_bytes())
            .map_err(KeystoreError::KDFError)?;

        let ciphertext = self.cipher.encrypt(secret, &encryption_key);
        let checksum = self.hash.generate_checksum(&encryption_key, &ciphertext);

        encryption_key.zeroize();

        Ok((ciphertext, checksum))
    }

    /// Decrypt the ciphertext with the passphrase
    /// Returns the corresponding secret
    ///
    /// See [CryptoModules::encrypt] for password normalization
    pub fn decrypt(
        &self,
        ciphertext: &[u8],
        password: &str,
        checksum: &[u8],
    ) -> Result<KeystoreSecret, KeystoreError> {
        let password_nfkd = password
            .nfkd()
            .filter(|&c| !c.is_control())
            .collect::<String>();

        let mut decryption_key = self
            .kdf
            .derive_key(password_nfkd.as_bytes())
            .map_err(KeystoreError::KDFError)?;

        self.hash
            .verify_checksum(&decryption_key, ciphertext, checksum)
            .map_err(KeystoreError::ChecksumError)?;

        let result = self.cipher.decrypt(ciphertext, &decryption_key);
        decryption_key.zeroize();

        Ok(KeystoreSecret::new(result))
    }
}

impl Keystore {
    // Creates a new keystore json file by providing a secret
    pub fn create_keystore_json(
        keystore_secret: &[u8],
        password: &str,
        path: &Path,
    ) -> Result<(), KeystoreError> {
        Self::create_keystore_json_with_version(
            keystore_secret,
            password,
            path,
            KeystoreVersion::Legacy,
        )
    }

    // Creates a new keystore json file with specified version
    pub fn create_keystore_json_with_version(
        keystore_secret: &[u8],
        password: &str,
        path: &Path,
        version: KeystoreVersion,
    ) -> Result<(), KeystoreError> {
        let crypto_modules = CryptoModules::create_default(&mut OsRng);
        let (ciphertext, checksum) = crypto_modules.encrypt(keystore_secret, password)?;

        let keystore = Keystore {
            version,
            ciphertext,
            checksum,
            crypto: crypto_modules,
        };
        let contents = serde_json::to_string(&keystore)?;

        let mut file = File::create(path)?;
        file.write_all(contents.as_bytes())?;
        Ok(())
    }

    // Reads a keystore json file and obtain the corresponding keystore secret and version
    pub fn load_key_with_version(
        path: &Path,
        password: &str,
    ) -> Result<(KeystoreSecret, KeystoreVersion), KeystoreError> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let keystore: Keystore = serde_json::from_str(&contents)?;
        let keystore_secret =
            keystore
                .crypto
                .decrypt(&keystore.ciphertext, password, &keystore.checksum)?;

        Ok((keystore_secret, keystore.version))
    }

    pub fn load_bls_key(path: &Path, password: &str) -> Result<BlsKeyPair, KeystoreError> {
        let (keystore_secret, version) = Keystore::load_key_with_version(path, password)?;
        keystore_secret.to_bls(version)
    }

    pub fn load_secp_key(path: &Path, password: &str) -> Result<KeyPair, KeystoreError> {
        let (keystore_secret, version) = Keystore::load_key_with_version(path, password)?;
        keystore_secret.to_secp(version)
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use serde_json::json;

    use crate::keystore::{CryptoModules, Keystore};

    #[test]
    fn test_keystore_encrypt() {
        let keystore_json = json!({
            "kdf": {
                "kdf_name": "scrypt",
                "params": {
                    "salt": "563198eb58ea328782ae76a9127b3e58e7af73f93387d77a31d4322b45ba91be",
                    "key_len": 32,
                    "N": 262144,
                    "r": 8,
                    "p": 1
                }
            },
            "hash": "SHA256",
            "cipher": {
                "cipher_function": "AES_128_CTR",
                "params": {
                    "iv": "af29e87a9fd4699e335f718d03e30ed9"
                }
            }
        });
        let sk = "6dd19c802af753f5b89d11becba0aeafe91493e14ade082c3af4e5797cae29b5";
        let password = "".to_owned();

        let serialized_json = keystore_json.to_string();
        let crypto_params = CryptoModules::new(&serialized_json).expect("invalid json format");

        let result = crypto_params
            .encrypt(&hex::decode(sk).unwrap(), &password)
            .expect("encryption failed");
        let expected_ciphertext =
            "c2755caf2c080f939f2b23ce55e6dfbf7430ac36593d778206a60f968baa7055";
        let expected_checksum = "fc52d979838891dbb18fb42e116403582b20dc4c40169d5ceec99419079eb6c4";

        assert!(hex::encode(result.0) == expected_ciphertext);
        assert!(hex::encode(result.1) == expected_checksum);
    }

    #[test]
    fn test_keystore_decrypt() {
        let keystore_json = json!({
            "kdf": {
                "kdf_name": "scrypt",
                "params": {
                    "salt": "563198eb58ea328782ae76a9127b3e58e7af73f93387d77a31d4322b45ba91be",
                    "key_len": 32,
                    "N": 262144,
                    "r": 8,
                    "p": 1
                }
            },
            "hash": "SHA256",
            "cipher": {
                "cipher_function": "AES_128_CTR",
                "params": {
                    "iv": "af29e87a9fd4699e335f718d03e30ed9"
                }
            }
        });

        let password = "".to_owned();
        let ciphertext = "c2755caf2c080f939f2b23ce55e6dfbf7430ac36593d778206a60f968baa7055";
        let checksum = "fc52d979838891dbb18fb42e116403582b20dc4c40169d5ceec99419079eb6c4";
        let expected_sk = "6dd19c802af753f5b89d11becba0aeafe91493e14ade082c3af4e5797cae29b5";

        let serialized_json = keystore_json.to_string();
        let crypto_params = CryptoModules::new(&serialized_json).expect("invalid json format");

        let sk = crypto_params
            .decrypt(
                &hex::decode(ciphertext).unwrap(),
                &password,
                &hex::decode(checksum).unwrap(),
            )
            .expect("decryption failed");

        assert!(hex::encode(sk) == expected_sk);
    }

    #[test]
    fn test_keystore_file() {
        let file_path = Path::new("./test-keys");
        let secret = "6dd19c802af753f5b89d11becba0aeafe91493e14ade082c3af4e5797cae29b5";
        let password = "testing";

        // create keystore json file
        let result =
            Keystore::create_keystore_json(&hex::decode(secret).unwrap(), password, file_path);
        assert!(result.is_ok());

        // decrypt keystore json file and verify version
        let (retrieved_secret, version) =
            Keystore::load_key_with_version(file_path, password).unwrap();
        assert!(hex::encode(retrieved_secret) == secret);
        assert_eq!(version, crate::keystore::KeystoreVersion::Legacy);
    }
}

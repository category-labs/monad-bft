use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

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

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Keystore {
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

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct SecretKey(Vec<u8>);

impl SecretKey {
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for SecretKey {
    fn from(data: Vec<u8>) -> Self {
        Self::new(data)
    }
}

impl AsRef<[u8]> for SecretKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsMut<[u8]> for SecretKey {
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

    /// Encrypt the private key with a passphrase Returns the corresponding
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
        private_key: &[u8],
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

        let ciphertext = self.cipher.encrypt(private_key, &encryption_key);
        let checksum = self.hash.generate_checksum(&encryption_key, &ciphertext);

        encryption_key.zeroize();

        Ok((ciphertext, checksum))
    }

    /// Decrypt the ciphertext with the passphrase
    /// Returns the corresponding private key
    ///
    /// See [CryptoModules::encrypt] for password normalization
    pub fn decrypt(
        &self,
        ciphertext: &[u8],
        password: &str,
        checksum: &[u8],
    ) -> Result<SecretKey, KeystoreError> {
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

        Ok(SecretKey::new(result))
    }
}

impl Keystore {
    // Creates a new keystore json file by providing a private key
    pub fn create_keystore_json(
        private_key: &[u8],
        password: &str,
        path: &Path,
    ) -> Result<(), KeystoreError> {
        let crypto_modules = CryptoModules::create_default(&mut OsRng);
        let (ciphertext, checksum) = crypto_modules.encrypt(private_key, password)?;

        let keystore = Keystore {
            ciphertext,
            checksum,
            crypto: crypto_modules,
        };
        let contents = serde_json::to_string(&keystore)?;

        let mut file = File::create(path)?;
        file.write_all(contents.as_bytes())?;
        Ok(())
    }

    // Reads a keystore json file and obtain the corresponding private key
    pub fn load_key(path: &Path, password: &str) -> Result<SecretKey, KeystoreError> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let keystore: Keystore = serde_json::from_str(&contents)?;
        let private_key =
            keystore
                .crypto
                .decrypt(&keystore.ciphertext, password, &keystore.checksum)?;

        Ok(private_key)
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
        let private_key = "6dd19c802af753f5b89d11becba0aeafe91493e14ade082c3af4e5797cae29b5";
        let password = "testing";

        // create keystore json file
        let result =
            Keystore::create_keystore_json(&hex::decode(private_key).unwrap(), password, file_path);
        assert!(result.is_ok());

        // decrypt keystore json file
        let retrieved_key = Keystore::load_key(file_path, password).unwrap();
        assert!(hex::encode(retrieved_key) == private_key);
    }
}

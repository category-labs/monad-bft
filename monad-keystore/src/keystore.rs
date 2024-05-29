use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

use rand::{RngCore, SeedableRng};
use serde::{Deserialize, Serialize};

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
    ReadOrWriteFileError,
}

impl From<serde_json::Error> for KeystoreError {
    fn from(_err: serde_json::Error) -> KeystoreError {
        KeystoreError::InvalidJSONFormat
    }
}

impl From<std::io::Error> for KeystoreError {
    fn from(_err: std::io::Error) -> KeystoreError {
        KeystoreError::ReadOrWriteFileError
    }
}

impl CryptoModules {
    pub fn new(str: &str) -> Result<Self, KeystoreError> {
        serde_json::from_str(str).map_err(|_| KeystoreError::InvalidJSONFormat)
    }

    // Default parameters if cryptographic modules not provided
    pub fn create_default(seed: u64) -> Self {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

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

    // Encrypt the private key with a passphrase
    // Returns the corresponding ciphertext and checksum
    pub fn encrypt(
        &self,
        private_key: &[u8],
        password: &String,
    ) -> Result<(Vec<u8>, Vec<u8>), KeystoreError> {
        let encryption_key = self
            .kdf
            .derive_key(password.as_bytes())
            .map_err(KeystoreError::KDFError)?;

        let ciphertext = self.cipher.encrypt(private_key, &encryption_key);
        let checksum = self.hash.generate_checksum(&encryption_key, &ciphertext);

        Ok((ciphertext, checksum))
    }

    // Decrypt the ciphertext with the passphrase
    // Returns the corresponding private key
    pub fn decrypt(
        &self,
        ciphertext: &[u8],
        password: &String,
        checksum: &[u8],
    ) -> Result<Vec<u8>, KeystoreError> {
        let decryption_key = self
            .kdf
            .derive_key(password.as_bytes())
            .map_err(KeystoreError::KDFError)?;

        self.hash
            .verify_checksum(&decryption_key, ciphertext, checksum)
            .map_err(KeystoreError::ChecksumError)?;

        Ok(self.cipher.decrypt(ciphertext, &decryption_key))
    }
}

impl Keystore {
    // Creates a new keystore json file by providing a private key
    pub fn create_keystore_json(
        private_key: &[u8],
        password: &str,
        path: &Path,
    ) -> Result<(), KeystoreError> {
        let crypto_modules = CryptoModules::create_default(rand::random());
        let (ciphertext, checksum) = crypto_modules.encrypt(private_key, &password.to_string())?;

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
    pub fn load_key(path: &Path, password: &str) -> Result<Vec<u8>, KeystoreError> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let keystore: Keystore = serde_json::from_str(&contents)?;
        let private_key = keystore.crypto.decrypt(
            &keystore.ciphertext,
            &password.to_string(),
            &keystore.checksum,
        )?;

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

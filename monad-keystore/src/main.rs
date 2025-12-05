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

/// A placeholder CLI tool to generate the keystore json file
/// The key generation tool is unaudited
/// DO NOT USE IN PRODUCTION YET
/// `cargo run -- --mode create --key-type [bls|secp] --keystore-path <path_for_file_to_be_created>`
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use rand::{rngs::OsRng, RngCore};

use crate::keystore::{Keystore, KeystoreSecret, KeystoreVersion};

pub mod checksum_module;
pub mod cipher_module;
pub mod hex_string;
pub mod kdf_module;
pub mod keystore;

#[derive(Parser)]
#[command(name = "monad-keystore", about, long_about = None, version = monad_version::version!())]
struct Args {
    #[command(subcommand)]
    mode: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create new random key
    Create {
        /// Path to write keystore file
        #[arg(long)]
        keystore_path: PathBuf,

        /// Keystore password
        #[arg(long)]
        password: String,

        /// Optionally print private and public key
        #[arg(long)]
        key_type: Option<KeyType>,
    },
    /// Recovers key from keystore
    Recover {
        /// Path to read keystore file
        #[arg(long)]
        keystore_path: PathBuf,

        /// Keystore password
        #[arg(long)]
        password: String,

        /// Optionally print private and public key
        #[arg(long)]
        key_type: Option<KeyType>,
    },
    /// Regenerate keystore from IKM
    Import {
        /// IKM in hex
        #[arg(long)]
        ikm: String,

        /// Path to write keystore file
        #[arg(long)]
        keystore_path: PathBuf,

        /// Keystore password
        #[arg(long)]
        password: String,

        /// Optionally print private and public key
        #[arg(long)]
        key_type: Option<KeyType>,
    },
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum KeyType {
    Secp,
    Bls,
}

fn run(args: Args) -> Result<(), String> {
    match args.mode {
        Commands::Create {
            keystore_path,
            password,
            key_type,
        } => {
            println!("It is recommended to generate key in air-gapped machine to be secure.");
            println!("This tool is currently not fit for production use.");

            let mut ikm = vec![0_u8; 32];
            OsRng.fill_bytes(&mut ikm);
            println!("Keep your IKM secure: {}", hex::encode(&ikm));

            let keystore_secret = KeystoreSecret::new(ikm);

            if let Some(key_type) = key_type {
                // print private and public key using version 2 approach
                match key_type {
                    KeyType::Bls => {
                        let bls_keypair = keystore_secret
                            .clone()
                            .to_bls(KeystoreVersion::DirectIkm)
                            .map_err(|e| format!("failed to create bls keypair: {:?}", e))?;
                        let private_key = bls_keypair.privkey_view();
                        let public_key = bls_keypair.pubkey();
                        println!("BLS private key: {}", private_key);
                        println!("BLS public key: {:?}", public_key);
                    }
                    KeyType::Secp => {
                        let secp_keypair = keystore_secret
                            .clone()
                            .to_secp(KeystoreVersion::DirectIkm)
                            .map_err(|e| format!("failed to create secp keypair: {:?}", e))?;
                        let private_key = secp_keypair.privkey_view();
                        let public_key = secp_keypair.pubkey();
                        println!("Secp private key: {}", private_key);
                        println!("Secp public key: {:?}", public_key);
                    }
                }
            }

            // generate keystore json file with version 2
            Keystore::create_keystore_json_with_version(
                keystore_secret.as_ref(),
                &password,
                &keystore_path,
                KeystoreVersion::DirectIkm,
            )
            .map_err(|e| format!("keystore file generation failed: {:?}", e))?;

            println!("Successfully generated keystore file.");
            Ok(())
        }
        Commands::Recover {
            keystore_path,
            password,
            key_type,
        } => {
            println!("Recovering secret from keystore file...");

            // recover keystore secret with version
            let (keystore_secret, version) =
                Keystore::load_key_with_version(&keystore_path, &password)
                    .map_err(|err| format!("unable to recover keystore secret: {:?}", err))?;

            println!("Keystore version: {}", version);
            println!("Keystore secret: {}", hex::encode(keystore_secret.as_ref()));

            if let Some(key_type) = key_type {
                // print public key based on key type and version
                match key_type {
                    KeyType::Bls => {
                        let bls_keypair = keystore_secret
                            .to_bls(version)
                            .map_err(|e| format!("failed to create bls keypair: {:?}", e))?;
                        let private_key = bls_keypair.privkey_view();
                        let public_key = bls_keypair.pubkey();
                        println!("BLS private key: {}", private_key);
                        println!("BLS public key: {:?}", public_key);
                    }
                    KeyType::Secp => {
                        let secp_keypair = keystore_secret
                            .to_secp(version)
                            .map_err(|e| format!("failed to create secp keypair: {:?}", e))?;
                        let private_key = secp_keypair.privkey_view();
                        let public_key = secp_keypair.pubkey();
                        println!("Secp private key: {}", private_key);
                        println!("Secp public key: {:?}", public_key);
                    }
                }
            }
            Ok(())
        }
        Commands::Import {
            ikm,
            keystore_path,
            password,
            key_type,
        } => {
            let ikm_hex = match ikm.strip_prefix("0x") {
                Some(hex) => hex,
                None => &ikm,
            };
            let ikm_vec =
                hex::decode(ikm_hex).map_err(|e| format!("failed to parse ikm as hex: {}", e))?;
            let keystore_secret = KeystoreSecret::new(ikm_vec);

            if let Some(key_type) = key_type {
                match key_type {
                    KeyType::Bls => {
                        let bls_keypair = keystore_secret
                            .clone()
                            .to_bls(KeystoreVersion::DirectIkm)
                            .map_err(|e| format!("failed to create bls keypair: {:?}", e))?;
                        let private_key = bls_keypair.privkey_view();
                        let public_key = bls_keypair.pubkey();
                        println!("BLS private key: {}", private_key);
                        println!("BLS public key: {:?}", public_key);
                    }
                    KeyType::Secp => {
                        let secp_keypair = keystore_secret
                            .clone()
                            .to_secp(KeystoreVersion::DirectIkm)
                            .map_err(|e| format!("failed to create secp keypair: {:?}", e))?;
                        let private_key = secp_keypair.privkey_view();
                        let public_key = secp_keypair.pubkey();
                        println!("Secp private key: {}", private_key);
                        println!("Secp public key: {:?}", public_key);
                    }
                }
            }

            Keystore::create_keystore_json_with_version(
                keystore_secret.as_ref(),
                &password,
                &keystore_path,
                KeystoreVersion::DirectIkm,
            )
            .map_err(|e| format!("keystore file generation failed: {:?}", e))?;

            println!("Successfully generated keystore file.");
            Ok(())
        }
    }
}

fn main() {
    let args = Args::parse();
    if let Err(e) = run(args) {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::signing_domain;
    use rstest::rstest;
    use tempfile::TempDir;

    use super::*;

    const TEST_PASSWORD: &str = "test_password";

    type TestSigningDomain = signing_domain::Tip;

    #[rstest]
    #[case::no_key_type(None)]
    #[case::bls_key_type(Some(KeyType::Bls))]
    #[case::secp_key_type(Some(KeyType::Secp))]
    fn test_create_keystore_contains_valid_key(#[case] key_type: Option<KeyType>) {
        let temp_dir = TempDir::new().unwrap();
        let keystore_path = temp_dir.path().join("test_keystore.json");

        let create_args = Args {
            mode: Commands::Create {
                keystore_path: keystore_path.clone(),
                password: TEST_PASSWORD.to_string(),
                key_type,
            },
        };
        run(create_args).unwrap();

        assert!(keystore_path.exists());

        let (keystore_secret, version) =
            Keystore::load_key_with_version(&keystore_path, TEST_PASSWORD).unwrap();

        if let Some(kt) = key_type {
            let test_message = b"test message";
            match kt {
                KeyType::Bls => {
                    let keypair = keystore_secret.to_bls(version).unwrap();
                    assert!(!keypair.privkey_view().to_string().is_empty());

                    let signature = keypair.sign::<TestSigningDomain>(test_message);
                    let pubkey = keypair.pubkey();
                    assert!(signature
                        .verify::<TestSigningDomain>(test_message, &pubkey)
                        .is_ok());
                }
                KeyType::Secp => {
                    let keypair = keystore_secret.to_secp(version).unwrap();
                    assert!(!keypair.privkey_view().to_string().is_empty());

                    let signature = keypair.sign::<TestSigningDomain>(test_message);
                    let pubkey = keypair.pubkey();
                    assert!(pubkey
                        .verify::<TestSigningDomain>(test_message, &signature)
                        .is_ok());
                }
            }
        }
    }

    #[rstest]
    #[case::no_key_type(None)]
    #[case::bls_key_type(Some(KeyType::Bls))]
    #[case::secp_key_type(Some(KeyType::Secp))]
    fn test_import_keystore_matches_created(#[case] key_type: Option<KeyType>) {
        let temp_dir = TempDir::new().unwrap();
        let created_path = temp_dir.path().join("created.json");
        let imported_path = temp_dir.path().join("imported.json");

        let create_args = Args {
            mode: Commands::Create {
                keystore_path: created_path.clone(),
                password: TEST_PASSWORD.to_string(),
                key_type,
            },
        };
        run(create_args).unwrap();

        let (secret, _) = Keystore::load_key_with_version(&created_path, TEST_PASSWORD).unwrap();
        let ikm_hex = hex::encode(secret.as_ref());

        let import_args = Args {
            mode: Commands::Import {
                ikm: ikm_hex,
                keystore_path: imported_path.clone(),
                password: TEST_PASSWORD.to_string(),
                key_type,
            },
        };
        run(import_args).unwrap();

        let (created_secret, _) =
            Keystore::load_key_with_version(&created_path, TEST_PASSWORD).unwrap();
        let (imported_secret, _) =
            Keystore::load_key_with_version(&imported_path, TEST_PASSWORD).unwrap();

        assert_eq!(created_secret.as_ref(), imported_secret.as_ref());
    }

    #[rstest]
    #[case::no_key_type(None)]
    #[case::bls_key_type(Some(KeyType::Bls))]
    #[case::secp_key_type(Some(KeyType::Secp))]
    fn test_recover_on_created_keystore(#[case] key_type: Option<KeyType>) {
        let temp_dir = TempDir::new().unwrap();
        let keystore_path = temp_dir.path().join("test_keystore.json");

        let create_args = Args {
            mode: Commands::Create {
                keystore_path: keystore_path.clone(),
                password: TEST_PASSWORD.to_string(),
                key_type: None,
            },
        };
        run(create_args).unwrap();

        let recover_args = Args {
            mode: Commands::Recover {
                keystore_path,
                password: TEST_PASSWORD.to_string(),
                key_type,
            },
        };

        let result = run(recover_args);
        assert!(result.is_ok());
    }
}

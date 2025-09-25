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
use zeroize::Zeroize;

use crate::keystore::{Keystore, KeystoreSecret, KeystoreVersion};

pub mod checksum_module;
pub mod cipher_module;
pub mod hex_string;
pub mod kdf_module;
pub mod keystore;

#[derive(Parser)]
#[command(name = "monad-keystore", about, long_about = None)]
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

fn main() {
    let args = Args::parse();
    let mode = args.mode;

    match mode {
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

            if let Some(key_type) = key_type {
                // print private and public key using version 2 approach
                let mut keystore_secret = KeystoreSecret::new(ikm.clone());
                match key_type {
                    KeyType::Bls => {
                        let bls_keypair =
                            keystore_secret.to_bls(KeystoreVersion::DirectIkm).unwrap();
                        let private_key = bls_keypair.privkey_view();
                        let public_key = bls_keypair.pubkey();
                        println!("BLS private key: {}", private_key);
                        println!("BLS public key: {:?}", public_key);
                    }
                    KeyType::Secp => {
                        let secp_keypair =
                            keystore_secret.to_secp(KeystoreVersion::DirectIkm).unwrap();
                        let private_key = secp_keypair.privkey_view();
                        let public_key = secp_keypair.pubkey();
                        println!("Secp private key: {}", private_key);
                        println!("Secp public key: {:?}", public_key);
                    }
                }
            }

            // generate keystore json file with version 2
            let result = Keystore::create_keystore_json_with_version(
                &ikm,
                &password,
                &keystore_path,
                KeystoreVersion::DirectIkm,
            );
            if result.is_ok() {
                println!("Successfully generated keystore file.");
            } else {
                println!("Keystore file generation failed, try again.");
            }
            ikm.zeroize();
        }
        Commands::Recover {
            keystore_path,
            password,
            key_type,
        } => {
            println!("Recovering secret from keystore file...");

            // recover keystore secret with version
            let result = Keystore::load_key_with_version(&keystore_path, &password);
            let (mut keystore_secret, version) = match result {
                Ok((keystore_secret, version)) => (keystore_secret, version),
                Err(err) => {
                    println!("Unable to recover keystore secret");
                    match err {
                        keystore::KeystoreError::InvalidJSONFormat => {
                            println!("Invalid JSON format")
                        }
                        keystore::KeystoreError::KDFError(kdf_err) => {
                            println!("KDFError {:?}", kdf_err)
                        }
                        keystore::KeystoreError::ChecksumError(chksum_err) => {
                            println!("ChecksumError {:?}", chksum_err)
                        }
                        keystore::KeystoreError::FileIOError(io_err) => {
                            println!("IO Error {:?}", io_err)
                        }
                    }
                    return;
                }
            };

            println!("Keystore version: {}", version);
            println!("Keystore secret: {}", hex::encode(keystore_secret.as_ref()));

            if let Some(key_type) = key_type {
                // print public key based on key type and version
                match key_type {
                    KeyType::Bls => {
                        let bls_keypair = keystore_secret.to_bls(version).unwrap();
                        let private_key = bls_keypair.privkey_view();
                        let public_key = bls_keypair.pubkey();
                        println!("BLS private key: {}", private_key);
                        println!("BLS public key: {:?}", public_key);
                    }
                    KeyType::Secp => {
                        let secp_keypair = keystore_secret.to_secp(version).unwrap();
                        let private_key = secp_keypair.privkey_view();
                        let public_key = secp_keypair.pubkey();
                        println!("Secp private key: {}", private_key);
                        println!("Secp public key: {:?}", public_key);
                    }
                }
            }
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
            let ikm_vec = hex::decode(ikm_hex).expect("failed to parse ikm as hex");
            let mut ikm: KeystoreSecret = ikm_vec.into();

            if let Some(key_type) = key_type {
                match key_type {
                    KeyType::Bls => {
                        let bls_keypair = ikm.to_bls(KeystoreVersion::DirectIkm).unwrap();
                        let private_key = bls_keypair.privkey_view();
                        let public_key = bls_keypair.pubkey();
                        println!("BLS private key: {}", private_key);
                        println!("BLS public key: {:?}", public_key);
                    }
                    KeyType::Secp => {
                        let secp_keypair = ikm.to_secp(KeystoreVersion::DirectIkm).unwrap();
                        let private_key = secp_keypair.privkey_view();
                        let public_key = secp_keypair.pubkey();
                        println!("Secp private key: {}", private_key);
                        println!("Secp public key: {:?}", public_key);
                    }
                }
            }

            let result = Keystore::create_keystore_json_with_version(
                ikm.as_ref(),
                &password,
                &keystore_path,
                KeystoreVersion::DirectIkm,
            );
            if result.is_ok() {
                println!("Successfully generated keystore file.");
            } else {
                println!("Keystore file generation failed, try again.");
            }
        }
    }
}

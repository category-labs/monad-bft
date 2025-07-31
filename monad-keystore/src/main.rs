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

use bip32::XPrv;
use bip39::{Language, Mnemonic, MnemonicType, Seed};
use clap::{Parser, Subcommand, ValueEnum};
use monad_bls::BlsKeyPair;
use monad_secp::KeyPair;
use zeroize::Zeroize;

use crate::keystore::{Keystore, SecretKey};

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

        /// Optionally print public key
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

        /// Optionally print public key
        #[arg(long)]
        key_type: Option<KeyType>,
    },
    /// Regenerate keystore from private key
    Import {
        /// Private key in hex
        #[arg(long)]
        private_key: String,

        /// Path to write keystore file
        #[arg(long)]
        keystore_path: PathBuf,

        /// Keystore password
        #[arg(long)]
        password: String,

        /// Optionally print public key
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

            // create a new randomly generated mnemonic phrase
            let mnemonic = Mnemonic::new(MnemonicType::Words12, Language::English);

            // get the HD wallet seed and the corresponding private key
            let seed = Seed::new(&mnemonic, "");
            let root_xprv = XPrv::new(seed.as_bytes()).expect("Failed to derive root key");
            let mut private_key = root_xprv.private_key().to_bytes();
            println!(
                "Keep your private key securely: {:?}",
                hex::encode(private_key)
            );

            if let Some(key_type) = key_type {
                // print public key
                print_public_key(&private_key, key_type);
            }

            // generate keystore json file
            let result = Keystore::create_keystore_json(&private_key, &password, &keystore_path);
            if result.is_ok() {
                println!("Successfully generated keystore file.");
            } else {
                println!("Keystore file generation failed, try again.");
            }
            private_key.zeroize();
        }
        Commands::Recover {
            keystore_path,
            password,
            key_type,
        } => {
            println!("Recovering private and public key from keystore file...");

            // recover private key
            let result = Keystore::load_key(&keystore_path, &password);
            let private_key = match result {
                Ok(private_key) => private_key,
                Err(err) => {
                    println!("Unable to recover private key");
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
            println!(
                "Keep your private key securely: {:?}",
                hex::encode(private_key.as_ref())
            );

            if let Some(key_type) = key_type {
                // print public key
                print_public_key(private_key.as_ref(), key_type);
            }
        }
        Commands::Import {
            private_key,
            keystore_path,
            password,
            key_type,
        } => {
            let private_key_hex = match private_key.strip_prefix("0x") {
                Some(hex) => hex,
                None => &private_key,
            };
            let private_key_vec =
                hex::decode(private_key_hex).expect("failed to parse private key as hex");
            let private_key: SecretKey = private_key_vec.into();

            if let Some(key_type) = key_type {
                // print public key
                print_public_key(private_key.as_ref(), key_type);
            }

            // generate keystore json file
            let result =
                Keystore::create_keystore_json(private_key.as_ref(), &password, &keystore_path);
            if result.is_ok() {
                println!("Successfully generated keystore file.");
            } else {
                println!("Keystore file generation failed, try again.");
            }
        }
    }
}

fn print_public_key(private_key: &[u8], key_type: KeyType) {
    match key_type {
        KeyType::Bls => {
            let bls_keypair = BlsKeyPair::from_bytes(private_key.to_vec());
            let pubkey = bls_keypair.unwrap().pubkey();
            println!("BLS public key: {:?}", pubkey);
        }
        KeyType::Secp => {
            let secp_keypair = KeyPair::from_bytes(&mut private_key.to_vec());
            let pubkey = secp_keypair.unwrap().pubkey();
            println!("Secp public key: {:?}", pubkey);
        }
    }
}

use std::path::PathBuf;

use clap::{error::ErrorKind, FromArgMatches};
use libp2p_identity::secp256k1::Keypair;
use monad_crypto::secp256k1::KeyPair;

use crate::{cli::Cli, config::NodeConfig, error::NodeSetupError};

pub struct NodeState {
    pub config: NodeConfig,
    pub identity: KeyPair,
    pub identity_libp2p: Keypair,
}

impl NodeState {
    pub fn setup(cmd: &mut clap::Command) -> Result<Self, NodeSetupError> {
        let cli = Cli::from_arg_matches_mut(&mut cmd.get_matches_mut())?;

        let config = toml::from_str(&std::fs::read_to_string(cli.config)?)?;

        let (identity, identity_libp2p) = load_identity_keypair(cli.identity)?;

        env_logger::try_init().map_err(NodeSetupError::EnvLoggerError)?;

        Ok(Self {
            config,
            identity,
            identity_libp2p,
        })
    }
}

fn load_identity_keypair(
    identity: PathBuf,
) -> Result<(KeyPair, libp2p_identity::secp256k1::Keypair), NodeSetupError> {
    let file = std::fs::File::open(identity)?;
    let mut reader = std::io::BufReader::new(file);

    let mut ec_private_keys = rustls_pemfile::ec_private_keys(&mut reader)?.into_iter();

    let Some(private_key_bytes) = ec_private_keys.next() else {
        return Err(NodeSetupError::Custom {
            kind: ErrorKind::ValueValidation,
            msg: "pem file does not contain \"EC PRIVATE KEY\" tag".to_owned(),
        });
    };

    if ec_private_keys.next().is_some() {
        return Err(NodeSetupError::Custom {
            kind: ErrorKind::ValueValidation,
            msg: "pem file contains more than one \"EC PRIVATE KEY\" tag".to_owned(),
        });
    }

    Ok(KeyPair::libp2p_from_der(private_key_bytes)?)
}

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
    fmt::Debug,
    fs::{self, File},
    io::{self, ErrorKind, Write},
    net::{SocketAddr, ToSocketAddrs},
    num::NonZeroU16,
    path::{Path, PathBuf},
};

use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_node_config::{NodeBootstrapConfig, NodeConfig};
use tempfile::NamedTempFile;
use tracing::info;

use crate::{Capability, MonadNameRecord, NameRecord};

pub const NAME_RECORD_FILE: &str = "self.toml";

pub enum Mode {
    ReadWrite,
    ReadOnly,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to prepare name record {}: {source}{backup_hint}", .path.display())]
    Record {
        path: PathBuf,
        #[source]
        source: Box<Error>,
        backup_hint: String,
    },
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("unable to resolve self address {address}: {source}")]
    ResolveAddress {
        address: String,
        #[source]
        source: io::Error,
    },
    #[error(transparent)]
    Deserialize(#[from] toml::de::Error),
    #[error(transparent)]
    Serialize(#[from] toml::ser::Error),
    #[error(transparent)]
    PeerConfig(#[from] crate::PeerConfigConversionError),
    #[error("saved name record must contain exactly one peer")]
    InvalidPeerCount,
    #[error("invalid name record signature")]
    InvalidSignature,
    #[error("name record revision exhausted; increase self_record_seq_num")]
    RevisionExhausted,
    #[error("changed name record must advance the saved sequence or revision")]
    SequenceNotAdvanced,
    #[error("read-only name record is missing. Create it with sign-name-record write")]
    MissingReadOnlyRecord,
    #[error(
        "read-only name record does not match the configured address, ports, sequence, or signature. Update it with sign-name-record write"
    )]
    ReadOnlyUpdateRequired,
}

/// Resolve the local IPv4 endpoint and build the record described by the node config.
pub fn from_config<ST: CertificateSignatureRecoverable>(
    node_config: &NodeConfig<ST>,
) -> Result<NameRecord, Error> {
    let config = &node_config.peer_discovery;
    let tcp_port = config.tcp_port().get();
    let address = (config.self_address.as_str(), tcp_port)
        .to_socket_addrs()
        .and_then(|mut addresses| {
            addresses
                .find_map(|address| match address {
                    SocketAddr::V4(address) => Some(address),
                    SocketAddr::V6(_) => None,
                })
                .ok_or_else(|| io::Error::new(ErrorKind::AddrNotAvailable, "no IPv4 address"))
        })
        .map_err(|source| Error::ResolveAddress {
            address: config.self_address.clone(),
            source,
        })?;
    let mut record = NameRecord::new_with_ports(
        *address.ip(),
        tcp_port,
        config.udp_port().map(NonZeroU16::get),
        config.self_auth_port.get(),
        config.self_direct_udp_port.map(NonZeroU16::get),
        config.self_record_seq_num,
    );
    if node_config.network.enable_encrypted_tcp {
        record.set_capability(Capability::EncryptedTcp);
    }
    Ok(record)
}

fn parse<ST: CertificateSignatureRecoverable>(
    contents: &str,
    pubkey: CertificateSignaturePubKey<ST>,
) -> Result<MonadNameRecord<ST>, Error> {
    let config: NodeBootstrapConfig<ST> = toml::from_str(contents)?;
    let [peer] = config.peers.as_slice() else {
        return Err(Error::InvalidPeerCount);
    };
    if peer.secp256k1_pubkey != pubkey {
        return Err(Error::InvalidSignature);
    }
    let record = MonadNameRecord::try_from(peer)?;
    verify_identity(&record, pubkey)?;
    Ok(record)
}

fn verify_identity<ST: CertificateSignatureRecoverable>(
    record: &MonadNameRecord<ST>,
    pubkey: CertificateSignaturePubKey<ST>,
) -> Result<(), Error> {
    record
        .recover_pubkey()
        .is_ok_and(|id| id.pubkey() == pubkey)
        .then_some(())
        .ok_or(Error::InvalidSignature)
}

/// Load the saved record. Read-write mode can also sign and persist a new record.
/// Read-only mode uses the saved revision and requires the configured fields to match.
pub fn load_or_create<ST: CertificateSignatureRecoverable>(
    path: &Path,
    name_record: NameRecord,
    signature: Option<ST>,
    key: &ST::KeyPairType,
    mode: Mode,
) -> Result<MonadNameRecord<ST>, Error> {
    let pubkey = key.pubkey();
    let read_only = matches!(mode, Mode::ReadOnly);
    let backup_path = path.with_added_extension("bak");
    let result = (|| -> Result<_, Error> {
        let _lock = if read_only {
            None
        } else {
            // A separate lock survives atomic replacement of the record's inode.
            let lock = File::options()
                .write(true)
                .create(true)
                .truncate(false)
                .open(path.with_added_extension("lock"))?;
            lock.lock()?;
            Some(lock)
        };
        let saved = match fs::read_to_string(path) {
            Ok(contents) => Some(contents),
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(err) => return Err(err.into()),
        };
        let previous = saved
            .as_deref()
            .map(|contents| parse(contents, pubkey))
            .transpose()?;

        let previous_revision = previous
            .as_ref()
            .filter(|previous| previous.seq() == name_record.seq())
            .map(|previous| previous.name_record.revision());
        let revision = match previous_revision {
            Some(revision) if read_only || signature.is_none() => revision,
            // Start above pre-signed records on first automatic startup.
            None if previous.is_none() && signature.is_none() && !read_only => 1,
            // Pre-signed records and higher operator sequences start at revision zero.
            _ => 0,
        };
        let mut name_record = name_record.with_revision(revision);
        let reused = previous.as_ref().filter(|previous| {
            previous.name_record == name_record
                && (!read_only || signature.is_none_or(|signature| signature == previous.signature))
        });

        if reused.is_none() {
            if read_only {
                return Err(match previous {
                    Some(_) => Error::ReadOnlyUpdateRequired,
                    None => Error::MissingReadOnlyRecord,
                });
            }
            if signature.is_none()
                && let Some(revision) = previous_revision
            {
                let revision = revision.checked_add(1).ok_or(Error::RevisionExhausted)?;
                name_record = name_record.with_revision(revision);
            }
        }

        let record = match signature.filter(|_| !read_only) {
            Some(signature) => {
                let configured = MonadNameRecord {
                    name_record,
                    signature,
                };
                verify_identity(&configured, pubkey)?;
                reused.cloned().unwrap_or(configured)
            }
            None => reused
                .cloned()
                .unwrap_or_else(|| MonadNameRecord::new(name_record, key)),
        };

        let action = if reused.is_some() {
            "reused"
        } else {
            // Changed records must advance sequence first, then revision.
            if previous
                .as_ref()
                .is_some_and(|previous| record.seq_and_revision() <= previous.seq_and_revision())
            {
                return Err(Error::SequenceNotAdvanced);
            }
            let contents = toml::to_string(&NodeBootstrapConfig::<ST> {
                peers: vec![record.with_pubkey(pubkey).into()],
            })?;
            // Save the old current record as backup before replacing it.
            let action = if let Some(saved) = &saved {
                write_atomic(&backup_path, saved.as_bytes())?;
                "updated"
            } else {
                "created"
            };
            write_atomic(path, contents.as_bytes())?;
            action
        };
        let changes = record_changes(
            previous.as_ref().map(|record| &record.name_record),
            &record.name_record,
        );
        let (record_seq_num, record_revision) = record.seq_and_revision();
        info!(
            node_id = %pubkey,
            action,
            read_only,
            ?changes,
            record_seq_num,
            record_revision,
            record_path = %path.display(),
            backup_path = %backup_path.display(),
            "name record"
        );
        Ok(record)
    })();
    result.map_err(|source| Error::Record {
        path: path.to_path_buf(),
        source: Box::new(source),
        backup_hint: if backup_path.is_file() {
            format!(
                "\nprevious name record available at {} for manual recovery; it is not restored automatically",
                backup_path.display()
            )
        } else {
            String::new()
        },
    })
}

/// Replace a file atomically and sync both its contents and directory.
fn write_atomic(path: &Path, contents: &[u8]) -> io::Result<()> {
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let directory_file = File::open(directory)?;
    let mut file = NamedTempFile::new_in(directory)?;
    file.write_all(contents)?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|err| err.error)?;
    directory_file.sync_all()
}

fn record_changes(
    previous_record: Option<&NameRecord>,
    current_record: &NameRecord,
) -> Vec<String> {
    fn change<T: Debug + PartialEq>(field: &str, old: Option<T>, new: T) -> Option<String> {
        (old.as_ref() != Some(&new)).then(|| format!("{field}: {old:?} -> {new:?}"))
    }

    let previous = previous_record.map(NameRecord::common);
    let current = current_record.common();
    [
        change("ip", previous.map(|record| record.ip), current.ip),
        change(
            "ports",
            previous.map(|record| &record.ports),
            &current.ports,
        ),
        change(
            "capabilities",
            previous.map(|record| record.capabilities),
            current.capabilities,
        ),
        change("seq", previous.map(|record| record.seq), current.seq),
        change(
            "revision",
            previous_record.map(NameRecord::revision),
            current_record.revision(),
        ),
    ]
    .into_iter()
    .flatten()
    .collect()
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use monad_secp::{KeyPair, SecpSignature};

    use super::*;

    #[test]
    fn name_record_lifecycle() -> Result<(), Error> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join(NAME_RECORD_FILE);
        let key = KeyPair::from_bytes(&mut [1; 32]).unwrap();
        let load = |port, seq, mode| {
            let record = NameRecord::new(Ipv4Addr::LOCALHOST, port, Some(port), 8001, 0, seq);
            load_or_create::<SecpSignature>(&path, record, None, &key, mode)
        };

        assert!(matches!(
            load(8000, 3, Mode::ReadOnly),
            Err(Error::Record { source, .. }) if matches!(*source, Error::MissingReadOnlyRecord)
        ));
        assert_eq!(fs::read_dir(directory.path())?.count(), 0);

        let first = load(8000, 3, Mode::ReadWrite)?;
        assert_eq!(first.seq_and_revision(), (3, 1));
        fs::remove_file(path.with_added_extension("lock"))?;
        let first_contents = fs::read(&path)?;

        assert_eq!(load(8000, 3, Mode::ReadOnly)?, first);
        assert!(matches!(
            load(8002, 3, Mode::ReadOnly),
            Err(Error::Record { source, .. }) if matches!(*source, Error::ReadOnlyUpdateRequired)
        ));
        assert_eq!(fs::read(&path)?, first_contents);
        assert_eq!(fs::read_dir(directory.path())?.count(), 1);

        let updated = load(8002, 3, Mode::ReadWrite)?;
        assert_eq!(updated.seq_and_revision(), (3, 2));
        assert!(matches!(
            load(8000, 2, Mode::ReadWrite),
            Err(Error::Record { source, .. }) if matches!(*source, Error::SequenceNotAdvanced)
        ));
        assert_eq!(load(8002, 3, Mode::ReadWrite)?, updated);
        assert_eq!(fs::read(path.with_added_extension("bak"))?, first_contents);
        Ok(())
    }

    #[test]
    fn capability_changes_are_signed_persisted_and_advance_revision() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(NAME_RECORD_FILE);
        let key = KeyPair::from_bytes(&mut [1; 32]).unwrap();
        let legacy = NameRecord::new(Ipv4Addr::LOCALHOST, 8000, None, 8001, 0, 1);
        let load = |record| {
            load_or_create::<SecpSignature>(&path, record, None, &key, Mode::ReadWrite).unwrap()
        };
        let initial = load(legacy.clone());
        let enabled = legacy.clone().with_capability(Capability::EncryptedTcp);
        let advertised = load(enabled.clone());
        assert_eq!(
            advertised.seq_and_revision(),
            (1, initial.name_record.revision() + 1)
        );
        assert!(
            advertised
                .name_record
                .check_capability(Capability::EncryptedTcp)
        );
        assert_eq!(advertised.recover_pubkey().unwrap().pubkey(), key.pubkey());
        assert_eq!(load(enabled), advertised);
        let disabled = load(legacy);
        assert_eq!(
            disabled.seq_and_revision(),
            (1, advertised.name_record.revision() + 1)
        );
        assert!(
            !disabled
                .name_record
                .check_capability(Capability::EncryptedTcp)
        );
        assert_eq!(disabled.recover_pubkey().unwrap().pubkey(), key.pubkey());
    }
}

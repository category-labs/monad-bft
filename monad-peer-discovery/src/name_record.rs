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
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Write},
    path::{Path, PathBuf},
};

use monad_crypto::certificate_signature::{CertificateKeyPair, CertificateSignatureRecoverable};
use monad_node_config::{NodeBootstrapConfig, NodeBootstrapPeerConfig};
use tempfile::NamedTempFile;
use tracing::info;

use crate::{MonadNameRecord, NameRecord};

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
    #[error("configured name record must advance the saved sequence")]
    SequenceNotAdvanced,
}

/// Reuse the saved record or sign and persist an updated record before discovery starts.
/// The operator controls the sequence; automatic signing controls the revision.
pub fn load_or_create<ST: CertificateSignatureRecoverable>(
    directory: &Path,
    mut name_record: NameRecord,
    signature: Option<ST>,
    key: &ST::KeyPairType,
) -> Result<MonadNameRecord<ST>, Error> {
    let path = directory.join("self.toml");
    let backup_path = directory.join("self.toml.bak");
    let result = (|| -> Result<_, Error> {
        // A separate lock survives atomic replacement of the record's inode.
        let lock = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.with_added_extension("lock"))?;
        lock.lock()?;
        let saved = match fs::read_to_string(&path) {
            Ok(contents) => Some(contents),
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(err) => return Err(err.into()),
        };
        let previous: Option<MonadNameRecord<ST>> = saved
            .as_deref()
            .map(|contents| {
                let config: NodeBootstrapConfig<ST> = toml::from_str(contents)?;
                let [peer] = config.peers.as_slice() else {
                    return Err(Error::InvalidPeerCount);
                };
                if peer.secp256k1_pubkey != key.pubkey() {
                    return Err(Error::InvalidSignature);
                }
                Ok(MonadNameRecord::try_from(peer)?)
            })
            .transpose()?;

        // Pre-signed records and higher operator sequences start at revision zero.
        name_record = name_record.with_revision(0);
        if signature.is_none() {
            if let Some(previous) = &previous {
                if name_record.seq() == previous.seq() {
                    // Match the saved revision before comparing contents; bump it only on changes.
                    name_record = name_record.with_revision(previous.name_record.revision());
                    if name_record != previous.name_record {
                        let revision = name_record
                            .revision()
                            .checked_add(1)
                            .ok_or(Error::RevisionExhausted)?;
                        name_record = name_record.with_revision(revision);
                    }
                }
            } else {
                // First automatic startup: supersede any pre-signed revision-zero record.
                name_record = name_record.with_revision(1);
            }
        }
        let mut record = match (signature, previous.as_ref()) {
            (Some(signature), _) => MonadNameRecord {
                name_record,
                signature,
            },
            (None, Some(previous)) if previous.name_record == name_record => previous.clone(),
            _ => MonadNameRecord::new(name_record, key),
        };
        if signature.is_some()
            && !record
                .recover_pubkey()
                .is_ok_and(|id| id.pubkey() == key.pubkey())
        {
            return Err(Error::InvalidSignature);
        }
        let reused = previous
            .as_ref()
            .is_some_and(|previous| previous.name_record == record.name_record);
        // Unchanged records leave both files untouched.
        if reused {
            record = previous.as_ref().unwrap().clone();
        } else {
            // Changed records must advance sequence first, then revision.
            if previous
                .as_ref()
                .is_some_and(|previous| record.seq_and_revision() <= previous.seq_and_revision())
            {
                return Err(Error::SequenceNotAdvanced);
            }
            let contents = toml::to_string(&NodeBootstrapConfig {
                peers: vec![NodeBootstrapPeerConfig::from(
                    record.with_pubkey(key.pubkey()),
                )],
            })?;
            // Save the old current record as backup before replacing it.
            if let Some(saved) = &saved {
                write_atomic(directory, "self.toml.bak", saved.as_bytes())?;
            }
            write_atomic(directory, "self.toml", contents.as_bytes())?;
        }
        let changes = record_changes(
            previous.as_ref().map(|record| &record.name_record),
            &record.name_record,
        );
        info!(
            node_id = %key.pubkey(),
            action = if reused { "reused" } else if previous.is_some() { "updated" } else { "created" },
            ?changes,
            record_seq_num = record.name_record.seq(),
            record_revision = record.name_record.revision(),
            record_path = %path.display(),
            backup_path = %backup_path.display(),
            "name record"
        );
        Ok(record)
    })();
    result.map_err(|source| Error::Record {
        path,
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
fn write_atomic(directory: &Path, name: &str, contents: &[u8]) -> io::Result<()> {
    let directory_file = File::open(directory)?;
    let mut file = NamedTempFile::new_in(directory)?;
    file.write_all(contents)?;
    file.as_file().sync_all()?;
    file.persist(directory.join(name))
        .map_err(|err| err.error)?;
    directory_file.sync_all()
}

fn record_changes(previous: Option<&NameRecord>, current: &NameRecord) -> Vec<String> {
    let mut changes = Vec::new();
    let previous_revision = previous.map(NameRecord::revision);
    let current_revision = current.revision();
    let previous = previous.map(NameRecord::common);
    let current = current.common();
    macro_rules! compare {
        ($($field:ident),*) => {$(
            if previous.map(|record| &record.$field) != Some(&current.$field) {
                changes.push(format!(
                    "{}: {:?} -> {:?}",
                    stringify!($field),
                    previous.map(|record| &record.$field),
                    current.$field
                ));
            }
        )*};
    }
    compare!(ip, ports, capabilities, seq);
    if previous_revision != Some(current_revision) {
        changes.push(format!(
            "revision: {previous_revision:?} -> {current_revision:?}"
        ));
    }
    changes
}

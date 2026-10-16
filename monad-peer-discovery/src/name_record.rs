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
    fmt::Display,
    fs::{self, OpenOptions},
    io::{self, ErrorKind},
    path::Path,
};

use monad_crypto::certificate_signature::{CertificateKeyPair, CertificateSignatureRecoverable};
use tracing::info;

use crate::{MonadNameRecord, NameRecord, atomic_file};

/// Reuse the saved record or sign and persist an updated record before discovery starts.
pub fn load_or_create<ST: CertificateSignatureRecoverable>(
    path: &Path,
    configured_seq: Option<u64>,
    mut name_record: NameRecord,
    signature: Option<ST>,
    key: &ST::KeyPairType,
) -> io::Result<MonadNameRecord<ST>> {
    // A separate lock survives atomic replacement of the record's inode.
    let lock = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.with_added_extension("lock"))?;
    lock.lock()?;
    let saved = match fs::read_to_string(path) {
        Ok(contents) => Some(contents),
        Err(err) if err.kind() == ErrorKind::NotFound => None,
        Err(err) => return Err(err),
    };
    let previous: Option<MonadNameRecord<ST>> = saved
        .as_deref()
        .map(toml::from_str)
        .transpose()
        .map_err(invalid_data)?;
    if let Some(previous) = &previous {
        verify(previous, key)?;
    }

    name_record.seq = configured_seq.unwrap_or_default();
    if signature.is_none() {
        if let Some(previous) = &previous {
            name_record.seq = previous.seq();
            if name_record != previous.name_record {
                name_record.seq = next_sequence(previous.seq())?;
            }
            name_record.seq = name_record.seq.max(configured_seq.unwrap_or_default());
        } else if let Some(sequence) = configured_seq {
            // Existing operators only need to remove the configured signature.
            name_record.seq = next_sequence(sequence)?;
        }
    }
    let mut record = match signature {
        Some(signature) => MonadNameRecord {
            name_record,
            signature,
        },
        None => MonadNameRecord::new(name_record, key),
    };
    verify(&record, key)?;
    let reused = previous
        .as_ref()
        .is_some_and(|previous| previous.name_record == record.name_record);
    if reused {
        record = previous.as_ref().unwrap().clone();
    } else {
        if previous
            .as_ref()
            .is_some_and(|previous| record.seq() <= previous.seq())
        {
            return Err(invalid_data(
                "configured name record must advance the saved sequence",
            ));
        }
        let contents = toml::to_string_pretty(&record).map_err(invalid_data)?;
        if let Some(saved) = &saved {
            atomic_file::write(&path.with_added_extension("bak"), saved.as_bytes())?;
        }
        atomic_file::write(path, contents.as_bytes())?;
    }
    let changes = record_changes(
        previous.as_ref().map(|record| &record.name_record),
        &record.name_record,
    );
    info!(
        node_id = %key.pubkey(),
        action = if reused { "reused" } else if previous.is_some() { "updated" } else { "created" },
        ?changes,
        sequence = record.seq(),
        record_path = %path.display(),
        backup_path = %path.with_added_extension("bak").display(),
        "name record"
    );
    Ok(record)
}

fn next_sequence(sequence: u64) -> io::Result<u64> {
    sequence
        .checked_add(1)
        .ok_or_else(|| invalid_data("name record sequence exhausted"))
}

fn invalid_data(error: impl Display) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, error.to_string())
}

fn verify<ST: CertificateSignatureRecoverable>(
    record: &MonadNameRecord<ST>,
    key: &ST::KeyPairType,
) -> io::Result<()> {
    record.name_record.validate().map_err(invalid_data)?;
    if !record
        .recover_pubkey()
        .is_ok_and(|id| id.pubkey() == key.pubkey())
    {
        return Err(invalid_data("invalid name record signature"));
    }
    Ok(())
}

fn record_changes(previous: Option<&NameRecord>, current: &NameRecord) -> Vec<String> {
    let mut changes = Vec::new();
    macro_rules! compare {
        ($field:ident) => {
            if previous.map(|record| &record.$field) != Some(&current.$field) {
                changes.push(format!(
                    "{}: {:?} -> {:?}",
                    stringify!($field),
                    previous.map(|record| &record.$field),
                    current.$field
                ));
            }
        };
    }
    compare!(ip);
    compare!(ports);
    compare!(capabilities);
    compare!(seq);
    changes
}

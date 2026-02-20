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
    fs::{File, OpenOptions},
    io::{self, ErrorKind, Read, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
    time::SystemTime,
};

use monad_consensus_types::{
    block::ConsensusBlockHeader,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{BlockId, ExecutionProtocol, Hash};
use monad_validator::signature_collection::SignatureCollection;

pub const BLOCKDB_HEADERS_PATH: &str = "headers";
const BLOCKDB_BODIES_PATH: &str = "bodies";
const BLOCKDB_PROPOSED_HEAD_PATH: &str = "proposed_head";
const BLOCKDB_VOTED_HEAD_PATH: &str = "voted_head";
const BLOCKDB_FINALIZED_HEAD_PATH: &str = "finalized_head";

pub trait BlockPersist<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn write_bft_header(&mut self, block: &ConsensusBlockHeader<ST, SCT, EPT>) -> io::Result<()>;
    fn write_bft_body(&mut self, payload: &ConsensusBlockBody<EPT>) -> io::Result<()>;

    fn update_proposed_head(&mut self, block_id: &BlockId) -> io::Result<()>;
    fn update_voted_head(&mut self, block_id: &BlockId) -> io::Result<()>;
    fn update_finalized_head(&mut self, block_id: &BlockId) -> io::Result<()>;

    fn read_proposed_head_bft_header(&self) -> io::Result<ConsensusBlockHeader<ST, SCT, EPT>>;
    fn read_bft_header(&self, block_id: &BlockId)
        -> io::Result<ConsensusBlockHeader<ST, SCT, EPT>>;
    fn read_bft_body(
        &self,
        payload_id: &ConsensusBlockBodyId,
    ) -> io::Result<ConsensusBlockBody<EPT>>;
}

fn block_id_to_hex(hash: &Hash) -> String {
    hex::encode(hash.0)
}

fn create_dir_safe(base: &Path, name: &str) -> PathBuf {
    let path = base.join(name);
    match std::fs::create_dir(&path) {
        Ok(()) => (),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
        Err(e) => panic!("failed to create directory {path:?}: {e}"),
    }
    path
}

fn atomic_symlink_update(target: &Path, link_path: &Path) -> io::Result<()> {
    let mut wip = link_path.to_path_buf();
    wip.set_extension("wip");
    std::os::unix::fs::symlink(target, &wip)?;
    std::fs::rename(&wip, link_path)?;
    Ok(())
}

fn read_and_decode<T: alloy_rlp::Decodable>(
    path: &Path,
    context: impl FnOnce() -> String,
) -> io::Result<T> {
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    let mut buf = vec![0; size as usize];
    file.read_exact(&mut buf)?;

    alloy_rlp::decode_exact(&buf).map_err(|err| {
        io::Error::other(format!("failed to rlp decode {}, err={:?}", context(), err))
    })
}

#[derive(Clone)]
pub struct FileBlockPersist<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    headers_path: PathBuf,
    bodies_path: PathBuf,
    proposed_head_path: PathBuf,
    voted_head_path: PathBuf,
    finalized_head_path: PathBuf,

    _pd: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> FileBlockPersist<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(ledger_path: PathBuf) -> Self {
        let headers_path = create_dir_safe(&ledger_path, BLOCKDB_HEADERS_PATH);
        let bodies_path = create_dir_safe(&ledger_path, BLOCKDB_BODIES_PATH);

        let proposed_head_path = headers_path.join(BLOCKDB_PROPOSED_HEAD_PATH);
        let voted_head_path = headers_path.join(BLOCKDB_VOTED_HEAD_PATH);
        let finalized_head_path = headers_path.join(BLOCKDB_FINALIZED_HEAD_PATH);

        Self {
            headers_path,
            bodies_path,
            proposed_head_path,
            voted_head_path,
            finalized_head_path,

            _pd: PhantomData,
        }
    }

    fn header_path(&self, block_id: &BlockId) -> PathBuf {
        self.headers_path.join(block_id_to_hex(&block_id.0))
    }

    fn body_path(&self, body_id: &ConsensusBlockBodyId) -> PathBuf {
        self.bodies_path.join(block_id_to_hex(&body_id.0))
    }
}

impl<ST, SCT, EPT> BlockPersist<ST, SCT, EPT> for FileBlockPersist<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn write_bft_header(&mut self, block: &ConsensusBlockHeader<ST, SCT, EPT>) -> io::Result<()> {
        let file_path = self.header_path(&block.get_id());

        if let Ok(existing_header) = OpenOptions::new().write(true).open(&file_path) {
            existing_header
                .set_modified(SystemTime::now())
                .expect("failed to update timestamp meta of existing block header");
            return Ok(());
        }
        let mut f = File::create(file_path).unwrap();
        f.write_all(&alloy_rlp::encode(block)).unwrap();

        Ok(())
    }

    fn write_bft_body(&mut self, body: &ConsensusBlockBody<EPT>) -> io::Result<()> {
        let file_path = self.body_path(&body.get_id());

        if let Ok(existing_body) = OpenOptions::new().write(true).open(&file_path) {
            existing_body
                .set_modified(SystemTime::now())
                .expect("failed to update timestamp meta of existing block body");
            return Ok(());
        }
        let mut f = File::create(file_path).unwrap();
        f.write_all(&alloy_rlp::encode(body)).unwrap();

        Ok(())
    }

    fn update_proposed_head(&mut self, block_id: &BlockId) -> io::Result<()> {
        atomic_symlink_update(&self.header_path(block_id), &self.proposed_head_path)
    }

    fn update_voted_head(&mut self, block_id: &BlockId) -> io::Result<()> {
        atomic_symlink_update(&self.header_path(block_id), &self.voted_head_path)
    }

    fn update_finalized_head(&mut self, block_id: &BlockId) -> io::Result<()> {
        atomic_symlink_update(&self.header_path(block_id), &self.finalized_head_path)
    }

    fn read_proposed_head_bft_header(&self) -> io::Result<ConsensusBlockHeader<ST, SCT, EPT>> {
        read_and_decode(&self.proposed_head_path, || {
            "ledger proposed_head bft header".into()
        })
    }

    fn read_bft_header(
        &self,
        block_id: &BlockId,
    ) -> io::Result<ConsensusBlockHeader<ST, SCT, EPT>> {
        read_and_decode(&self.header_path(block_id), || {
            format!("ledger bft header, block_id={:?}", block_id)
        })
    }

    fn read_bft_body(&self, body_id: &ConsensusBlockBodyId) -> io::Result<ConsensusBlockBody<EPT>> {
        read_and_decode(&self.body_path(body_id), || {
            format!("ledger bft body, body_id={:?}", body_id)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn count_files_in_dir(dir: &Path) -> usize {
        fs::read_dir(dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .count()
    }

    #[test]
    fn test_atomic_symlink_update_single_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let target_path = temp_dir.path().join("target_file");
        let link_path = temp_dir.path().join("my_link");

        fs::write(&target_path, b"test content").unwrap();

        assert!(!link_path.is_symlink());

        atomic_symlink_update(&target_path, &link_path).unwrap();

        assert!(link_path.is_symlink());
        assert_eq!(count_files_in_dir(temp_dir.path()), 2);

        let link_target = fs::read_link(&link_path).unwrap();
        assert_eq!(link_target, target_path);
    }

    #[test]
    fn test_atomic_symlink_update_with_inotify() {
        let temp_dir = tempfile::tempdir().unwrap();
        let target_path = temp_dir.path().join("target_file");
        let link_path = temp_dir.path().join("my_link");

        fs::write(&target_path, b"test content").unwrap();

        let mut inotify = inotify::Inotify::init().unwrap();
        inotify
            .watches()
            .add(
                temp_dir.path(),
                inotify::WatchMask::CREATE
                    | inotify::WatchMask::MOVED_TO
                    | inotify::WatchMask::DELETE,
            )
            .unwrap();

        atomic_symlink_update(&target_path, &link_path).unwrap();

        let mut buffer = [0u8; 4096];

        let inotify_events = inotify
            .read_events_blocking(&mut buffer)
            .unwrap()
            .collect::<Vec<_>>();

        let mut events = Vec::new();

        for event in inotify_events {
            if let Some(name) = event.name {
                events.push((event.mask, name.to_string_lossy().to_string()));
            }
        }

        assert_eq!(events.len(), 2, "Expected exactly 2 events");
        assert_eq!(count_files_in_dir(temp_dir.path()), 2);

        // CREATE for my_link.wip
        assert!(
            events[0].0.eq(&inotify::EventMask::CREATE),
            "First event should be CREATE"
        );
        assert_eq!(
            events[0].1, "my_link.wip",
            "First event should be for my_link.wip"
        );

        // MOVED_TO for my_link
        assert!(
            events[1].0.eq(&inotify::EventMask::MOVED_TO),
            "Second event should be MOVED_TO"
        );
        assert_eq!(events[1].1, "my_link", "Second event should be for my_link");
    }
}

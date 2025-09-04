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
    io::{ErrorKind, Read, Write},
    marker::PhantomData,
    path::PathBuf,
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
const BLOCKDB_FINALIZED_HEAD_PATH: &str = "finalized_head";

pub trait BlockPersist<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn write_bft_header(
        &mut self,
        block: &ConsensusBlockHeader<ST, SCT, EPT>,
    ) -> std::io::Result<()>;
    fn write_bft_body(&mut self, payload: &ConsensusBlockBody<EPT>) -> std::io::Result<()>;

    fn update_proposed_head(&mut self, block_id: &BlockId) -> std::io::Result<()>;
    fn update_finalized_head(&mut self, block_id: &BlockId) -> std::io::Result<()>;

    fn read_bft_header(
        &self,
        block_id: &BlockId,
    ) -> std::io::Result<ConsensusBlockHeader<ST, SCT, EPT>>;
    fn read_bft_body(
        &self,
        payload_id: &ConsensusBlockBodyId,
    ) -> std::io::Result<ConsensusBlockBody<EPT>>;
}

fn block_id_to_hex_prefix(hash: &Hash) -> String {
    hex::encode(hash.0)
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
        let headers_path = {
            let headers_path = PathBuf::from(&ledger_path).join(BLOCKDB_HEADERS_PATH);
            match std::fs::create_dir(&headers_path) {
                Ok(_) => (),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
                Err(e) => panic!("{}", e),
            }
            headers_path
        };

        let bodies_path = {
            let bodies_path = PathBuf::from(&ledger_path).join(BLOCKDB_BODIES_PATH);
            match std::fs::create_dir(&bodies_path) {
                Ok(_) => (),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
                Err(e) => panic!("{}", e),
            }
            bodies_path
        };

        let proposed_head_path = PathBuf::from(&headers_path).join(BLOCKDB_PROPOSED_HEAD_PATH);
        let finalized_head_path = PathBuf::from(&headers_path).join(BLOCKDB_FINALIZED_HEAD_PATH);

        Self {
            headers_path,
            bodies_path,
            proposed_head_path,
            finalized_head_path,

            _pd: PhantomData,
        }
    }

    fn header_path(&self, block_id: &BlockId) -> PathBuf {
        let mut file_path = PathBuf::from(&self.headers_path);
        file_path.push(block_id_to_hex_prefix(&block_id.0));
        file_path
    }

    fn body_path(&self, body_id: &ConsensusBlockBodyId) -> PathBuf {
        let mut file_path = PathBuf::from(&self.bodies_path);
        file_path.push(block_id_to_hex_prefix(&body_id.0));
        file_path
    }

    pub fn read_proposed_head_bft_header(
        &self,
    ) -> std::io::Result<ConsensusBlockHeader<ST, SCT, EPT>> {
        let mut file = File::open(&self.proposed_head_path)?;
        let size = file.metadata()?.len();
        let mut buf = vec![0; size as usize];
        file.read_exact(&mut buf)?;

        let header = alloy_rlp::decode_exact(&buf).map_err(|err| {
            std::io::Error::other(format!(
                "failed to rlp decode ledger proposed_head bft header, err={:?}",
                err
            ))
        })?;

        Ok(header)
    }
}

impl<ST, SCT, EPT> BlockPersist<ST, SCT, EPT> for FileBlockPersist<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn write_bft_header(
        &mut self,
        block: &ConsensusBlockHeader<ST, SCT, EPT>,
    ) -> std::io::Result<()> {
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

    fn write_bft_body(&mut self, body: &ConsensusBlockBody<EPT>) -> std::io::Result<()> {
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

    fn update_proposed_head(&mut self, block_id: &BlockId) -> std::io::Result<()> {
        let mut wip = PathBuf::from(&self.proposed_head_path);
        wip.set_extension(".wip");
        std::os::unix::fs::symlink(self.header_path(block_id), &wip)?;
        std::fs::rename(&wip, &self.proposed_head_path)?;
        Ok(())
    }

    fn update_finalized_head(&mut self, block_id: &BlockId) -> std::io::Result<()> {
        let mut wip = PathBuf::from(&self.finalized_head_path);
        wip.set_extension(".wip");
        std::os::unix::fs::symlink(self.header_path(block_id), &wip)?;
        std::fs::rename(&wip, &self.finalized_head_path)?;
        Ok(())
    }

    fn read_bft_header(
        &self,
        block_id: &BlockId,
    ) -> std::io::Result<ConsensusBlockHeader<ST, SCT, EPT>> {
        let file_path = self.header_path(block_id);

        let mut file = File::open(file_path)?;
        let size = file.metadata()?.len();
        let mut buf = vec![0; size as usize];
        file.read_exact(&mut buf)?;

        let header = alloy_rlp::decode_exact(&buf).map_err(|err| {
            std::io::Error::other(format!(
                "failed to rlp decode ledger bft header, block_id={:?}, err={:?}",
                block_id, err
            ))
        })?;

        Ok(header)
    }

    fn read_bft_body(
        &self,
        body_id: &ConsensusBlockBodyId,
    ) -> std::io::Result<ConsensusBlockBody<EPT>> {
        let file_path = self.body_path(body_id);
        let mut file = File::open(file_path)?;
        let size = file.metadata()?.len();
        let mut buf = vec![0; size as usize];
        file.read_exact(&mut buf)?;

        let body = alloy_rlp::decode_exact(&buf).map_err(|err| {
            std::io::Error::other(format!(
                "failed to rlp decode ledger bft body, body_id={:?}, err={:?}",
                body_id, err
            ))
        })?;

        Ok(body)
    }
}

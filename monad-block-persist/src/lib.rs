use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    marker::PhantomData,
    path::PathBuf,
    time::SystemTime,
};

use monad_consensus_types::{
    block::ConsensusBlockHeader,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{BlockId, ExecutionProtocol, Hash};

pub const BLOCKDB_HEADER_EXTENSION: &str = ".header";
pub const BLOCKDB_BODY_EXTENSION: &str = ".body";

pub trait BlockPersist<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn write_bft_header(&self, block: &ConsensusBlockHeader<ST, SCT, EPT>) -> std::io::Result<()>;
    fn write_bft_body(&self, payload: &ConsensusBlockBody<EPT>) -> std::io::Result<()>;

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
    ledger_path: PathBuf,

    _pd: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> FileBlockPersist<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(ledger_path: PathBuf) -> Self {
        Self {
            ledger_path,

            _pd: PhantomData,
        }
    }

    fn header_path(&self, block_id: &BlockId) -> PathBuf {
        let mut file_path = PathBuf::from(&self.ledger_path);
        file_path.push(format!(
            "{}{}",
            block_id_to_hex_prefix(&block_id.0),
            BLOCKDB_HEADER_EXTENSION
        ));
        file_path
    }

    fn body_path(&self, body_id: &ConsensusBlockBodyId) -> PathBuf {
        let mut file_path = PathBuf::from(&self.ledger_path);
        file_path.push(format!(
            "{}{}",
            block_id_to_hex_prefix(&body_id.0),
            BLOCKDB_BODY_EXTENSION
        ));
        file_path
    }
}

impl<ST, SCT, EPT> BlockPersist<ST, SCT, EPT> for FileBlockPersist<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn write_bft_header(&self, block: &ConsensusBlockHeader<ST, SCT, EPT>) -> std::io::Result<()> {
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

    fn write_bft_body(&self, body: &ConsensusBlockBody<EPT>) -> std::io::Result<()> {
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

    fn read_bft_header(
        &self,
        block_id: &BlockId,
    ) -> std::io::Result<ConsensusBlockHeader<ST, SCT, EPT>> {
        let file_path = self.header_path(block_id);

        let mut file = File::open(file_path)?;
        let size = file.metadata()?.len();
        let mut buf = vec![0; size as usize];
        file.read_exact(&mut buf)?;

        // TODO maybe expect is too strict
        let block =
            alloy_rlp::decode_exact(&buf).expect("local ledger consensus header decode failed");

        Ok(block)
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

        // TODO maybe expect is too strict
        let body =
            alloy_rlp::decode_exact(&buf).expect("local ledger consensus body decode failed");

        Ok(body)
    }
}

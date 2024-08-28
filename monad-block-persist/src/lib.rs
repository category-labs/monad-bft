use std::{
    fs::File,
    io::{Read, Write},
    path::PathBuf,
};

use monad_consensus_types::{
    block::{Block, BlockType},
    payload::{Payload, PayloadId},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_proto::proto::block::{ProtoBlock, ProtoPayload};
use monad_types::BlockId;
use prost::Message;

pub trait BlockPersist<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn write_bft_block(&self, block: &Block<SCT>) -> std::io::Result<()>;
    fn write_bft_payload(&self, payload: &Payload) -> std::io::Result<()>;

    fn read_bft_block(&self, block_id: &BlockId) -> std::io::Result<Block<SCT>>;
    fn read_bft_block_by_num(&self, block_num: u64) -> std::io::Result<Block<SCT>>;
    fn read_bft_payload(&self, payload_id: &PayloadId) -> std::io::Result<Payload>;
    fn read_encoded_eth_block(&self, block_num: u64) -> std::io::Result<Vec<u8>>;
}

#[derive(Clone)]
pub struct FileBlockPersist {
    block_dir_path: PathBuf,
    payload_dir_path: PathBuf,
    eth_block_dir_path: PathBuf,
}

impl FileBlockPersist {
    pub fn new(
        block_dir_path: PathBuf,
        payload_dir_path: PathBuf,
        eth_block_dir_path: PathBuf,
    ) -> Self {
        Self {
            block_dir_path,
            payload_dir_path,
            eth_block_dir_path,
        }
    }
}

impl<ST, SCT> BlockPersist<ST, SCT> for FileBlockPersist
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn write_bft_block(&self, block: &Block<SCT>) -> std::io::Result<()> {
        let proto_block: ProtoBlock = block.into();
        let encoded = proto_block.encode_to_vec();

        let filename = block.get_id().0.to_string();
        let mut file_path = PathBuf::from(&self.block_dir_path);
        file_path.push(format!("{}", filename));

        let mut f = File::create(file_path).unwrap();
        f.write_all(&encoded).unwrap();

        Ok(())
    }

    fn write_bft_payload(&self, payload: &Payload) -> std::io::Result<()> {
        let proto_payload: ProtoPayload = payload.into();
        let encoded = proto_payload.encode_to_vec();

        let filename = payload.get_id().0.to_string();
        let mut file_path = PathBuf::from(&self.payload_dir_path);
        file_path.push(format!("{}", filename));

        let mut f = File::create(file_path).unwrap();
        f.write_all(&encoded).unwrap();

        Ok(())
    }

    fn read_bft_block(&self, block_id: &BlockId) -> std::io::Result<Block<SCT>> {
        let filename = block_id.0.to_string();
        let mut file_path = PathBuf::from(&self.block_dir_path);
        file_path.push(format!("{}", filename));
        let mut file = File::open(file_path)?;

        let size = file.metadata()?.len();
        let mut buf = vec![0; size as usize];
        file.read_exact(&mut buf)?;

        // TODO maybe expect is too strict
        let proto_block = ProtoBlock::decode(buf.as_slice()).expect("local protoblock decode");
        let block: Block<SCT> = proto_block
            .try_into()
            .expect("proto_block to block should not be invalid");

        Ok(block)
    }

    fn read_bft_payload(&self, payload_id: &PayloadId) -> std::io::Result<Payload> {
        let filename = payload_id.0.to_string();
        let mut file_path = PathBuf::from(&self.payload_dir_path);
        file_path.push(format!("{}", filename));
        let mut file = File::open(file_path)?;

        let size = file.metadata()?.len();
        let mut buf = vec![0; size as usize];
        file.read_exact(&mut buf)?;

        // TODO maybe expect is too strict
        let proto_payload =
            ProtoPayload::decode(buf.as_slice()).expect("local protopayload decode");
        let payload: Payload = proto_payload
            .try_into()
            .expect("proto_payload to payload should not be invalid");

        Ok(payload)
    }

    fn read_bft_block_by_num(&self, block_num: u64) -> std::io::Result<Block<SCT>> {
        let filename = block_num.to_string();
        let mut file_path = PathBuf::from(&self.block_dir_path);
        file_path.push(format!("{}", filename));
        let mut file = File::open(file_path)?;

        let size = file.metadata()?.len();
        let mut buf = vec![0; size as usize];
        file.read_exact(&mut buf)?;

        // TODO maybe expect is too strict
        let proto_block = ProtoBlock::decode(buf.as_slice()).expect("local protoblock decode");
        let block: Block<SCT> = proto_block
            .try_into()
            .expect("proto_block to block should not be invalid");

        Ok(block)
    }

    fn read_encoded_eth_block(&self, block_num: u64) -> std::io::Result<Vec<u8>> {
        let filename = block_num.to_string();
        let mut file_path = PathBuf::from(&self.eth_block_dir_path);
        file_path.push(format!("{}", filename));
        let mut file = File::open(file_path)?;

        let size = file.metadata().unwrap().len();
        let mut buf = vec![0; size as usize];
        file.read_exact(&mut buf).unwrap();

        Ok(buf)
    }
}

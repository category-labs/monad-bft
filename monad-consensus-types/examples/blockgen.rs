use alloy_consensus::Header;
use alloy_rlp::Encodable;
use monad_consensus_types::{
    block::ConsensusBlockHeader,
    payload::{
        ConsensusBlockBody, EthBlockBody, EthExecutionProtocol, EthHeader, ProposedEthHeader,
        RoundSignature,
    },
    quorum_certificate::QuorumCertificate,
};
use monad_crypto::{
    certificate_signature::CertificateKeyPair,
    NopKeyPair, NopSignature,
};
use monad_multi_sig::MultiSig;
use monad_types::{Epoch, NodeId, Round, SeqNum};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let body: ConsensusBlockBody<EthExecutionProtocol> = ConsensusBlockBody {
        execution_body: EthBlockBody {
            transactions: Default::default(),
            ommers: Default::default(),
            withdrawals: Default::default(),
        },
    };

    let keypair = NopKeyPair::from_bytes(&mut [1_u8; 32])?;
    let header: ConsensusBlockHeader<NopSignature, MultiSig<NopSignature>, EthExecutionProtocol> =
        ConsensusBlockHeader::new(
            NodeId::new(keypair.pubkey()),
            Epoch(5),
            Round(10),
            vec![EthHeader(Header::default())],
            ProposedEthHeader::default(),
            body.get_id(),
            QuorumCertificate::genesis_qc(),
            SeqNum(5),
            1731018255 * 1000,
            RoundSignature::new(Round(10), &keypair),
        );

    let mut header_bytes = Vec::default();
    header.encode(&mut header_bytes);
    std::fs::write("sample_header", &header_bytes).expect("failed to write file");

    let mut body_bytes = Vec::default();
    body.encode(&mut body_bytes);
    std::fs::write("sample_body", &body_bytes).expect("failed to write file");

    Ok(())
}

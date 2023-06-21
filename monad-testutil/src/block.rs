use monad_consensus::signatures::aggregate_signature::AggregateSignatures;
use monad_consensus::types::block::Block;
use monad_consensus::types::block::TransactionList;
use monad_consensus::types::ledger::LedgerCommitInfo;
use monad_consensus::types::quorum_certificate::QcInfo;
use monad_consensus::types::quorum_certificate::QuorumCertificate;
use monad_consensus::types::signature::SignatureCollection;
use monad_consensus::types::voting::VoteInfo;
use monad_consensus::validation::hashing::Hasher;
use monad_consensus::validation::hashing::Sha256Hash;
use monad_crypto::secp256k1::SecpKeyPair;
use monad_crypto::secp256k1::SecpSignature;
use monad_crypto::KeyPair;
use monad_types::BlockId;
use monad_types::Hash;
use monad_types::NodeId;
use monad_types::Round;

pub fn setup_block(
    author: NodeId,
    block_round: u64,
    qc_round: u64,
    txns: TransactionList,
    keypairs: &[SecpKeyPair],
) -> Block<AggregateSignatures<SecpSignature>> {
    let txns = txns;
    let round = Round(block_round);

    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        round: Round(qc_round),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(0),
    };
    let lci = LedgerCommitInfo::new::<Sha256Hash>(None, &vi);

    let qcinfo = QcInfo {
        vote: vi,
        ledger_commit: lci,
    };
    let qcinfo_hash = Sha256Hash::hash_object(&qcinfo.ledger_commit);

    let mut aggsig = AggregateSignatures::new();
    for keypair in keypairs.iter() {
        aggsig.add_signature(keypair.sign(qcinfo_hash.as_ref()));
    }

    let qc = QuorumCertificate::<AggregateSignatures<SecpSignature>>::new(qcinfo, aggsig);

    Block::<AggregateSignatures<SecpSignature>>::new::<Sha256Hash>(author, round, &txns, &qc)
}

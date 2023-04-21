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
use monad_crypto::secp256k1::KeyPair;
use monad_crypto::secp256k1::SecpSignature;
use monad_types::BlockId;
use monad_types::Hash;
use monad_types::NodeId;
use monad_types::Round;

pub fn setup_block(
    author: NodeId,
    block_round: u64,
    qc_round: u64,
    txns: TransactionList,
    keypairs: &Vec<KeyPair>,
) -> Block<AggregateSignatures<SecpSignature>> {
    let txns = txns;
    let round = Round(block_round);

    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32].into())),
        round: Round(qc_round),
        parent_id: BlockId(Hash([43_u8; 32].into())),
        parent_round: Round(0),
    };
    let lci = LedgerCommitInfo {
        commit_state_hash: None,
        vote_info_hash: Sha256Hash::hash_object(&vi),
    };

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

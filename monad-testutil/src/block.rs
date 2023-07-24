use monad_consensus_types::{
    block::{Block, TransactionList},
    ledger::LedgerCommitInfo,
    multi_sig::MultiSig,
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature::SignatureCollection,
    validation::{Hasher, Sha256Hash},
    voting::VoteInfo,
};
use monad_crypto::secp256k1::{KeyPair, SecpSignature};
use monad_types::{BlockId, Hash, NodeId, Round};

pub fn setup_block(
    author: NodeId,
    block_round: u64,
    qc_round: u64,
    txns: TransactionList,
    keypairs: &[KeyPair],
) -> Block<MultiSig<SecpSignature>> {
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

    let mut aggsig = MultiSig::new();
    for keypair in keypairs.iter() {
        aggsig.add_signature(keypair.sign(qcinfo_hash.as_ref()));
    }

    let qc = QuorumCertificate::<MultiSig<SecpSignature>>::new(qcinfo, aggsig);

    Block::<MultiSig<SecpSignature>>::new::<Sha256Hash>(author, round, &txns, &qc)
}

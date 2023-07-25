use monad_consensus_types::{
    block::{Block, TransactionList},
    ledger::LedgerCommitInfo,
    multi_sig::MultiSig,
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature::{SignatureBuilder, SignatureCollection},
    validation::{Hasher, Sha256Hash},
    voting::VoteInfo,
};
use monad_crypto::{
    secp256k1::{KeyPair, SecpSignature},
    Signature,
};
use monad_types::{BlockId, Hash, NodeId, Round};
use monad_validator::validator_property::{ValidatorSetProperty, ValidatorSetPropertyType};

pub fn setup_block(
    author: NodeId,
    block_round: u64,
    qc_round: u64,
    txns: TransactionList,
    keypairs: &[KeyPair],
    validators_property: &ValidatorSetProperty,
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

    let mut builder = SignatureBuilder::new();
    for keypair in keypairs.iter() {
        let idx = validators_property
            .get_index(&NodeId(keypair.pubkey()))
            .unwrap();
        builder.add_signature(idx, SecpSignature::sign(qcinfo_hash.as_ref(), keypair));
    }

    let aggsig = MultiSig::new(
        builder,
        validators_property.get_voting_list(),
        qcinfo_hash.as_ref(),
    )
    .unwrap();

    let qc = QuorumCertificate::<MultiSig<SecpSignature>>::new(qcinfo, aggsig);

    Block::<MultiSig<SecpSignature>>::new::<Sha256Hash>(author, round, &txns, &qc)
}

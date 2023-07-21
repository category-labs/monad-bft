use monad_consensus_types::{
    block::{Block, TransactionList},
    ledger::LedgerCommitInfo,
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature::{SignatureBuilder, SignatureCollection, SignatureCollectionKeyPairType},
    validation::{Hasher, Sha256Hash},
    voting::VoteInfo,
};
use monad_crypto::GenericSignature;
use monad_types::{BlockId, Hash, NodeId, Round};
use monad_validator::validator_property::{ValidatorSetProperty, ValidatorSetPropertyType};

pub fn setup_block<'a, SCT: SignatureCollection>(
    author: NodeId,
    block_round: u64,
    qc_round: u64,
    txns: TransactionList,
    voters: impl Iterator<Item = (NodeId, &'a SignatureCollectionKeyPairType<SCT>)>,
    validators_property: &ValidatorSetProperty,
) -> Block<SCT> {
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
    for (node_id, keypair) in voters {
        let idx = validators_property.get_index(&node_id).unwrap();
        builder.add_signature(
            idx,
            <SCT::SignatureType as GenericSignature>::sign(qcinfo_hash.as_ref(), keypair),
        );
    }

    let aggsig = SCT::new(
        builder,
        validators_property.get_voting_list(),
        qcinfo_hash.as_ref(),
    )
    .unwrap();

    let qc = QuorumCertificate::<SCT>::new(qcinfo, aggsig);

    Block::<SCT>::new::<Sha256Hash>(author, round, &txns, &qc)
}

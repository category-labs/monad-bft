use monad_consensus_types::{
    block::FullBlock,
    quorum_certificate::QuorumCertificate,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    validation::Hasher,
    voting::ValidatorMapping,
};
use monad_types::{NodeId, Round};
use monad_validator::validator_set::ValidatorSetType;

pub enum ElectionInfo<'a, SCT, VT>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType,
{
    Commits(
        &'a Vec<FullBlock<SCT>>,
        &'a ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    ),
    QC(&'a QuorumCertificate<SCT>, Round, &'a VT),
}

pub trait LeaderElection {
    fn submit_election_info<H, SCT, VT>(&mut self, _info: ElectionInfo<SCT, VT>)
    where
        H: Hasher,
        SCT: SignatureCollection,
        VT: ValidatorSetType,
    {
    }
    fn get_leader<VT>(&self, round: Round, valset: &VT) -> Option<NodeId>
    where
        VT: ValidatorSetType;
}

pub(crate) use chorus::types::NodeId;
pub(crate) use monad_mcp_chorus::stub as chorus;

// The keypair used to sign/verify proposal. Not used for aggregation.
pub struct ProposalKeyPair(NodeId);

// todo: simulate signature validation
pub struct ProposalSignature;

pub struct OpaqueHeader {
    merkle_proof: Vec<chorus::types::MerkleRoot>,
}

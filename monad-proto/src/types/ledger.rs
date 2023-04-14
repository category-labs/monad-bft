use monad_consensus::types::ledger::LedgerCommitInfo;

use crate::error::ProtoError;

include!(concat!(env!("OUT_DIR"), "/monad_proto.ledger.rs"));

impl From<&LedgerCommitInfo> for ProtoLedgerCommitInfo {
    fn from(value: &LedgerCommitInfo) -> Self {
        ProtoLedgerCommitInfo {
            commit_state_hash: value.commit_state_hash.map(|csh| csh.to_vec()),
            vote_info_hash: value.vote_info_hash.to_vec(),
        }
    }
}
impl TryFrom<ProtoLedgerCommitInfo> for LedgerCommitInfo {
    type Error = ProtoError;
    fn try_from(proto_lci: ProtoLedgerCommitInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            commit_state_hash: proto_lci
                .commit_state_hash
                .map(|csh: Vec<_>| {
                    csh.try_into().map_err(|e: Vec<_>| {
                        Self::Error::WrongHashLen(format!("commit state hash {}", e.len()))
                    })
                })
                .transpose()?,
            vote_info_hash: proto_lci.vote_info_hash.try_into().map_err(|e: Vec<_>| {
                Self::Error::WrongHashLen(format!("vote info hash {}", e.len()))
            })?,
        })
    }
}

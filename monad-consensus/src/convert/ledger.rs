use monad_proto::error::ProtoError;
use monad_proto::proto::ledger::*;

use crate::types::ledger::LedgerCommitInfo;

impl From<&LedgerCommitInfo> for ProtoLedgerCommitInfo {
    fn from(value: &LedgerCommitInfo) -> Self {
        ProtoLedgerCommitInfo {
            commit_state_hash: value.commit_state_hash.as_ref().map(|v| v.into()),
            vote_info_hash: Some((&(value.vote_info_hash)).into()),
        }
    }
}

impl TryFrom<ProtoLedgerCommitInfo> for LedgerCommitInfo {
    type Error = ProtoError;
    fn try_from(proto_lci: ProtoLedgerCommitInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            commit_state_hash: proto_lci
                .commit_state_hash
                .map(|v| v.try_into())
                .transpose()?,
            vote_info_hash: proto_lci
                .vote_info_hash
                .ok_or(Self::Error::MissingRequiredField(
                    "LedgerCommitInfo.vote_info_hash".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

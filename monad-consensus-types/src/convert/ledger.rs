use monad_proto::{error::ProtoError, proto::ledger::*};

use crate::ledger::LedgerCommitInfo;

impl From<&LedgerCommitInfo> for ProtoLedgerCommitInfo {
    fn from(value: &LedgerCommitInfo) -> Self {
        ProtoLedgerCommitInfo {
            commit_state_hash: value.commit_state_hash.as_ref().map(|v| v.into()),
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
        })
    }
}

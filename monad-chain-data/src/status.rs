use crate::{
    logs::types::LogSequencingState, store::publication::FinalizedHeadState,
    traces::TraceSequencingState, txs::TxFamilyState,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceStatus {
    pub head_state: FinalizedHeadState,
    pub log_state: LogSequencingState,
    pub tx_state: TxFamilyState,
    pub trace_state: TraceSequencingState,
}

use monad_proto::error::ProtoError;
use monad_proto::proto::pacemaker::ProtoPacemakerTimerExpire;

use crate::pacemaker::PacemakerTimerExpire;

impl From<&PacemakerTimerExpire> for ProtoPacemakerTimerExpire {
    fn from(_value: &PacemakerTimerExpire) -> Self {
        Self {}
    }
}

impl TryFrom<ProtoPacemakerTimerExpire> for PacemakerTimerExpire {
    type Error = ProtoError;
    fn try_from(_value: ProtoPacemakerTimerExpire) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}

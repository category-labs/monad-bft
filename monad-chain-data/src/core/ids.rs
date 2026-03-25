use crate::core::layout::{LOCAL_ID_BITS, LOCAL_ID_MASK, MAX_LOCAL_ID};

const MAX_FAMILY_SHARD: u64 = u64::MAX >> LOCAL_ID_BITS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FamilyId(u64);

impl FamilyId {
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn shard_raw(self) -> u64 {
        self.0 >> LOCAL_ID_BITS
    }

    pub const fn local_raw(self) -> u32 {
        (self.0 & LOCAL_ID_MASK) as u32
    }

    pub const fn compose(shard_raw: u64, local_raw: u32) -> Self {
        Self((shard_raw << LOCAL_ID_BITS) | (local_raw as u64))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidFamilyShard {
    raw: u64,
}

impl InvalidFamilyShard {
    pub const fn raw(self) -> u64 {
        self.raw
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidFamilyLocalId {
    raw: u32,
}

impl InvalidFamilyLocalId {
    pub const fn raw(self) -> u32 {
        self.raw
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FamilyShard(u64);

impl FamilyShard {
    pub fn new(raw: u64) -> Result<Self, InvalidFamilyShard> {
        if raw <= MAX_FAMILY_SHARD {
            Ok(Self(raw))
        } else {
            Err(InvalidFamilyShard { raw })
        }
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) const fn new_masked(raw: u64) -> Self {
        Self(raw)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FamilyLocalId(u32);

impl FamilyLocalId {
    pub fn new(raw: u32) -> Result<Self, InvalidFamilyLocalId> {
        if raw <= MAX_LOCAL_ID {
            Ok(Self(raw))
        } else {
            Err(InvalidFamilyLocalId { raw })
        }
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub(crate) const fn new_masked(raw: u32) -> Self {
        Self(raw)
    }
}

macro_rules! define_family_id_types {
    ($id:ident, $shard:ident, $invalid_shard:ident, $local:ident, $invalid_local:ident, $compose:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $id(FamilyId);

        impl $id {
            pub const fn new(raw: u64) -> Self {
                Self(FamilyId::new(raw))
            }

            pub const fn from_family_id(value: FamilyId) -> Self {
                Self(value)
            }

            pub const fn into_family_id(self) -> FamilyId {
                self.0
            }

            pub const fn get(self) -> u64 {
                self.0.get()
            }

            pub const fn shard(self) -> $shard {
                $shard::from_family_shard(FamilyShard::new_masked(self.0.shard_raw()))
            }

            pub const fn local(self) -> $local {
                $local::from_family_local_id(FamilyLocalId::new_masked(self.0.local_raw()))
            }

            pub const fn split(self) -> ($shard, $local) {
                (self.shard(), self.local())
            }
        }

        impl From<u64> for $id {
            fn from(value: u64) -> Self {
                Self::new(value)
            }
        }

        impl From<$id> for u64 {
            fn from(value: $id) -> Self {
                value.get()
            }
        }

        impl Default for $id {
            fn default() -> Self {
                Self::new(0)
            }
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $shard(FamilyShard);

        impl $shard {
            pub fn new(raw: u64) -> Result<Self, $invalid_shard> {
                FamilyShard::new(raw)
                    .map(Self)
                    .map_err(|err| $invalid_shard { raw: err.raw() })
            }

            pub const fn from_family_shard(value: FamilyShard) -> Self {
                Self(value)
            }

            pub const fn into_family_shard(self) -> FamilyShard {
                self.0
            }

            pub const fn get(self) -> u64 {
                self.0.get()
            }
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $invalid_shard {
            raw: u64,
        }

        impl $invalid_shard {
            pub const fn raw(self) -> u64 {
                self.raw
            }
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $invalid_local {
            raw: u32,
        }

        impl $invalid_local {
            pub const fn raw(self) -> u32 {
                self.raw
            }
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $local(FamilyLocalId);

        impl $local {
            pub fn new(raw: u32) -> Result<Self, $invalid_local> {
                FamilyLocalId::new(raw)
                    .map(Self)
                    .map_err(|err| $invalid_local { raw: err.raw() })
            }

            pub const fn from_family_local_id(value: FamilyLocalId) -> Self {
                Self(value)
            }

            pub const fn into_family_local_id(self) -> FamilyLocalId {
                self.0
            }

            pub const fn get(self) -> u32 {
                self.0.get()
            }
        }

        pub const fn $compose(shard: $shard, local: $local) -> $id {
            $id::from_family_id(FamilyId::compose(shard.get(), local.get()))
        }
    };
}

define_family_id_types!(
    LogId,
    LogShard,
    InvalidLogShard,
    LogLocalId,
    InvalidLogLocalId,
    compose_log_id
);
define_family_id_types!(
    TxId,
    TxShard,
    InvalidTxShard,
    TxLocalId,
    InvalidTxLocalId,
    compose_tx_id
);
define_family_id_types!(
    TraceId,
    TraceShard,
    InvalidTraceShard,
    TraceLocalId,
    InvalidTraceLocalId,
    compose_trace_id
);

pub trait FamilyIdValue: Copy + Ord {
    fn new(raw: u64) -> Self;
    fn get(self) -> u64;
}

pub fn family_local_range_for_shard<I: FamilyIdValue>(
    from: I,
    to_inclusive: I,
    shard_raw: u64,
) -> (u32, u32) {
    let from_shard = from.get() >> LOCAL_ID_BITS;
    let to_shard = to_inclusive.get() >> LOCAL_ID_BITS;
    let local_from = if shard_raw == from_shard {
        (from.get() & LOCAL_ID_MASK) as u32
    } else {
        0
    };
    let local_to = if shard_raw == to_shard {
        (to_inclusive.get() & LOCAL_ID_MASK) as u32
    } else {
        MAX_LOCAL_ID
    };
    (local_from, local_to)
}

impl FamilyIdValue for LogId {
    fn new(raw: u64) -> Self {
        Self::new(raw)
    }

    fn get(self) -> u64 {
        self.get()
    }
}

impl FamilyIdValue for TxId {
    fn new(raw: u64) -> Self {
        Self::new(raw)
    }

    fn get(self) -> u64 {
        self.get()
    }
}

impl FamilyIdValue for TraceId {
    fn new(raw: u64) -> Self {
        Self::new(raw)
    }

    fn get(self) -> u64 {
        self.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FamilyIdRange<I> {
    pub start: I,
    pub end_inclusive: I,
}

impl<I: FamilyIdValue> FamilyIdRange<I> {
    pub fn new(start: I, end_inclusive: I) -> Option<Self> {
        (start <= end_inclusive).then_some(Self {
            start,
            end_inclusive,
        })
    }

    pub fn contains(&self, id: I) -> bool {
        self.start <= id && id <= self.end_inclusive
    }

    pub fn resume_strictly_after(&self, id: I) -> Option<Self> {
        let next_start = id.get().checked_add(1).map(I::new)?;
        Self::new(next_start, self.end_inclusive)
    }
}

pub type PrimaryIdRange = FamilyIdRange<LogId>;
pub type TraceIdRange = FamilyIdRange<TraceId>;

#[cfg(test)]
mod tests {
    use super::{
        compose_log_id, compose_trace_id, FamilyId, FamilyIdRange, LogId, LogLocalId, LogShard,
        PrimaryIdRange, TraceId, TraceIdRange, TraceLocalId, TraceShard,
    };
    use crate::core::layout::MAX_LOCAL_ID;

    #[test]
    fn family_id_roundtrips_shard_and_local() {
        let value = FamilyId::compose(u64::from(u32::MAX) + 1, 7);
        assert_eq!(value.shard_raw(), u64::from(u32::MAX) + 1);
        assert_eq!(value.local_raw(), 7);
    }

    #[test]
    fn log_id_roundtrips_at_boundaries() {
        let values = [
            LogId::new(0),
            LogId::new(1),
            LogId::new(u64::from(MAX_LOCAL_ID)),
            LogId::new(u64::from(MAX_LOCAL_ID) + 1),
            compose_log_id(
                LogShard::new(u64::from(u32::MAX) + 1).expect("valid shard"),
                LogLocalId::new(7).expect("valid local id"),
            ),
        ];

        for value in values {
            let (shard, local) = value.split();
            assert_eq!(compose_log_id(shard, local), value);
            assert_eq!(
                value.into_family_id(),
                FamilyId::compose(shard.get(), local.get())
            );
        }
    }

    #[test]
    fn primary_id_range_uses_typed_log_ids() {
        let range = PrimaryIdRange::new(LogId::new(10), LogId::new(12)).expect("valid range");
        assert!(range.contains(LogId::new(10)));
        assert!(range.contains(LogId::new(12)));
        assert!(!range.contains(LogId::new(13)));
    }

    #[test]
    fn trace_id_range_uses_typed_trace_ids() {
        let range = TraceIdRange::new(TraceId::new(10), TraceId::new(12)).expect("valid range");
        assert!(range.contains(TraceId::new(10)));
        assert!(range.contains(TraceId::new(12)));
        assert!(!range.contains(TraceId::new(13)));
        assert_eq!(
            range.resume_strictly_after(TraceId::new(10)),
            Some(TraceIdRange {
                start: TraceId::new(11),
                end_inclusive: TraceId::new(12),
            })
        );
    }

    #[test]
    fn typed_ranges_reject_reversed_bounds() {
        assert_eq!(FamilyIdRange::new(LogId::new(12), LogId::new(10)), None);
    }

    #[test]
    fn trace_id_roundtrips_at_boundaries() {
        let values = [
            TraceId::new(0),
            TraceId::new(1),
            TraceId::new(u64::from(MAX_LOCAL_ID)),
            TraceId::new(u64::from(MAX_LOCAL_ID) + 1),
            compose_trace_id(
                TraceShard::new(u64::from(u32::MAX) + 1).expect("valid shard"),
                TraceLocalId::new(7).expect("valid local id"),
            ),
        ];

        for value in values {
            let (shard, local) = value.split();
            assert_eq!(compose_trace_id(shard, local), value);
            assert_eq!(
                value.into_family_id(),
                FamilyId::compose(shard.get(), local.get())
            );
        }
    }
}

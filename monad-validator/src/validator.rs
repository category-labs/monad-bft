use std::fmt::Debug;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Address(pub i64); // placeholder type for Address
#[derive(Clone, Copy, Debug)]
pub struct PubKey(pub i64); // placeholder type
#[derive(Clone, Copy)]
pub struct Stake(pub i64);

#[derive(Clone, Copy)]
pub struct Validator {
    pub address: Address,
    pub pubkey: PubKey,
    pub stake: Stake,
}

impl Debug for Validator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "validator {:?}", self.address.0)
    }
}

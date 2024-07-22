use libfuzzer_sys::arbitrary::{Arbitrary, Error, Unstructured};
use monad_types::{Round, SeqNum};

#[derive(Clone, Debug)]
pub struct VerifyBlockData {
    pub block_round: Round,
    pub qc_round: Round,
    pub parent_round: Round,
    pub qc_seq_num: SeqNum,
    pub block_seq_num: SeqNum,
}

impl<'a> Arbitrary<'a> for VerifyBlockData {
    fn arbitrary(raw: &mut Unstructured<'a>) -> Result<Self, Error> {
        let mut buf = [0; 40];
        raw.fill_buffer(&mut buf)?;

        Ok(VerifyBlockData {
            block_round: Round(u64::from_ne_bytes(
                buf[0..8].try_into().expect("slice with incorrect length"),
            )),
            qc_round: Round(u64::from_ne_bytes(
                buf[8..16].try_into().expect("slice with incorrect length"),
            )),
            parent_round: Round(u64::from_ne_bytes(
                buf[16..24].try_into().expect("slice with incorrect length"),
            )),
            qc_seq_num: SeqNum(u64::from_ne_bytes(
                buf[24..32].try_into().expect("slice with incorrect length"),
            )),
            block_seq_num: SeqNum(u64::from_ne_bytes(
                buf[32..40].try_into().expect("slice with incorrect length"),
            )),
        })
    }
}

impl VerifyBlockData {
    pub fn unwrap(self) -> (Round, Round, Round, SeqNum, SeqNum) {
        (
            self.block_round,
            self.qc_round,
            self.parent_round,
            self.qc_seq_num,
            self.block_seq_num,
        )
    }
}

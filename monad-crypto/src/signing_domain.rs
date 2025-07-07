pub trait SigningDomain {
    // first byte must be the length of the following message
    // the length of the following message must be < 128
    // last byte must be \n
    const PREFIX: &'static [u8];
}

#[allow(dead_code)]
const fn assert_signing_prefix<SD: SigningDomain>() {
    let prefix_len = SD::PREFIX[0];

    // "For a single byte whose value is in the [0, 127] range, that byte is its own RLP encoding."
    assert!(prefix_len < 128);
    assert!(prefix_len as usize == SD::PREFIX.len() - 1);

    let last_byte = SD::PREFIX[SD::PREFIX.len() - 1];
    assert!(last_byte == b'\n');
}

pub struct ConsensusMessage;
const _: () = assert_signing_prefix::<ConsensusMessage>();
impl SigningDomain for ConsensusMessage {
    const PREFIX: &'static [u8] = b"\x1Amonad/consensus-message/1\n";
}

pub struct Tip;
const _: () = assert_signing_prefix::<Tip>();
impl SigningDomain for Tip {
    const PREFIX: &'static [u8] = b"\x0Cmonad/tip/1\n";
}

pub struct Vote;
const _: () = assert_signing_prefix::<Vote>();
impl SigningDomain for Vote {
    const PREFIX: &'static [u8] = b"\x0Dmonad/vote/1\n";
}

pub struct Timeout;
const _: () = assert_signing_prefix::<Timeout>();
impl SigningDomain for Timeout {
    const PREFIX: &'static [u8] = b"\x10monad/timeout/1\n";
}

pub struct NoEndorsement;
const _: () = assert_signing_prefix::<NoEndorsement>();
impl SigningDomain for NoEndorsement {
    const PREFIX: &'static [u8] = b"\x17monad/no-endorsement/1\n";
}

pub struct RoundSignature;
const _: () = assert_signing_prefix::<RoundSignature>();
impl SigningDomain for RoundSignature {
    const PREFIX: &'static [u8] = b"\x18monad/round-signature/1\n";
}

pub struct NameRecord;
const _: () = assert_signing_prefix::<NameRecord>();
impl SigningDomain for NameRecord {
    const PREFIX: &'static [u8] = b"\x14monad/name-record/1\n";
}

pub struct RaptorcastAppMessage;
const _: () = assert_signing_prefix::<RaptorcastAppMessage>();
impl SigningDomain for RaptorcastAppMessage {
    const PREFIX: &'static [u8] = b"\x1Fmonad/raptorcast-app-message/1\n";
}

pub struct RaptorcastChunk;
const _: () = assert_signing_prefix::<RaptorcastChunk>();
impl SigningDomain for RaptorcastChunk {
    const PREFIX: &'static [u8] = b"\x19monad/raptorcast-chunk/1\n";
}

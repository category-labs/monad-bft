/// Opaque shell for the later zero-copy transaction view type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxRef {
    _private: (),
}

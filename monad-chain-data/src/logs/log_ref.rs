/// Opaque shell for the later zero-copy log view type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRef {
    _private: (),
}

/// Opaque shell for the later zero-copy trace view type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceRef {
    _private: (),
}

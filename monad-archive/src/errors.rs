use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArchiveError {
    pub message: String,
}

impl ArchiveError {
    pub fn custom_error(message: String) -> Self {
        Self { message: (message) }
    }

    pub fn generic_error() -> Self {
        Self {
            message: "generic error".into(),
        }
    }
}

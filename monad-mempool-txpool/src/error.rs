use thiserror::Error;

#[derive(Error, Debug)]
pub enum PoolError {
    #[error("Transaction already exists in pool")]
    DuplicateTransactionError,
}

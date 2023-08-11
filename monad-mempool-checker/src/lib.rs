use ethers::{
    types::{
        transaction::{
            eip2718::{TypedTransaction, TypedTransactionError},
            eip2930::AccessList,
        },
        Bytes, SignatureError, U256,
    },
    utils::rlp,
};
use monad_mempool_types::tx::EthTx;
use thiserror::Error;
use tracing::error;

const TX_GAS_CONTRACT_CREATION: u64 = 53000;
const TX_GAS: u64 = 21000;
const TX_NON_ZERO_DATA_COST_FRONTIER: u64 = 68;
// In EIP2028, the cost of non-zero bytes was reduced to 16 gas
const TX_NON_ZERO_DATA_COST_EIP2028: u64 = 16;
const TX_ZERO_DATA_COST: u64 = 4;
// Cost of warming up storage access
const TX_ACCESS_LIST_STORAGE_GAS: u64 = 1900;
// Cost of warming up  access
const TX_ACCESS_LIST_ADDRESS_GAS: u64 = 2400;

const TX_MAX_SIZE: usize = 128 * 1024;
const BLOCK_GAS_LIMIT: u64 = 30000000;

#[derive(Error, Debug)]
pub enum CheckerError {
    #[error("error decoding transaction from rlp data ({0})")]
    TransactionDecodeError(TypedTransactionError),
    #[error("transaction is too big")]
    OversizedDataError,
    #[error("transaction gas exceeds current block limit")]
    GasLimitError,
    #[error("invalid sender recovered ({0})")]
    InvalidSenderError(SignatureError),
    #[error("overflow calculating intrinsic gas")]
    GasOverflowError,
    #[error("gas must be greater than intrinsic gas")]
    GasTooLowError,
    #[error("transaction hash mismatch")]
    MismatchedHashError,
}

pub struct CheckerConfig {
    gas_limit: U256,
    skip_check_tx: bool,
}

impl Default for CheckerConfig {
    fn default() -> Self {
        Self {
            gas_limit: BLOCK_GAS_LIMIT.into(),
            skip_check_tx: false,
        }
    }
}

impl CheckerConfig {
    pub fn with_gas_limit(mut self, gas_limit: U256) -> Self {
        self.gas_limit = gas_limit;
        self
    }

    pub fn with_skip_check_tx(mut self, skip_check_tx: bool) -> Self {
        self.skip_check_tx = skip_check_tx;
        self
    }
}

#[derive(Clone)]
pub struct Checker {
    gas_limit: U256,
    skip_check_tx: bool,
}

impl Checker {
    pub fn new(config: &CheckerConfig) -> Self {
        Self {
            gas_limit: config.gas_limit,
            skip_check_tx: config.skip_check_tx,
        }
    }

    pub fn check_eth_tx(&self, eth_tx: &EthTx) -> Result<(), CheckerError> {
        let rlp = rlp::Rlp::new(eth_tx.rlpdata.as_slice());
        // This also ensures that sender is valid by recovering it from the signature
        let (typed_tx, signature) =
            TypedTransaction::decode_signed(&rlp).map_err(CheckerError::TransactionDecodeError)?;

        if !self.skip_check_tx {
            if eth_tx.hash != typed_tx.hash(&signature).as_bytes() {
                return Err(CheckerError::MismatchedHashError);
            }
            self.check_typed_tx(&typed_tx)?;
        }

        Ok(())
    }

    fn check_typed_tx(&self, tx: &TypedTransaction) -> Result<(), CheckerError> {
        // Reject transactions with data over defined size to prevent DOS attacks
        if tx.rlp().len() > TX_MAX_SIZE {
            return Err(CheckerError::OversizedDataError);
        }
        // If gas is None, we assume a sensible default
        if let Some(g) = tx.gas() {
            // Ensure the transaction doesn't exceed the current block gas limit
            if *g > self.gas_limit {
                return Err(CheckerError::GasLimitError);
            }

            // Gas is always less than 2^64, so this should not overflow
            let intrinsic_gas =
                intrinsic_gas(&tx.data(), &tx.access_list(), tx.to().is_none(), true, true)?;
            // Transaction must have more gas than the intrinsic gas
            if *g < intrinsic_gas.into() {
                return Err(CheckerError::GasTooLowError);
            }
        }

        Ok(())
    }
}

pub fn intrinsic_gas(
    data: &Option<&Bytes>,
    access_list: &Option<&AccessList>,
    is_contract_creation: bool,
    is_homestead: bool,
    is_eip2028: bool,
) -> Result<u64, CheckerError> {
    let mut gas: u64;
    if is_contract_creation && is_homestead {
        gas = TX_GAS_CONTRACT_CREATION;
    } else {
        gas = TX_GAS;
    }

    if let Some(data) = *data {
        // Count of non-zero bytes in the data
        let mut nz = 0;
        // Count of zero bytes in the data
        let mut z: u64 = 0;
        for b in data {
            if *b == 0 {
                z += 1;
            } else {
                nz += 1;
            }
        }

        let nz_gas_cost = if is_eip2028 {
            TX_NON_ZERO_DATA_COST_EIP2028
        } else {
            TX_NON_ZERO_DATA_COST_FRONTIER
        };
        let nz_gas = u64::checked_mul(nz, nz_gas_cost).ok_or(CheckerError::GasOverflowError)?;
        gas = u64::checked_add(gas, nz_gas).ok_or(CheckerError::GasOverflowError)?;

        let z_gas = u64::checked_mul(z, TX_ZERO_DATA_COST).ok_or(CheckerError::GasOverflowError)?;
        gas = u64::checked_add(gas, z_gas).ok_or(CheckerError::GasOverflowError)?;
    }

    if let Some(access_list) = access_list {
        let access_gas = u64::checked_mul(access_list.0.len() as u64, TX_ACCESS_LIST_ADDRESS_GAS)
            .ok_or(CheckerError::GasOverflowError)?;
        let storage_keys = access_list
            .0
            .iter()
            .map(|item| item.storage_keys.len())
            .sum::<usize>() as u64;
        let storage_gas = u64::checked_mul(storage_keys, TX_ACCESS_LIST_STORAGE_GAS)
            .ok_or(CheckerError::GasOverflowError)?;

        gas = u64::checked_add(gas, access_gas).ok_or(CheckerError::GasOverflowError)?;
        gas = u64::checked_add(gas, storage_gas).ok_or(CheckerError::GasOverflowError)?;
    }

    Ok(gas)
}

#[cfg(test)]
mod test {
    use ethers::{
        signers::LocalWallet,
        types::{transaction::eip2718::TypedTransaction, Address, TransactionRequest, U256},
        utils::keccak256,
    };
    use monad_mempool_types::tx::EthTx;

    use super::{Checker, CheckerConfig, CheckerError};

    const LOCAL_TEST_KEY: &str = "046507669b0b9d460fe9d48bb34642d85da927c566312ea36ac96403f0789b69";

    fn create_valid_tx(gas: Option<U256>, data: Option<Vec<u8>>) -> EthTx {
        let tx = create_typed_transaction(gas, data);

        let wallet = LOCAL_TEST_KEY.parse::<LocalWallet>().unwrap();
        let signature = wallet.sign_transaction_sync(&tx).unwrap();

        EthTx {
            hash: tx.hash(&signature).as_bytes().to_vec(),
            rlpdata: tx.rlp_signed(&signature).to_vec(),
        }
    }

    fn create_typed_transaction(gas: Option<U256>, data: Option<Vec<u8>>) -> TypedTransaction {
        TransactionRequest::new()
            .to("0xc582768697b4a6798f286a03A2A774c8743163BB"
                .parse::<Address>()
                .unwrap())
            .gas(gas.unwrap_or(21337.into()))
            .gas_price(42)
            .value(31415)
            .data(data.unwrap_or_default())
            .nonce(1)
            .chain_id(15446)
            .into()
    }

    #[test]
    fn checker() {
        let checker = Checker::new(&CheckerConfig::default());

        // EthTx with invalid rlp
        // Should throw TransactionDecodeError
        let mut tx_invalid_rlp = create_valid_tx(None, None);
        tx_invalid_rlp.rlpdata = vec![0x01];

        // EthTx with invalid hash
        // Should throw MismatchedHashError
        let mut tx_mismatched_hash = create_valid_tx(None, None);
        tx_mismatched_hash.hash = vec![0x01];

        // EthTx with oversized tx
        // Should throw OversizedDataError
        let tx_oversized = create_valid_tx(None, Some(vec![1; 256 * 1024]));

        // EthTx with gas limit greater than block gas limit
        // Should throw GasLimitError
        let tx_gas_limit = create_valid_tx(Some(100000000.into()), None);

        // EthTx with gas limit less than intrinsic gas
        // Should throw GasTooLowError
        let tx_gas_too_low = create_valid_tx(Some(0.into()), None);

        // EthTx with an invalid signature
        // Should throw TransactionDecodeError
        let wallet = LOCAL_TEST_KEY.parse::<LocalWallet>().unwrap();
        let tx_invalid_sender = create_typed_transaction(None, None);
        let signature = wallet.sign_transaction_sync(&tx_invalid_sender).unwrap();
        let mut rlp_signed = tx_invalid_sender.rlp_signed(&signature).to_vec();
        // Since the signature for a signed rlp is appended to the end of the tx rlp, we modify a sequence
        // of bytes at the end of the rlp to invalidate the signature.
        rlp_signed.splice(rlp_signed.len() - 40..rlp_signed.len() - 20, [0x2; 20]);
        let tx_invalid_sender = EthTx {
            hash: keccak256(&rlp_signed).into(),
            rlpdata: rlp_signed,
        };

        let tx_valid = create_valid_tx(None, None);

        assert!(matches!(
            checker.check_eth_tx(&tx_invalid_rlp).unwrap_err(),
            CheckerError::TransactionDecodeError(_)
        ));
        assert!(matches!(
            checker.check_eth_tx(&tx_mismatched_hash).unwrap_err(),
            CheckerError::MismatchedHashError
        ));
        assert!(matches!(
            checker.check_eth_tx(&tx_oversized).unwrap_err(),
            CheckerError::OversizedDataError
        ));
        assert!(matches!(
            checker.check_eth_tx(&tx_gas_limit).unwrap_err(),
            CheckerError::GasLimitError
        ));
        assert!(matches!(
            checker.check_eth_tx(&tx_gas_too_low).unwrap_err(),
            CheckerError::GasTooLowError
        ));
        assert!(matches!(
            checker.check_eth_tx(&tx_invalid_sender).unwrap_err(),
            CheckerError::TransactionDecodeError(_)
        ));

        assert!(checker.check_eth_tx(&tx_valid).is_ok());
    }
}

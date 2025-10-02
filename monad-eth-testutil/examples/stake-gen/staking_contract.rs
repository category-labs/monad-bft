use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::{hex, Address, Bytes, FixedBytes, TxHash, TxKind, U256};
use alloy_rlp::{BytesMut, Encodable};
use alloy_rpc_client::{ClientBuilder, ReqwestClient};
use alloy_signer::SignerSync;
use alloy_signer_local::LocalSigner;
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolType};
use monad_crypto::hasher::{Blake3Hash, Hasher};
use monad_types::Epoch;
use secp256k1::KeyPair as SecpKeyPair;
use serde_json::json;
use url::Url;

use crate::{Delegator, NodeData};

const BLS_DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";

const STAKING_CONTRACT_ADDRESS: Address =
    Address::new(hex!("0x0000000000000000000000000000000000001000"));

pub const MON: u64 = 1_000_000_000_000_000_000;
pub const MIN_STAKE_MON: u64 = 1_000_000;
pub const ACTIVE_VALIDATOR_STAKE_MON: u64 = 50_000_000;

const ADD_VALIDATOR_FUNCTION_SELECTOR: FixedBytes<4> = FixedBytes::new(hex!("0xf145204c"));
const DELEGATE_FUNCTION_SELECTOR: FixedBytes<4> = FixedBytes::new(hex!("0x84994fec"));
const UNDELEGATE_FUNCTION_SELECTOR: FixedBytes<4> = FixedBytes::new(hex!("0x5cf41514"));

sol! {
    contract ExecutionStakingContract {
        // Write functions
        function addValidator(bytes payload, bytes secpSig, bytes blsSig) external payable returns (uint256 validatorId);
        function changeCommission(uint64 validatorId, uint256 commission) external returns (bool success);
        function delegate(uint64 validatorId) external payable returns (bool success);
        function undelegate(uint64 validatorId, uint256 amount, uint8 withdrawId) external returns (bool success);
        function withdraw(uint64 validatorId, uint8 withdrawId) external returns (bool success);
        function compound(uint64 validatorId) external returns (bool success);
        function claimRewards(uint64 validatorId) external returns (bool success);

        // Read functions
        function getValidator(uint64 validatorId) external view returns (Validator memory);
        function getDelegator(uint64 validatorId, address delegator) external view returns (Delegation memory);
        function getWithdrawalRequest(uint64 validatorId, address delegator, uint8 withdrawId) external view returns(Withdraw memory);
        function getConsensusValidatorSet(uint32 startIndex) external view returns (bool atEnd, uint32 nextStartIndex, uint256[] memory validatorIds);
        function getSnapshotValidatorSet(uint32 startIndex) external view returns (bool atEnd, uint32 nextStartIndex, uint256[] memory validatorIds);
        function getExecutionValidatorSet(uint32 startIndex) external view returns (bool atEnd, uint32 nextStartIndex, uint256[] memory validatorIds);
        function getDelegations(address delegator, uint64 startValidatorId) external view returns (bool atEnd, uint32 nextStartIndex, uint256[] memory validatorIds);
        function getDelegators(uint64 validatorId, address startAddress) external returns (bool atEnd, uint32 nextStartIndex, address[] memory delegators);
        function getEpoch() external view returns (uint256 epoch, bool inEpochDelayPeriod);

        // Syscalls
        function syscallOnEpochChange(uint64 epoch) external;
        function syscallReward(address blockAuthor) external;
        function syscallSnapshot() external;

        // Structs
        struct Validator {
            address authAddress;
            uint256 flags;
            uint256 stake;
            uint256 accumulatedRewardsPerToken;
            uint256 commission;
            uint256 unclaimedRewards;
            uint256 consensusStake;
            uint256 consensusCommission;
            uint256 snapshotStake;
            uint256 snapshotCommission;
            bytes secpPubkey;
            bytes blsPubkey;
        }

        struct Delegation {
            uint256 stake;
            uint256 accumulatedRewardsPerToken;
            uint256 rewards;
            uint256 deltaStake;
            uint256 nextDeltaStake;
            uint256 deltaEpoch;
            uint256 nextDeltaEpoch;
        }

        struct Withdraw {
            uint256 amount;
            uint256 accumulatedRewardsPerToken;
            uint256 epoch;
        }
    }
}

impl std::fmt::Debug for ExecutionStakingContract::Validator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Validator")
            .field("auth_address", &self.authAddress)
            .field("flags", &self.flags)
            .field("stake", &self.stake)
            .field(
                "accumulated_rewards_per_token",
                &self.accumulatedRewardsPerToken,
            )
            .field("commission", &self.commission)
            .field("unclaimed_rewards", &self.unclaimedRewards)
            .field("consensus_stake", &self.consensusStake)
            .field("consensus_commission", &self.consensusCommission)
            .field("snapshot_stake", &self.snapshotStake)
            .field("snapshot_commission", &self.snapshotCommission)
            .field("secp_pubkey", &self.secpPubkey)
            .field("bls_pubkey", &self.blsPubkey)
            .finish()
    }
}

impl std::fmt::Debug for ExecutionStakingContract::Delegation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delegation")
            .field("stake", &self.stake)
            .field(
                "accumulated_rewards_per_token",
                &self.accumulatedRewardsPerToken,
            )
            .field("rewards", &self.rewards)
            .field("delta_stake", &self.deltaStake)
            .field("next_delta_stake", &self.nextDeltaStake)
            .field("delta_epoch", &self.deltaEpoch)
            .field("next_delta_epoch", &self.nextDeltaEpoch)
            .finish()
    }
}

impl std::fmt::Debug for ExecutionStakingContract::Withdraw {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Withdraw")
            .field("amount", &self.amount)
            .field(
                "accumulated_rewards_per_token",
                &self.accumulatedRewardsPerToken,
            )
            .field("epoch", &self.epoch)
            .finish()
    }
}

pub struct StakingContract {
    pub client: ReqwestClient,
    pub chain_id: u64,
}

impl StakingContract {
    pub fn new(rpc_url: Url, chain_id: u64) -> Self {
        Self {
            client: ClientBuilder::default().http(rpc_url),
            chain_id,
        }
    }

    pub fn generate_add_validator_data(
        node: &NodeData,
        amount: U256,
        auth_address: Option<Address>,
        commission: Option<U256>,
    ) -> Bytes {
        let secp_keypair = SecpKeyPair::from_secret_key(secp256k1::SECP256K1, &node.secp_privkey);
        let bls_privkey = &node.bls_privkey;

        let auth_address = auth_address.unwrap_or(node.eth_address);
        let commission = commission.unwrap_or(U256::ZERO);

        let payload_parts = [
            node.secp_pubkey.serialize().to_vec(),
            node.bls_pubkey.compress().to_vec(),
            auth_address.0.to_vec(),
            amount.to_be_bytes_vec(),
            commission.to_be_bytes_vec(),
        ];
        let payload = payload_parts.concat();

        let mut hasher = Blake3Hash::new();
        hasher.update(&payload);
        let msg = secp256k1::Message::from_slice(&hasher.hash().0).expect("32 bytes");
        let secp_sig = secp_keypair
            .secret_key()
            .sign_ecdsa(msg)
            .serialize_compact();

        let bls_sig = bls_privkey.sign(&payload, BLS_DST, &[]).to_bytes();

        let args = ExecutionStakingContract::addValidatorCall {
            payload: payload.into(),
            secpSig: secp_sig.into(),
            blsSig: bls_sig.into(),
        };

        args.abi_encode().into()
    }

    pub async fn add_validator(
        &self,
        node: &mut NodeData,
        amount: U256,
        auth_address: Option<Address>,
        commission: Option<U256>,
    ) -> Option<TxHash> {
        let add_validator_data =
            StakingContract::generate_add_validator_data(node, amount, auth_address, commission);

        let mut nonce = node.nonce.borrow_mut();
        let tx = TxEip1559 {
            chain_id: self.chain_id,
            nonce: *nonce,
            gas_limit: 1_000_000,
            max_fee_per_gas: 100_000_000_000,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(STAKING_CONTRACT_ADDRESS),
            value: amount,
            access_list: Default::default(),
            input: add_validator_data,
        };

        let signer = LocalSigner::from_bytes(&FixedBytes::<32>::from_slice(
            &node.secp_privkey.secret_bytes(),
        ))
        .unwrap();

        let signature_hash = tx.signature_hash();
        let signature = signer.sign_hash_sync(&signature_hash).unwrap();
        let signed_tx = TxEnvelope::Eip1559(tx.into_signed(signature));

        let mut rlp_encoded_tx = BytesMut::new();
        signed_tx.encode(&mut rlp_encoded_tx);

        match self
            .client
            .request::<_, TxHash>(
                "eth_sendRawTransaction",
                &[format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await
        {
            Ok(tx_hash) => {
                *nonce += 1;
                Some(tx_hash)
            }
            Err(err) => {
                println!("add_validator RPC error: {}", err);
                None
            }
        }
    }

    fn generate_delegate_data(val_id: u64) -> Bytes {
        let payload = U256::from(val_id).to_be_bytes_vec();

        let delegate_data = [DELEGATE_FUNCTION_SELECTOR.to_vec(), payload];

        Bytes::from(delegate_data.concat())
    }

    pub async fn delegate(
        &self,
        delegator: &mut Delegator,
        val_id: u64,
        amount: U256,
    ) -> Option<TxHash> {
        let delegate_data = StakingContract::generate_delegate_data(val_id);

        let mut nonce = delegator.nonce.borrow_mut();
        let tx = TxEip1559 {
            chain_id: self.chain_id,
            nonce: *nonce,
            gas_limit: 1_000_000,
            max_fee_per_gas: 100_000_000_000,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(STAKING_CONTRACT_ADDRESS),
            value: amount,
            access_list: Default::default(),
            input: delegate_data,
        };

        let signer = LocalSigner::from_bytes(&FixedBytes::<32>::from_slice(
            &delegator.secp_privkey.secret_bytes(),
        ))
        .unwrap();

        let signature_hash = tx.signature_hash();
        let signature = signer.sign_hash_sync(&signature_hash).unwrap();
        let signed_tx = TxEnvelope::Eip1559(tx.into_signed(signature));

        let mut rlp_encoded_tx = BytesMut::new();
        signed_tx.encode(&mut rlp_encoded_tx);

        match self
            .client
            .request::<_, TxHash>(
                "eth_sendRawTransaction",
                &[format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await
        {
            Ok(tx_hash) => {
                *nonce += 1;
                Some(tx_hash)
            }
            Err(err) => {
                println!("delegate RPC error: {}", err);
                None
            }
        }
    }

    fn generate_undelegate_data(val_id: u64, amount: U256, withdraw_id: u8) -> Bytes {
        let payload_parts = [
            U256::from(val_id).to_be_bytes_vec(),
            amount.to_be_bytes_vec(),
            U256::from(withdraw_id).to_be_bytes_vec(),
        ];
        let payload = payload_parts.concat();

        let undelegate_data = [UNDELEGATE_FUNCTION_SELECTOR.to_vec(), payload];

        Bytes::from(undelegate_data.concat())
    }

    pub async fn undelegate(
        &self,
        delegator: &mut Delegator,
        val_id: u64,
        amount: U256,
    ) -> Option<TxHash> {
        let undelegate_data =
            StakingContract::generate_undelegate_data(val_id, amount, delegator.withdrawal_id);
        delegator.withdrawal_id += 1;

        let mut nonce = delegator.nonce.borrow_mut();
        let tx = TxEip1559 {
            chain_id: self.chain_id,
            nonce: *nonce,
            gas_limit: 1_000_000,
            max_fee_per_gas: 100_000_000_000,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(STAKING_CONTRACT_ADDRESS),
            value: U256::ZERO,
            access_list: Default::default(),
            input: undelegate_data,
        };

        let signer = LocalSigner::from_bytes(&FixedBytes::<32>::from_slice(
            &delegator.secp_privkey.secret_bytes(),
        ))
        .unwrap();

        let signature_hash = tx.signature_hash();
        let signature = signer.sign_hash_sync(&signature_hash).unwrap();
        let signed_tx = TxEnvelope::Eip1559(tx.into_signed(signature));

        let mut rlp_encoded_tx = BytesMut::new();
        signed_tx.encode(&mut rlp_encoded_tx);

        match self
            .client
            .request::<_, TxHash>(
                "eth_sendRawTransaction",
                &[format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await
        {
            Ok(tx_hash) => {
                *nonce += 1;
                Some(tx_hash)
            }
            Err(err) => {
                println!("undelegate RPC error: {}", err);
                None
            }
        }
    }

    pub async fn get_epoch(&self) -> (Epoch, bool) {
        let input = ExecutionStakingContract::getEpochCall {};
        let call = json!({
            "to": STAKING_CONTRACT_ADDRESS,
            "data": input.abi_encode()
        });

        let result: String = self.client.request("eth_call", [call]).await.unwrap();

        let bytes = hex::decode(result).unwrap();
        let get_epoch_return =
            ExecutionStakingContract::getEpochCall::abi_decode_returns(&bytes, true)
                .expect("decoding getEpoch return should pass");
        let epoch = get_epoch_return.epoch.to::<u64>();
        let in_epoch_delay = get_epoch_return.inEpochDelayPeriod;

        (Epoch(epoch), in_epoch_delay)
    }

    pub async fn get_validator(&self, val_id: u64) -> ExecutionStakingContract::Validator {
        let input = ExecutionStakingContract::getValidatorCall {
            validatorId: val_id,
        };
        let call = json!({
            "to": STAKING_CONTRACT_ADDRESS,
            "data": input.abi_encode()
        });

        let result: String = self.client.request("eth_call", [call]).await.unwrap();

        let bytes = hex::decode(result).unwrap();
        let get_validator_return =
            ExecutionStakingContract::Validator::abi_decode_params(&bytes, true)
                .expect("decoding getValidator return should pass");

        get_validator_return
    }

    pub async fn get_consensus_validator_set(&self) -> Vec<U256> {
        let input = ExecutionStakingContract::getConsensusValidatorSetCall { startIndex: 0 };
        let call = json!({
            "to": STAKING_CONTRACT_ADDRESS,
            "data": input.abi_encode()
        });

        let result: String = self.client.request("eth_call", [call]).await.unwrap();

        let bytes = hex::decode(result).unwrap();
        let get_validator_set_return =
            ExecutionStakingContract::getConsensusValidatorSetCall::abi_decode_returns(
                &bytes, true,
            )
            .expect("decoding getConsensusValidatorSet return should pass");
        let is_end = get_validator_set_return.atEnd;
        let _next_start_index = get_validator_set_return.nextStartIndex;
        // support paginated results
        assert!(is_end);

        let val_ids = get_validator_set_return.validatorIds;

        val_ids
    }

    pub async fn get_snapshot_validator_set(&self) -> Vec<U256> {
        let input = ExecutionStakingContract::getSnapshotValidatorSetCall { startIndex: 0 };
        let call = json!({
            "to": STAKING_CONTRACT_ADDRESS,
            "data": input.abi_encode()
        });

        let result: String = self.client.request("eth_call", [call]).await.unwrap();

        let bytes = hex::decode(result).unwrap();
        let get_validator_set_return =
            ExecutionStakingContract::getSnapshotValidatorSetCall::abi_decode_returns(&bytes, true)
                .expect("decoding getSnapshotValidatorSetCall return should pass");
        let is_end = get_validator_set_return.atEnd;
        let _next_start_index = get_validator_set_return.nextStartIndex;
        // TODO: support paginated results
        assert!(is_end);

        get_validator_set_return.validatorIds
    }
}

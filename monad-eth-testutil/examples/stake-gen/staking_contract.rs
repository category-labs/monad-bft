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
use secp256k1::{Keypair as SecpKeyPair, PublicKey as SecpPublicKey, SecretKey as SecpPrivateKey};
use serde_json::json;
use tracing::error;
use url::Url;

use crate::{
    node_data::{Delegator, NodeData, ValidatorId, WithdrawalId},
    send_raw_tx,
};

const BLS_DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";

const STAKING_CONTRACT_ADDRESS: Address =
    Address::new(hex!("0x0000000000000000000000000000000000001000"));
const VAL_ID_SECP_NAMESPACE: FixedBytes<1> = FixedBytes::new(hex!("0x06"));

pub const MON: u64 = 1_000_000_000_000_000_000;
pub const MIN_STAKE_MON: u64 = 100_000;
pub const ACTIVE_VALIDATOR_STAKE_MON: u64 = 25_000_000;

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

    fn generate_signed_txn(
        &self,
        nonce: u64,
        value: U256,
        input: Bytes,
        secp_privkey: SecpPrivateKey,
    ) -> TxEnvelope {
        let tx = TxEip1559 {
            chain_id: self.chain_id,
            nonce: nonce,
            gas_limit: 1_000_000,
            max_fee_per_gas: 100_000_000_000,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(STAKING_CONTRACT_ADDRESS),
            value,
            access_list: Default::default(),
            input,
        };

        let signer =
            LocalSigner::from_bytes(&FixedBytes::<32>::from_slice(&secp_privkey.secret_bytes()))
                .unwrap();

        let signature_hash = tx.signature_hash();
        let signature = signer.sign_hash_sync(&signature_hash).unwrap();

        TxEnvelope::Eip1559(tx.into_signed(signature))
    }

    pub fn generate_add_validator_data(
        node: &NodeData,
        amount: U256,
        commission: Option<U256>,
    ) -> Bytes {
        let secp_keypair = SecpKeyPair::from_secret_key(secp256k1::SECP256K1, &node.secp_privkey);
        let bls_privkey = &node.bls_privkey;

        let auth_address = node.eth_address;
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
        let msg = secp256k1::Message::from_digest(hasher.hash().0);
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

    pub async fn send_add_validator_tx(
        &self,
        node: &mut NodeData,
        amount: U256,
        commission: Option<U256>,
    ) -> Option<TxHash> {
        let add_validator_data =
            StakingContract::generate_add_validator_data(node, amount, commission);

        let signed_tx =
            self.generate_signed_txn(node.nonce, amount, add_validator_data, node.secp_privkey);

        match send_raw_tx(&self.client, signed_tx).await {
            Ok(tx_hash) => {
                node.nonce += 1;
                Some(tx_hash)
            }
            Err(err) => {
                error!("add_validator RPC error: {}", err);
                None
            }
        }
    }

    fn generate_delegate_data(val_id: u64) -> Bytes {
        let args = ExecutionStakingContract::delegateCall {
            validatorId: val_id,
        };

        args.abi_encode().into()
    }

    pub async fn send_delegate_tx(
        &self,
        delegator: &mut dyn Delegator,
        val_id: ValidatorId,
        amount: U256,
    ) -> Option<TxHash> {
        let delegate_data = StakingContract::generate_delegate_data(val_id);

        let signed_tx = self.generate_signed_txn(
            delegator.get_nonce(),
            amount,
            delegate_data,
            delegator.get_secp_priv_key(),
        );
        match send_raw_tx(&self.client, signed_tx).await {
            Ok(tx_hash) => {
                *delegator.get_nonce_mut() += 1;
                Some(tx_hash)
            }
            Err(err) => {
                error!("delegate tx RPC error: {}", err);
                None
            }
        }
    }

    fn generate_undelegate_data(val_id: u64, amount: U256, withdraw_id: u8) -> Bytes {
        let args = ExecutionStakingContract::undelegateCall {
            validatorId: val_id,
            amount,
            withdrawId: withdraw_id,
        };

        args.abi_encode().into()
    }

    pub async fn send_undelegate_tx(
        &self,
        delegator: &mut dyn Delegator,
        val_id: ValidatorId,
        withdrawal_id: u8,
        amount: U256,
    ) -> Option<TxHash> {
        let undelegate_data =
            StakingContract::generate_undelegate_data(val_id, amount, withdrawal_id);

        let signed_tx = self.generate_signed_txn(
            delegator.get_nonce(),
            U256::ZERO,
            undelegate_data,
            delegator.get_secp_priv_key(),
        );

        match send_raw_tx(&self.client, signed_tx).await {
            Ok(tx_hash) => {
                *delegator.get_nonce_mut() += 1;
                Some(tx_hash)
            }
            Err(err) => {
                error!("undelegate txn RPC error: {}", err);
                None
            }
        }
    }

    fn generate_withdraw_data(val_id: u64, withdraw_id: u8) -> Bytes {
        let args = ExecutionStakingContract::withdrawCall {
            validatorId: val_id,
            withdrawId: withdraw_id,
        };

        args.abi_encode().into()
    }

    pub async fn send_withdraw_tx(
        &self,
        delegator: &mut dyn Delegator,
        val_id: ValidatorId,
        withdraw_id: u8,
    ) -> Option<TxHash> {
        let withdraw_data = StakingContract::generate_withdraw_data(val_id, withdraw_id);

        let signed_tx = self.generate_signed_txn(
            delegator.get_nonce(),
            U256::ZERO,
            withdraw_data,
            delegator.get_secp_priv_key(),
        );
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
                *delegator.get_nonce_mut() += 1;
                Some(tx_hash)
            }
            Err(err) => {
                error!("withdraw txn RPC error: {}", err);
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

    pub async fn get_validator(&self, val_id: ValidatorId) -> ExecutionStakingContract::Validator {
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
                .expect("decoding Validator return should pass");

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

    pub async fn get_execution_validator_set(&self) -> Vec<U256> {
        let input = ExecutionStakingContract::getExecutionValidatorSetCall { startIndex: 0 };
        let call = json!({
            "to": STAKING_CONTRACT_ADDRESS,
            "data": input.abi_encode()
        });

        let result: String = self.client.request("eth_call", [call]).await.unwrap();

        let bytes = hex::decode(result).unwrap();
        let get_validator_set_return =
            ExecutionStakingContract::getExecutionValidatorSetCall::abi_decode_returns(
                &bytes, true,
            )
            .expect("decoding getExecutionValidatorSetCall return should pass");
        let is_end = get_validator_set_return.atEnd;
        let _next_start_index = get_validator_set_return.nextStartIndex;
        // TODO: support paginated results
        assert!(is_end);

        get_validator_set_return.validatorIds
    }

    pub async fn get_withdrawal_request(
        &self,
        val_id: ValidatorId,
        delegator_address: Address,
        withdrawal_id: WithdrawalId,
    ) -> ExecutionStakingContract::Withdraw {
        let input = ExecutionStakingContract::getWithdrawalRequestCall {
            validatorId: val_id,
            delegator: delegator_address,
            withdrawId: withdrawal_id,
        };
        let call = json!({
            "to": STAKING_CONTRACT_ADDRESS,
            "data": input.abi_encode()
        });

        let result: String = self.client.request("eth_call", [call]).await.unwrap();

        let bytes = hex::decode(result).unwrap();
        let get_withdrawal_request_return =
            ExecutionStakingContract::Withdraw::abi_decode_params(&bytes, true)
                .expect("decoding Withdraw should pass");

        get_withdrawal_request_return
    }

    pub async fn get_validator_id_from_pubkey(&self, secp_pubkey: &SecpPublicKey) -> u64 {
        let eth_address = Address::from_raw_public_key(&secp_pubkey.serialize_uncompressed()[1..]);

        let slot_key_bytes = [
            VAL_ID_SECP_NAMESPACE.to_vec(),
            eth_address.0.to_vec(),
            vec![0_u8; 11],
        ]
        .concat();
        let slot_key = hex::encode_prefixed(slot_key_bytes);

        let params = [
            STAKING_CONTRACT_ADDRESS.to_string(),
            slot_key,
            "latest".to_owned(),
        ];

        match self
            .client
            .request::<_, String>("eth_getStorageAt", params)
            .await
        {
            Ok(slot_data_hex) => {
                let slot_data_bytes = hex::decode(slot_data_hex).unwrap();
                let val_id = u64::from_be_bytes(slot_data_bytes[0..8].try_into().unwrap());

                val_id
            }
            Err(err) => {
                panic!("eth_getStorageAt failed");
            }
        }
    }
}

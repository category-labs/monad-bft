// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use alloy_consensus::{SignableTransaction, TxEip1559, TxEip7702, TxEnvelope};
use alloy_eips::{eip2718::Encodable2718, eip7702::SignedAuthorization};
use alloy_primitives::{
    aliases::U192,
    hex::{self, FromHex},
    keccak256, Address, Bytes, FixedBytes, TxKind, U256,
};
use alloy_rlp::Encodable;
use alloy_rpc_client::ReqwestClient;
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolValue};
use eyre::Result;
use serde::Deserialize;

use crate::{
    shared::{ensure_contract_deployed, eth_json_rpc::EthJsonRpc, private_key::PrivateKey},
    SimpleAccount,
};

const ENTRYPOINT_BYTECODE: &str = include_str!("erc4337_entrypoint_bytecode.txt");

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(transparent)]
//  Wrapper around the EntryPoint v0.9 contract
pub struct EntryPoint {
    pub addr: Address,
}

impl EntryPoint {
    pub async fn deploy(
        deployer: &(Address, PrivateKey),
        client: &ReqwestClient,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> Result<Self> {
        let nonce = client.get_transaction_count(&deployer.0).await?;
        let input = Bytes::from_hex(ENTRYPOINT_BYTECODE).unwrap();
        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 10_000_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 10,
            to: TxKind::Create,
            value: U256::ZERO,
            access_list: Default::default(),
            input,
        };

        let sig = deployer.1.sign_transaction(&tx);
        let tx = TxEnvelope::Eip1559(tx.into_signed(sig));
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);

        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;

        let addr = calculate_contract_addr(&deployer.0, nonce);
        ensure_contract_deployed(client, addr, tx.tx_hash()).await?;
        Ok(EntryPoint { addr })
    }

    /// Get nonce for a sender account from EntryPoint
    ///
    /// # Arguments
    /// * `sender` - The account address
    /// * `key` - Nonce key (allows parallel UserOps). This is a 192-bit value that creates
    ///           separate nonce "channels". Use 0 for default sequential nonces.
    ///           Different keys allow parallel UserOps from the same sender.
    ///
    /// # Returns
    /// The sequence number for this key (uint64). The full nonce used in UserOp is:
    /// `uint256(key) << 64 | uint64(sequence)`
    pub async fn get_nonce(
        &self,
        client: &ReqwestClient,
        sender: Address,
        key: U192,
    ) -> Result<U256> {
        let call = IEntryPoint::getNonceCall { sender, key };
        let calldata = call.abi_encode();

        let nonce_bytes: Bytes = client
            .request(
                "eth_call",
                serde_json::json!([
                    {
                        "to": self.addr,
                        "data": format!("0x{}", hex::encode(&calldata)),
                    },
                    "latest"
                ]),
            )
            .await?;

        let nonce_return = IEntryPoint::getNonceCall::abi_decode_returns(&nonce_bytes)
            .map_err(|e| eyre::eyre!("Failed to decode getNonce() return: {}", e))?;

        Ok(nonce_return)
    }

    /// Create a PackedUserOperation (ERC-4337 v0.9 format)
    ///
    /// # Arguments
    /// * `sender` - Account address
    /// * `nonce` - Full nonce from EntryPoint.getNonce()
    /// * `init_code`
    /// * `call_data`
    /// * `verification_gas_limit` - Gas for validateUserOp
    /// * `call_gas_limit` - Gas for the actual call
    /// * `pre_verification_gas` - Gas for batch overhead
    /// * `max_priority_fee_per_gas`
    /// * `max_fee_per_gas`
    /// * `paymaster_and_data` - Empty for self-funded, or paymaster data
    ///
    /// # Returns
    /// Unsigned PackedUserOperation
    #[allow(clippy::too_many_arguments)]
    pub fn create_user_operation(
        &self,
        sender: Address,
        nonce: U256,
        init_code: Bytes,
        call_data: Bytes,
        verification_gas_limit: u128,
        call_gas_limit: u128,
        pre_verification_gas: u128,
        max_priority_fee_per_gas: u128,
        max_fee_per_gas: u128,
        paymaster_and_data: Bytes,
    ) -> PackedUserOperation {
        // Pack accountGasLimits: uint128(verificationGasLimit) || uint128(callGasLimit)
        let account_gas_limits = pack_u128_pair(verification_gas_limit, call_gas_limit);

        // Pack gasFees: uint128(maxPriorityFeePerGas) || uint128(maxFeePerGas)
        let gas_fees = pack_u128_pair(max_priority_fee_per_gas, max_fee_per_gas);

        PackedUserOperation {
            sender,
            nonce,
            initCode: init_code,
            callData: call_data,
            accountGasLimits: FixedBytes::from(account_gas_limits),
            preVerificationGas: U256::from(pre_verification_gas),
            gasFees: FixedBytes::from(gas_fees),
            paymasterAndData: paymaster_and_data,
            signature: Bytes::new(), // Empty - must be filled by caller
        }
    }

    /// Sign a UserOperation
    ///
    /// Creates the userOpHash and signs it with the provided key.
    /// The userOpHash is: keccak256(abi.encode(userOp.hash(), entryPoint, chainId))
    pub fn sign_user_operation(
        &self,
        user_op: &PackedUserOperation,
        signer_key: &PrivateKey,
        chain_id: u64,
    ) -> Bytes {
        let user_op_hash = self.get_user_op_hash(user_op, chain_id);
        let user_op_hash_fixed = FixedBytes::<32>::from(user_op_hash);
        let signature = signer_key.sign_hash(&user_op_hash_fixed);

        Bytes::copy_from_slice(&signature.as_bytes())
    }

    /// Get the userOpHash for a PackedUserOperation
    ///
    /// userOpHash = keccak256(abi.encode(userOp.hash(), entryPoint, chainId))
    ///
    /// where userOp.hash() = keccak256(abi.encode(
    ///     sender, nonce, initCode, callData, accountGasLimits,
    ///     preVerificationGas, gasFees, paymasterAndData
    /// ))
    pub fn get_user_op_hash(&self, user_op: &PackedUserOperation, chain_id: u64) -> [u8; 32] {
        // Hash the UserOperation (excluding signature)
        let user_op_packed = (
            user_op.sender,
            user_op.nonce,
            keccak256(&user_op.initCode),
            keccak256(&user_op.callData),
            user_op.accountGasLimits,
            user_op.preVerificationGas,
            user_op.gasFees,
            keccak256(&user_op.paymasterAndData),
        );

        let user_op_hash = keccak256(user_op_packed.abi_encode());

        let final_hash_input = (user_op_hash, self.addr, U256::from(chain_id));
        keccak256(final_hash_input.abi_encode()).into()
    }

    /// Create handleOps transaction
    /// For traditional ERC-4337 account without EIP-7702
    pub fn create_handle_ops_tx(
        &self,
        bundler: &mut SimpleAccount,
        user_ops: Vec<PackedUserOperation>,
        beneficiary: Address,
        max_fee_per_gas: u128,
        chain_id: u64,
        gas_limit: Option<u64>,
        priority_fee: Option<u128>,
    ) -> TxEnvelope {
        let call = IEntryPoint::handleOpsCall {
            ops: user_ops,
            beneficiary,
        };
        let calldata = call.abi_encode();

        let tx = TxEip1559 {
            chain_id,
            nonce: bundler.nonce,
            gas_limit: gas_limit.unwrap_or(10_000_000),
            max_fee_per_gas,
            max_priority_fee_per_gas: priority_fee.unwrap_or(0),
            to: TxKind::Call(self.addr),
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::from(calldata),
        };

        let sig = bundler.key.sign_transaction(&tx);

        bundler.nonce += 1;
        bundler.native_bal = bundler
            .native_bal
            .checked_sub(U256::from(
                gas_limit.unwrap_or(10_000_000) as u128 * max_fee_per_gas,
            ))
            .unwrap_or(U256::ZERO);

        TxEnvelope::Eip1559(tx.into_signed(sig))
    }

    /// Create handleOps transaction wrapped in EIP-7702
    ///
    /// For ERC-4337 + EIP-7702 integration (Simple7702Account)
    ///
    /// # Arguments
    /// * `bundler` - The bundler account
    /// * `user_ops` - UserOperations to execute
    /// * `authorizations` - EIP-7702 authorizations (one per unique sender in user_ops)
    /// * `max_fee_per_gas`
    /// * `chain_id`
    /// * `gas_limit`
    /// * `priority_fee`
    #[allow(clippy::too_many_arguments)]
    pub fn create_handle_ops_tx_with_7702(
        &self,
        bundler: &mut SimpleAccount,
        user_ops: Vec<PackedUserOperation>,
        authorizations: Vec<SignedAuthorization>,
        max_fee_per_gas: u128,
        chain_id: u64,
        gas_limit: Option<u64>,
        priority_fee: Option<u128>,
    ) -> TxEnvelope {
        let call = IEntryPoint::handleOpsCall {
            ops: user_ops,
            beneficiary: bundler.addr,
        };
        let calldata = call.abi_encode();

        let tx = TxEip7702 {
            chain_id,
            nonce: bundler.nonce,
            gas_limit: gas_limit.unwrap_or(10_000_000),
            max_fee_per_gas,
            max_priority_fee_per_gas: priority_fee.unwrap_or(0),
            to: self.addr,
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::from(calldata),
            authorization_list: authorizations,
        };

        let sig = bundler.key.sign_transaction(&tx);

        bundler.nonce += 1;
        bundler.native_bal = bundler
            .native_bal
            .checked_sub(U256::from(
                gas_limit.unwrap_or(10_000_000) as u128 * max_fee_per_gas,
            ))
            .unwrap_or(U256::ZERO);

        TxEnvelope::Eip7702(tx.into_signed(sig))
    }
}

/// Pack two u128 values into a bytes32
///
/// Format: upper128 || lower128
fn pack_u128_pair(upper: u128, lower: u128) -> [u8; 32] {
    let mut result = [0u8; 32];
    result[0..16].copy_from_slice(&upper.to_be_bytes());
    result[16..32].copy_from_slice(&lower.to_be_bytes());
    result
}

pub fn calculate_contract_addr(deployer: &Address, nonce: u64) -> Address {
    let mut out = Vec::new();
    let enc: [&dyn Encodable; 2] = [&deployer, &nonce];
    alloy_rlp::encode_list::<_, dyn Encodable>(&enc, &mut out);
    let hash = keccak256(out);
    let (_, contract_address) = hash.as_slice().split_at(12);
    Address::from_slice(contract_address)
}

sol! {
    #[derive(Debug, PartialEq, Eq)]
    struct PackedUserOperation {
        address sender;
        uint256 nonce;
        bytes initCode;
        bytes callData;
        bytes32 accountGasLimits;
        uint256 preVerificationGas;
        bytes32 gasFees;
        bytes paymasterAndData;
        bytes signature;
    }

    interface IEntryPoint {
        function handleOps(
            PackedUserOperation[] calldata ops,
            address payable beneficiary
        ) external;

        function getNonce(address sender, uint192 key)
            external view returns (uint256 nonce);
    }
}

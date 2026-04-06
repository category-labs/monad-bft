//! TinyVM integration for the in-memory state backend.
//!
//! This module is the **single boundary** between protocol code and backend
//! crates. `in_memory.rs` interacts with TinyVM exclusively through
//! [`TinyVmState`] — no backend types leak outward.
//!
//! State is stored as opaque bytes in a [`BTreeMap`] implementing
//! [`TinyVmStateStore`]. Backends read/write through this interface.
//! Adding a new backend: implement the trait, add to [`registry()`]. Done.

use std::collections::BTreeMap;

use alloy_consensus::{Transaction, TxEnvelope};
use alloy_primitives::{Address, TxKind};
use anyhow::Result;
use serde::{Deserialize, Serialize};

use tiny_privacy_vm_confidential_backend::ConfidentialTinyVmBackend;
pub use tiny_privacy_vm_confidential_backend::{
    encode_register_token_call, encode_shield_public_call, encode_transfer_call,
    encode_transfer_call_with_sidecars, encode_unshield_public_call,
};
use tiny_privacy_vm_runtime::{
    TinyVmBackend, TinyVmDecodedCall, TinyVmRegistry, TinyVmStateStore, TinyVmTxContext,
};

/// Returns the encoded system call input for registering the native MON token.
pub fn encode_register_native_token() -> Vec<u8> {
    use tiny_privacy_vm_confidential_backend::{
        TokenConfig, native_mon_token_id, native_mon_public_asset,
    };
    encode_register_token_call(&TokenConfig {
        token: native_mon_token_id(),
        public_asset: native_mon_public_asset(),
    })
}

// ── Public helpers ──────────────────────────────────────────────────────────

/// Returns true if `tx.to` targets any registered TinyVM backend address.
pub fn is_tinyvm_tx(tx: &TxEnvelope) -> bool {
    match tx.kind() {
        TxKind::Call(to) => registry().is_tinyvm_address(&to.into_array()),
        _ => false,
    }
}

/// Entrypoint address of the first registered backend.
// TODO: with multiple backends, callers must specify which backend by ID.
pub fn tinyvm_entrypoint() -> Address {
    Address::from(
        registry()
            .backends()
            .first()
            .expect("at least one backend registered")
            .entrypoint_address(),
    )
}

pub fn inflight_check(input: &[u8]) -> Result<TinyVmDecodedCall, String> {
    registry()
        .inflight_check_input(input)
        .map_err(|err| err.to_string())
}

// ── TinyVmState ─────────────────────────────────────────────────────────────

/// Protocol-level TinyVM state. All registered backends share this store,
/// each using its own key namespace. No backend types appear in this API.
///
/// All backends share this BTreeMap, but the registry wraps it with a
/// ScopedStore that prefixes keys with backend_id. Backends cannot access
/// each other's state. On the C++ execution side, isolation is enforced
/// by the EVM — each precompile address has its own storage trie.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TinyVmState {
    store: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl TinyVmStateStore for TinyVmState {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.store.get(key).cloned()
    }
    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.store.insert(key.to_vec(), value.to_vec());
    }
}

impl TinyVmState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Verify and apply a TinyVM transaction. The registry dispatches to
    /// the backend matching the `backend_id` in the call payload. The
    /// backend verifies the proof and mutates state through the store.
    pub fn process_tx(
        &mut self,
        sender: Address,
        tx_value: alloy_primitives::U256,
        input: &[u8],
    ) -> Result<()> {
        let tx_value: u128 = tx_value.try_into()
            .map_err(|_| anyhow::anyhow!("tx value exceeds u128"))?;
        let ctx = TinyVmTxContext {
            sender: sender.into_array(),
            tx_value,
        };
        registry().apply_input(&ctx, input, self)?;
        Ok(())
    }
}

// ── Backend registry ────────────────────────────────────────────────────────
// To add a new backend: instantiate it here and append to BACKENDS.

fn registry() -> TinyVmRegistry<'static> {
    static CONFIDENTIAL: ConfidentialTinyVmBackend = ConfidentialTinyVmBackend;
    static BACKENDS: [&dyn TinyVmBackend; 1] = [&CONFIDENTIAL];
    TinyVmRegistry::new(&BACKENDS)
}

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

use std::{
    ffi::CString,
    ffi::c_char,
    ffi::c_void,
    path::PathBuf,
};
use alloy_primitives::{Address, U256};

#[repr(C)]
struct RawMonadRunloopWord {
    bytes: [u8; 32]
}

#[repr(C)]
struct RawMonadRunloopAddress {
    bytes: [u8; 20]
}

// Opaque runloop structure:
type RawMonadRunloop = c_void;

extern "C" {
    // Make a new runloop client
    fn monad_runloop_new(
        chain_id: u64,
        ledger_path: *const c_char,
        db_path: *const c_char,
    ) -> *mut RawMonadRunloop;

    // Deallocate a runloop client
    fn monad_runloop_delete(runloop: *mut RawMonadRunloop);

    // Execute and finalize `nblocks` number of blocks.
    fn monad_runloop_run(runloop: *mut RawMonadRunloop, nblocks: u64);

    // Set balance of the account with given address.
    fn monad_runloop_set_balance(
        runloop: *mut RawMonadRunloop,
        address: *const RawMonadRunloopAddress,
        balance: *const RawMonadRunloopWord,
    );

    // Get balance of the account with given address.
    // Balance is stored in `result_balance`
    fn monad_runloop_get_balance(
        runloop: *mut RawMonadRunloop,
        address: *const RawMonadRunloopAddress,
        result_balance: *mut RawMonadRunloopWord,
    );

    // Store current state root in `result_state_root`.
    fn monad_runloop_get_state_root(
        runloop: *mut RawMonadRunloop,
        result_state_root: *mut RawMonadRunloopWord,
    );
}

pub struct MonadRunloop {
    raw: *mut RawMonadRunloop
}

impl Drop for MonadRunloop {
    fn drop(&mut self) {
        unsafe {
            monad_runloop_delete(self.raw)
        }
    }
}

impl MonadRunloop {
    pub fn new(
        chain_id: u64,
        ledger_path: PathBuf,
        db_path: PathBuf,
    ) -> MonadRunloop {
        let ledger_path = ledger_path.into_os_string().into_string().unwrap();
        let ledger_path = CString::new(ledger_path).unwrap();
        let db_path = db_path.into_os_string().into_string().unwrap();
        let db_path = CString::new(db_path).unwrap();
        MonadRunloop {
            raw: unsafe {
                monad_runloop_new(
                    chain_id,
                    ledger_path.as_ptr(),
                    db_path.as_ptr())
            }
        }
    }

    pub fn run(&mut self, nblocks: u64) {
        unsafe {
            monad_runloop_run(self.raw, nblocks)
        }
    }

    pub fn set_balance(&mut self, address: Address, balance: U256) {
        let address = RawMonadRunloopAddress{
            bytes: address.0.0,
        };
        let balance = RawMonadRunloopWord{
            bytes: balance.to_be_bytes::<32>(),
        };
        unsafe {
            monad_runloop_set_balance(self.raw, &address, &balance)
        }
    }

    pub fn get_balance(&mut self, address: Address) -> U256 {
        let address = RawMonadRunloopAddress{
            bytes: address.0.0,
        };
        let mut balance = RawMonadRunloopWord{
            bytes: [0; 32],
        };
        unsafe {
            monad_runloop_get_balance(self.raw, &address, &mut balance)
        };
        U256::from_be_bytes(balance.bytes)
    }

    pub fn get_state_root(&mut self) -> U256 {
        let mut state_root = RawMonadRunloopWord{
            bytes: [0; 32],
        };
        unsafe {
            monad_runloop_get_state_root(self.raw, &mut state_root)
        };
        U256::from_be_bytes(state_root.bytes)
    }
}

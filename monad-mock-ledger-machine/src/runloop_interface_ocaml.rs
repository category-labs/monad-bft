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
    ffi::{CString, c_char, c_void},
    path::PathBuf, sync::Once,
};
use alloy_primitives::{Address, U256};
use crate::runloop::Runloop;

#[repr(C)]
struct RawCamlRunloopWord {
    bytes: [u8; 32]
}

#[repr(C)]
struct RawCamlRunloopAddress {
    bytes: [u8; 20]
}

// Opaque runloop structure:
type RawCamlRunloop = c_void;

extern "C" {
    // Make a new runloop client
    fn runloop_fuzz_client_new(
        chain_id: u64,
        ledger_path: *const c_char,
        db_path: *const c_char,
    ) -> *mut RawCamlRunloop;

    // Deallocate a runloop client
    fn runloop_fuzz_client_delete(runloop: *mut RawCamlRunloop);

    // Execute and finalize `nblocks` number of blocks.
    fn runloop_fuzz_client_run(runloop: *mut RawCamlRunloop, nblocks: u64);

    // Set balance of the account with given address.
    fn runloop_fuzz_client_set_balance(
        runloop: *mut RawCamlRunloop,
        address: *const RawCamlRunloopAddress,
        balance: *const RawCamlRunloopWord,
    );

    // Get balance of the account with given address.
    // Balance is stored in `result_balance`
    fn runloop_fuzz_client_get_balance(
        runloop: *mut RawCamlRunloop,
        address: *const RawCamlRunloopAddress,
        result_balance: *mut RawCamlRunloopWord,
    );

    // Store current state root in `result_state_root`.
    fn runloop_fuzz_client_get_state_root(
        runloop: *mut RawCamlRunloop,
        result_state_root: *mut RawCamlRunloopWord,
    );

    // Dump db state as json to stdout.
    fn runloop_fuzz_client_dump(runloop: *mut RawCamlRunloop);

    // Initialize OCaml runtime
    fn caml_main(arg_array : *const *const c_char);
}

fn initialize_ocaml_runtime() {
    let args: Vec<String> = std::env::args().collect();

    // Convert to CString (null-terminated)
    let c_strings: Vec<CString> = args
        .into_iter()
        .map(|arg| CString::new(arg).unwrap())
        .collect();

    // Create array of char* pointers + null terminator
    let mut argv: Vec<*const i8> = c_strings
        .iter()
        .map(|s| s.as_ptr())
        .collect();
    argv.push(std::ptr::null()); // Null-terminate the array

    let argv_ptr = argv.as_ptr();
    unsafe { caml_main(argv_ptr) };
}

static CAML_RUNTIME_ONCE: Once = Once::new();

pub struct CamlRunloop {
    raw: *mut RawCamlRunloop
}

impl Drop for CamlRunloop {
    fn drop(&mut self) {
        unsafe {
            runloop_fuzz_client_delete(self.raw)
        }
    }
}

impl CamlRunloop {
    pub fn new(
        chain_id: u64,
        ledger_path: PathBuf,
        db_path: PathBuf,
    ) -> CamlRunloop {
        CAML_RUNTIME_ONCE.call_once(|| {
            initialize_ocaml_runtime();
        });
        let ledger_path = ledger_path.into_os_string().into_string().unwrap();
        let ledger_path = CString::new(ledger_path).unwrap();
        let db_path = db_path.into_os_string().into_string().unwrap();
        let db_path = CString::new(db_path).unwrap();
        CamlRunloop {
            raw: unsafe {
                runloop_fuzz_client_new(
                    chain_id,
                    ledger_path.as_ptr(),
                    db_path.as_ptr())
            }
        }
    }
}

impl Runloop for CamlRunloop {
    fn run(&mut self, nblocks: u64) {
        unsafe {
            runloop_fuzz_client_run(self.raw, nblocks)
        }
    }

    fn set_balance(&mut self, address: Address, balance: U256) {
        let address = RawCamlRunloopAddress{
            bytes: address.0.0,
        };
        let balance = RawCamlRunloopWord{
            bytes: balance.to_be_bytes::<32>(),
        };
        unsafe {
            runloop_fuzz_client_set_balance(self.raw, &address, &balance)
        }
    }

    fn get_balance(&mut self, address: Address) -> U256 {
        let address = RawCamlRunloopAddress{
            bytes: address.0.0,
        };
        let mut balance = RawCamlRunloopWord{
            bytes: [0; 32],
        };
        unsafe {
            runloop_fuzz_client_get_balance(self.raw, &address, &mut balance)
        };
        U256::from_be_bytes(balance.bytes)
    }

    fn get_state_root(&mut self) -> U256 {
        let mut state_root = RawCamlRunloopWord{
            bytes: [0; 32],
        };
        unsafe {
            runloop_fuzz_client_get_state_root(self.raw, &mut state_root)
        };
        U256::from_be_bytes(state_root.bytes)
    }

    fn dump(&mut self) {
        unsafe {
            runloop_fuzz_client_dump(self.raw)
        }
    }
}

I want to implement support for *namespaces*. The rest of this document describes the specification of these.

# Namespace
We'll first define what a namespace is. A namespace is a separate execution environment that's isolated from the rest of the existing (global) state. Multiple namespaces can exist simultaneously in a single Monad L1. One of the motivations is that execution of *namespaced transactions* can be done fully in parallel with *global transactions*

Namespaces can be trustlessly created by arbitrary EOAs with enough funds through a special Monad-specific precompile. The namespace creation precompile will look similarly to CREATE/CREATE2 but in precompile form. Because of this, each distinct namespace will have its own unique address. There also will be a precompile used for bridging funds into/out of namespaces.

The state trie for any given namespace exists as a subtrie rooted under the account in the regular (global) state trie. Concretely, `monad-triedb-utils/src/key.rs` would need to allow for the specification of a `namespace: [u8; 20]` for any KeyInput that touches state - at least Address and Storage, but maybe more. The final state trie can be updated at the end after all global + namespace execution results are ready.

The namespace creation and bridging precompiles have a 1-block delay before they take affect in execution. For instance, a namespace created in block N is only active in block N+1. Similarly, bridging funds into a namespace in block N from a global EOA will immediately debit the global EOA in block N, but the credit will only apply to the namespace in block N+1. This is to make the above desired parallel execution property easier to implement, as we can know that the state will be disjoint between namespaces and global state within a single block.


# Namespaced Transaction
A *namespaced transaction* is a wrapped version of a standard Ethereum transaction. It looks something like this:

0x80 || <namespace> || 2718 envelope

The idea is that 2718 transactions can't be prefixed by 0x80, so that can serve as the discriminant for a namespaced transaction. The tx signature would be over the entire payload including the namespace and prefix, so that the tx can't be replayed in other namespaces or in the global state.



# Implementation Plan
I'd like to start by only implementing the BFT side of this (eg, don't touch anything in monad-execution). We also don't know the exact format of the namespace creation and bridging precompiles yet. Obviously, this limits the extent to which we can test things. For now, we can test the following:
- Namespaced transactions are accepted into the txpool
- Namespaced transactions are validated and included in blocks
- Balance checks for namespaced transactions are done correctly against namespaced balances
- Add namespace support to the mock StateBackends so that the `monad-mock-swarm` EthSwarm tests can test inclusion and validation of namespaced transactions
- Create a mock-swarm test that tests both standard and namespaced transactions. Because we don't support creation and bridging yet, seed both global and namespaced balances in the test so that namespaced EOAs have funds.

Feel free to update the real StateBackend (create_triedb_key) stuff as well, but be aware that these won't be testable until we implement the execution side.
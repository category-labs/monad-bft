#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

enum monad_chain_config
{
    CHAIN_CONFIG_ETHEREUM_MAINNET = 0,
    CHAIN_CONFIG_MONAD_DEVNET = 1,
    CHAIN_CONFIG_MONAD_TESTNET = 2,
};

struct monad_evmc_result
{
    int status_code;
    std::vector<uint8_t> output_data;
    std::string message;
    int64_t gas_used;
    int64_t gas_refund;

    int get_status_code() const;
    std::vector<uint8_t> get_output_data() const;
    std::string get_message() const;
    int64_t get_gas_used() const;
    int64_t get_gas_refund() const;
};

struct monad_state_override_set
{
    using bytes = std::vector<uint8_t>;

    struct monad_state_override_object
    {
    };

    std::map<bytes, monad_state_override_object> override_sets;

    monad_state_override_set() {}

    void add_override_address(bytes const &address);

    void set_override_balance(bytes const &address, bytes const &balance);

    void set_override_nonce(bytes const &address, uint64_t const &nonce);

    void set_override_code(bytes const &address, bytes const &code);

    void set_override_state_diff(
        bytes const &address, bytes const &key, bytes const &value);

    void set_override_state(
        bytes const &address, bytes const &key, bytes const &value);
};

monad_evmc_result eth_call(
    monad_chain_config const chain_config, std::vector<uint8_t> const &rlp_txn,
    std::vector<uint8_t> const &rlp_header,
    std::vector<uint8_t> const &rlp_sender,
    std::string const &triedb_path,
    monad_state_override_set const &state_overrides);

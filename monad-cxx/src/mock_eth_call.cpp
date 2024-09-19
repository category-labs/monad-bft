#include "eth_call.h"

#include <array>

// monad_evmc_result functions

int get_status_code(const struct monad_evmc_result *result)
{
    return result->status_code;
}

bytes get_output_data(const struct monad_evmc_result *result)
{
    return result->output_data;
}

uint64_t get_output_size(const struct monad_evmc_result *result)
{
    return result->output_size;
}

char const *get_message(const struct monad_evmc_result *result)
{
    return result->message;
}

int64_t get_gas_used(const struct monad_evmc_result *result)
{
    return result->gas_used;
}

int64_t get_gas_refund(const struct monad_evmc_result *result)
{
    return result->gas_refund;
}

// monad_state_override functions
struct monad_state_override_set
{
};

struct monad_state_override_set *create_empty_state_override_set()
{
    return nullptr;
}

void add_override_address(monad_state_override_set *, bytes address) {}

void set_override_balance(
    monad_state_override_set *, bytes address, bytes balance,
    uint64_t balance_len)
{
}

void set_override_nonce(
    monad_state_override_set *, bytes address, uint64_t nonce)
{
}

void set_override_code(
    monad_state_override_set *, bytes address, bytes code, uint64_t code_len)
{
}

void set_override_state_diff(
    monad_state_override_set *, bytes address, bytes key, bytes value)
{
}

void set_override_state(
    monad_state_override_set *, bytes address, bytes key, bytes value)
{
}

monad_evmc_result eth_call(
    bytes rlp_txn, uint64_t txn_len, bytes rlp_header, uint64_t header_len,
    bytes rlp_sender, uint64_t const block_number, char const *triedb_path,
    char const *blockdb_path, monad_state_override_set *state_overrides)
{
    static constexpr auto N = 32;
    std::array<uint8_t, N> data = {
        0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe,
        0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad,
        0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef};
    return monad_evmc_result{
        .status_code = 0,
        .output_data = data.data(),
        .message = (char *)("test message"),
        .gas_used = 21000,
        .gas_refund = 0};
}

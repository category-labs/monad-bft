#include "eth_call.h"

#include <array>

// monad_evmc_result functions
int monad_evmc_result::get_status_code() const
{
    return status_code;
}

bytes monad_evmc_result::get_output_data() const
{
    return output_data;
}

char const *monad_evmc_result::get_message() const
{
    return message;
}

int64_t monad_evmc_result::get_gas_used() const
{
    return gas_used;
}

int64_t monad_evmc_result::get_gas_refund() const
{
    return gas_refund;
}

// monad_state_override functions
struct monad_state_override_set
{
};

void add_override_address(monad_state_override_set *, bytes const &) {}

void set_override_balance(
    monad_state_override_set *, bytes const &, bytes const &)
{
}

void set_override_nonce(
    monad_state_override_set *, bytes address, uint64_t nonce)
{
}

void set_override_code(monad_state_override_set *, bytes address, bytes code) {}

void set_override_state_diff(
    monad_state_override_set *, bytes address, bytes key, bytes value)
{
}

void set_override_state(
    monad_state_override_set *, bytes address, bytes key, bytes value)
{
}

monad_evmc_result eth_call(
    bytes rlp_txn, bytes rlp_header, bytes rlp_sender,
    uint64_t const block_number, char const *triedb_path,
    char const *blockdb_path, monad_state_override_set const &state_overrides)
{
    static constexpr auto N = 32;
    std::array<uint8_t, N> data = {
        0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe,
        0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad,
        0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef};
    return monad_evmc_result{
        .status_code = 0,
        .output_data = data.data(),
        .message = "test message",
        .gas_used = 21000,
        .gas_refund = 0};
}

#include "eth_call.hpp"

#include <array>
#include <cstring>
#include <stdlib.h>

int monad_evmc_result::get_status_code() const
{
    return status_code;
}

std::vector<uint8_t> monad_evmc_result::get_output_data() const
{
    return output_data;
}

std::string monad_evmc_result::get_message() const
{
    return message;
}

int64_t monad_evmc_result::get_gas_used() const
{
    return gas_used;
}

monad_evmc_result eth_call(
    std::vector<uint8_t> const &rlp_encoded_transaction,
    std::vector<uint8_t> const &rlp_encoded_block_header,
    std::vector<uint8_t> const &rlp_encoded_sender, uint64_t const block_number,
    std::string const &triedb_path, std::string const &block_db_path)
{
    static constexpr auto N = 32;
    std::array<uint8_t, N> data = {
        0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe,
        0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad,
        0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef};
    return monad_evmc_result{
        .status_code = 0,
        .output_data = std::vector<uint8_t>{data.begin(), data.end()},
        .message = "test message",
        .gas_used = 21000};
}

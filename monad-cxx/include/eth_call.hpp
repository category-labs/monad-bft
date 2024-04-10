#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

struct monad_evmc_result
{
    int status_code;
    std::vector<uint8_t> output_data;

    int get_status_code() const;
    std::vector<uint8_t> get_output_data() const;
};

monad_evmc_result eth_call(
    std::vector<uint8_t> const &rlp_encoded_transaction,
    std::vector<uint8_t> const &rlp_encoded_block_header,
    std::vector<uint8_t> const &rlp_encoded_sender, uint64_t const block_number,
    std::string const &triedb_path, std::string const &block_db_path);

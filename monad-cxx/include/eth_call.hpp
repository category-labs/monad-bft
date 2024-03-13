#pragma once

#include "evmc.h"
#include <string>
#include <vector>

namespace monad
{
    using block_num_t = uint64_t;

    namespace rpc
    {
        evmc_result eth_call(
            std::vector<uint8_t> const &rlp_encoded_transaction,
            std::vector<uint8_t> const &rlp_encoded_block_header,
            std::vector<uint8_t> const &rlp_encoded_sender,
            block_num_t const block_number, std::string const &triedb_path,
            std::string const &block_db_path);
    } // namespace rpc
} // namespace monad

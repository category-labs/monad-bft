#include "eth_call.hpp"

#include <monad/execution/trace.hpp>
#include <monad/rpc/eth_call.hpp>

namespace monad
{
    quill::Logger *tracer = nullptr;
}

int monad_evmc_result::get_status_code() const
{
    return status_code;
}

std::vector<uint8_t> monad_evmc_result::get_output_data() const
{
    return output_data;
}

monad_evmc_result eth_call(
    std::vector<uint8_t> const &rlp_encoded_transaction,
    std::vector<uint8_t> const &rlp_encoded_block_header,
    std::vector<uint8_t> const &rlp_encoded_sender,
    block_num_t const block_number, std::string const &triedb_path,
    std::string const &block_db_path)
{
    // TODO: this no longer has to return the raw evmc_result type and can
    // instead use the evmc::Result type
    auto evmc_result = ::monad::rpc::eth_call(
        rlp_encoded_transaction,
        rlp_encoded_block_header,
        rlp_encoded_sender,
        block_number,
        triedb_path,
        block_db_path);

    auto const status_code = static_cast<int>(evmc_result.status_code);
    auto output_data = [&] {
        if (evmc_result.output_data) {
            std::vector<uint8_t> res{
                evmc_result.output_data,
                evmc_result.output_data + evmc_result.output_size};
            MONAD_ASSERT(evmc_result.release);
            evmc_result.release(&evmc_result);
            return res;
        }
        else {
            return std::vector<uint8_t>{};
        }
    }();

    return monad_evmc_result{
        .status_code = status_code, .output_data = std::move(output_data)};
}

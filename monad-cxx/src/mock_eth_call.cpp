#include "eth_call.hpp"

#include <array>
#include <cstring>
#include <stdlib.h>

namespace monad
{
    namespace rpc
    {
        // this should get called on the Rust side in `impl Drop for EvmcResult`
        static void evmc_free_result_memory(const struct evmc_result *result)
        {
            printf("evmc_free_result_memory");
            free((uint8_t *)result->output_data);
        }

        evmc_result eth_call(
            std::vector<uint8_t> const &rlp_encoded_transaction,
            std::vector<uint8_t> const &rlp_encoded_block_header,
            std::vector<uint8_t> const &rlp_encoded_sender,
            block_num_t const block_number, std::string const &triedb_path,
            std::string const &block_db_path)
        {
            static constexpr auto N = 32;
            std::array<uint8_t, N> data = {
                0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef,
                0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef,
                0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef,
                0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef};
            uint8_t *out{static_cast<uint8_t *>(std::malloc((N)))};

            std::memcpy((void *)out, (void *)&data, N);

            return evmc_result{
                .status_code = EVMC_SUCCESS,
                .output_data = out,
                .output_size = N,
                .release = evmc_free_result_memory};
        }
    }
}

#include "eth_call.hpp"

#include <monad/core/block.hpp>
#include <monad/core/rlp/address_rlp.hpp>
#include <monad/core/rlp/block_rlp.hpp>
#include <monad/core/rlp/transaction_rlp.hpp>
#include <monad/core/transaction.hpp>
#include <monad/db/trie_db.hpp>
#include <monad/execution/block_hash_buffer.hpp>
#include <monad/execution/evmc_host.hpp>
#include <monad/execution/execute_transaction.hpp>
#include <monad/execution/tx_context.hpp>
#include <monad/execution/validate_transaction.hpp>
#include <monad/state2/block_state.hpp>
#include <monad/state3/state.hpp>

#include <boost/outcome/try.hpp>

#include <quill/Quill.h>

#include <filesystem>
#include <vector>

using namespace monad;

namespace
{
    Result<evmc::Result> eth_call_helper(
        Transaction const &txn, BlockHeader const &header,
        uint64_t const block_number, Address const &sender,
        BlockHashBuffer const &buffer,
        std::vector<std::filesystem::path> const &dbname_paths)
    {
        // TODO: Hardset rev to be Shanghai at the moment
        static constexpr auto rev = EVMC_SHANGHAI;
        Transaction enriched_txn{txn};

        // SignatureAndChain validation hacks
        enriched_txn.sc.chain_id = 1;
        enriched_txn.sc.r = 1;
        enriched_txn.sc.s = 1;

        BOOST_OUTCOME_TRY(static_validate_transaction<rev>(
            enriched_txn, header.base_fee_per_gas))

        TrieDb ro{mpt::ReadOnlyOnDiskDbConfig{.dbname_paths = dbname_paths}};
        ro.set_block_number(block_number);
        ro.load_latest();
        BlockState block_state{ro};
        State state{block_state};

        // nonce validation hack
        auto const acct = ro.read_account(sender);
        enriched_txn.nonce = acct.has_value() ? acct.value().nonce : 0;

        BOOST_OUTCOME_TRY(validate_transaction(enriched_txn, acct));
        auto const tx_context =
            get_tx_context<rev>(enriched_txn, sender, header);
        EvmcHost<rev> host{tx_context, buffer, state};
        return execute_impl_no_validation<rev>(
            state,
            host,
            enriched_txn,
            sender,
            header.base_fee_per_gas.value_or(0),
            header.beneficiary);
    }
}

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
    std::vector<uint8_t> const &rlp_txn, std::vector<uint8_t> const &rlp_header,
    std::vector<uint8_t> const &rlp_sender, uint64_t const block_number,
    std::string const &triedb_path, std::string const &blockdb_path)
{
    byte_string_view rlp_txn_view(rlp_txn.begin(), rlp_txn.end());
    auto const txn_result = rlp::decode_transaction(rlp_txn_view);
    MONAD_ASSERT(!txn_result.has_error());
    MONAD_ASSERT(rlp_txn_view.empty());
    auto const txn = txn_result.value();

    byte_string_view rlp_header_view(rlp_header.begin(), rlp_header.end());
    auto const block_header_result = rlp::decode_block_header(rlp_header_view);
    MONAD_ASSERT(rlp_header_view.empty());
    MONAD_ASSERT(!block_header_result.has_error());
    auto const block_header = block_header_result.value();

    byte_string_view rlp_sender_view(rlp_sender.begin(), rlp_sender.end());
    auto const sender_result = rlp::decode_address(rlp_sender_view);
    MONAD_ASSERT(rlp_sender_view.empty());
    MONAD_ASSERT(!sender_result.has_error());
    auto const sender = sender_result.value();

    BlockHashBuffer buffer{};
    for (size_t i = block_number < 256 ? 1 : block_number - 255;
         i < block_number;
         ++i) {
        auto const path =
            std::filesystem::path{blockdb_path} / std::to_string(i);
        MONAD_ASSERT(std::filesystem::exists(path));
        std::ifstream istream(path);
        std::ostringstream buf;
        buf << istream.rdbuf();
        auto view = byte_string_view{
            (unsigned char *)buf.view().data(), buf.view().size()};
        auto const block_result = rlp::decode_block(view);
        MONAD_ASSERT(block_result.has_value());
        MONAD_ASSERT(view.empty());
        auto const &block = block_result.assume_value();
        buffer.set(i - 1, block.header.parent_hash);
    }

    MONAD_ASSERT(std::filesystem::is_directory(triedb_path));
    std::vector<std::filesystem::path> paths;
    for (auto const &file : std::filesystem::directory_iterator(triedb_path)) {
        paths.emplace_back(file.path());
    }
    auto const result =
        eth_call_helper(txn, block_header, block_number, sender, buffer, paths);
    monad_evmc_result ret;
    if (MONAD_UNLIKELY(result.has_error())) {
        LOG_ERROR("failed with {}", result.error().message().c_str());
        ret.status_code = INT_MAX;
    }
    else {
        ret.status_code = result.assume_value().status_code;
        ret.output_data = {
            result.assume_value().output_data,
            result.assume_value().output_data +
                result.assume_value().output_size};
    }
    return ret;
}

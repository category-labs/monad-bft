#include "eth_call.hpp"

#include <monad/core/block.hpp>
#include <monad/core/rlp/address_rlp.hpp>
#include <monad/core/rlp/block_rlp.hpp>
#include <monad/core/rlp/transaction_rlp.hpp>
#include <monad/core/transaction.hpp>
#include <monad/db/block_db.hpp>
#include <monad/db/trie_db.hpp>
#include <monad/execution/block_hash_buffer.hpp>
#include <monad/execution/evmc_host.hpp>
#include <monad/execution/execute_transaction.hpp>
#include <monad/execution/explicit_evmc_revision.hpp>
#include <monad/execution/trace.hpp>
#include <monad/execution/transaction_gas.hpp>
#include <monad/execution/tx_context.hpp>
#include <monad/execution/validate_transaction.hpp>
#include <monad/state2/block_state.hpp>
#include <monad/state3/state.hpp>

#include <evmc/evmc.h>

#include <boost/outcome/config.hpp>
#include <boost/outcome/try.hpp>

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

using namespace monad;

Result<evmc::Result> eth_call_helper(
    Transaction const &txn, BlockHeader const &header, uint64_t const block_id,
    Address const &sender, BlockHashBuffer const &buffer,
    std::vector<std::filesystem::path> const &dbname_paths)
{
    // TODO: Hardset rev to be Shanghai at the moment
    static constexpr auto rev = EVMC_SHANGHAI;
    Transaction enriched_txn{txn};

    // SignatureAndChain validation hacks
    enriched_txn.sc.chain_id = 1;
    enriched_txn.sc.r = 1;
    enriched_txn.sc.s = 1;

    BOOST_OUTCOME_TRY(
        static_validate_transaction<rev>(enriched_txn, header.base_fee_per_gas))

    mpt::ReadOnlyOnDiskDbConfig const ro_config{.dbname_paths = dbname_paths};
    TrieDbRO trie_db_ro{ro_config, block_id};
    BlockState block_state{trie_db_ro};
    State state{block_state};

    auto const sender_account = state.recent_account(sender);

    // nonce validation hack
    enriched_txn.nonce =
        sender_account.has_value() ? sender_account.value().nonce : 0;

    BOOST_OUTCOME_TRY(validate_transaction(enriched_txn, sender_account));

    auto const tx_context = get_tx_context<rev>(enriched_txn, sender, header);
    EvmcHost<rev> host{tx_context, buffer, state};

    return execute_impl_no_validation<rev>(
        state,
        host,
        enriched_txn,
        sender,
        header.base_fee_per_gas.value_or(0),
        header.beneficiary);
}

monad_evmc_result eth_call(
    std::vector<uint8_t> const &rlp_encoded_transaction,
    std::vector<uint8_t> const &rlp_encoded_block_header,
    std::vector<uint8_t> const &rlp_encoded_sender, uint64_t const block_number,
    std::string const &triedb_path, std::string const &block_db_path)
{
    byte_string_view encoded_transaction(
        rlp_encoded_transaction.begin(), rlp_encoded_transaction.end());
    auto const txn_result = rlp::decode_transaction(encoded_transaction);
    MONAD_ASSERT(!txn_result.has_error());
    MONAD_ASSERT(encoded_transaction.empty());
    auto const txn = txn_result.value();

    byte_string_view encoded_block_header(
        rlp_encoded_block_header.begin(), rlp_encoded_block_header.end());
    auto const block_header_result =
        rlp::decode_block_header(encoded_block_header);
    MONAD_ASSERT(!block_header_result.has_error());
    MONAD_ASSERT(encoded_block_header.empty());
    auto const block_header = block_header_result.value();

    byte_string_view encoded_sender(
        rlp_encoded_sender.begin(), rlp_encoded_sender.end());
    auto const sender_result = rlp::decode_address(encoded_sender);
    MONAD_ASSERT(!sender_result.has_error());
    MONAD_ASSERT(encoded_sender.empty());
    auto const sender = sender_result.value();

    BlockHashBuffer buffer{};
    uint64_t start_block_number = block_number < 256 ? 1 : block_number - 255;
    BlockDb block_db{block_db_path.c_str()};

    while (start_block_number < block_number) {
        Block block{};
        bool const result = block_db.get(start_block_number, block);
        MONAD_ASSERT(result);
        buffer.set(start_block_number - 1, block.header.parent_hash);
        ++start_block_number;
    }

    // TODO: construct triedb_path properly
    auto evmc_result = eth_call_helper(
        txn, block_header, block_number, sender, buffer, {triedb_path.c_str()});

    auto const status_code = evmc_result.has_error()
                                 ? EVMC_FAILURE
                                 : evmc_result.value().status_code;
    auto output_data = [&] {
        if (!evmc_result.has_error()) {
            std::vector<uint8_t> res{
                evmc_result.value().output_data,
                evmc_result.value().output_data +
                    evmc_result.value().output_size};
            return res;
        }
        else {
            return std::vector<uint8_t>{};
        }
    }();

    return monad_evmc_result{
        .status_code = status_code, .output_data = std::move(output_data)};
}

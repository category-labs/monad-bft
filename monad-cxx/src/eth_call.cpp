#include "eth_call.h"

#include <monad/chain/monad_devnet.hpp>
#include <monad/core/assert.h>
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
#include <monad/types/incarnation.hpp>

#include <boost/outcome/try.hpp>

#include <quill/Quill.h>

#include <filesystem>
#include <map>
#include <vector>

using namespace monad;

namespace monad
{
    quill::Logger *tracer = nullptr;
}

// monad_evmc_result functions

int64_t get_status_code(monad_evmc_result const *result)
{
    return result->status_code;
}

bytes get_output_data(monad_evmc_result const *result)
{
    return result->output_data;
}

uint64_t get_output_size(monad_evmc_result const *result)
{
    return result->output_size;
}

char const *get_message(monad_evmc_result const *result)
{
    return result->message;
}

int64_t get_gas_used(monad_evmc_result const *result)
{
    return result->gas_used;
}

int64_t get_gas_refund(monad_evmc_result const *result)
{
    return result->gas_refund;
}

// define real moand_state_override_set struct
using cxx_bytes = std::vector<uint8_t>;

struct monad_state_override_set
{
    struct monad_state_override_object
    {
        std::optional<cxx_bytes> balance{std::nullopt};
        std::optional<uint64_t> nonce{std::nullopt};
        std::optional<cxx_bytes> code{std::nullopt};
        std::map<cxx_bytes, cxx_bytes> state{};
        std::map<cxx_bytes, cxx_bytes> state_diff{};
    };

    std::map<cxx_bytes, monad_state_override_object> override_sets;
};

struct monad_state_override_set *create_empty_state_override_set()
{
    return new monad_state_override_set{};
}

void add_override_address(
    monad_state_override_set *state_overrides, bytes address)
{
    cxx_bytes const cxx_address(address, address + sizeof(Address));

    auto &override_sets = state_overrides->override_sets;
    MONAD_ASSERT(override_sets.find(cxx_address) == override_sets.end());
    override_sets.emplace(
        cxx_address, monad_state_override_set::monad_state_override_object{});
}

void set_override_balance(
    monad_state_override_set *state_overrides, bytes address, bytes balance,
    uint64_t balance_len)
{
    cxx_bytes const cxx_address(address, address + sizeof(Address));
    cxx_bytes const cxx_balance(balance, balance + balance_len);

    auto &override_sets = state_overrides->override_sets;
    MONAD_ASSERT(override_sets.find(cxx_address) != override_sets.end());
    override_sets[cxx_address].balance = cxx_balance;
}

void set_override_nonce(
    monad_state_override_set *state_overrides, bytes address, uint64_t nonce)
{
    cxx_bytes const cxx_address(address, address + sizeof(Address));

    auto &override_sets = state_overrides->override_sets;
    MONAD_ASSERT(override_sets.find(cxx_address) != override_sets.end());
    override_sets[cxx_address].nonce = nonce;
}

void set_override_code(
    monad_state_override_set *state_overrides, bytes address, bytes code,
    uint64_t code_len)
{
    cxx_bytes const cxx_address(address, address + sizeof(Address));
    cxx_bytes const cxx_code(code, code + code_len);

    auto &override_sets = state_overrides->override_sets;
    MONAD_ASSERT(override_sets.find(cxx_address) != override_sets.end());
    override_sets[cxx_address].code = cxx_code;
}

void set_override_state_diff(
    [[maybe_unused]] monad_state_override_set *state_overrides,
    [[maybe_unused]] bytes address, [[maybe_unused]] bytes key,
    [[maybe_unused]] bytes value)
{
    // TODO
    return;
}

void set_override_state(
    [[maybe_unused]] monad_state_override_set *state_overrides,
    [[maybe_unused]] bytes address, [[maybe_unused]] bytes key,
    [[maybe_unused]] bytes value)
{
    // TODO
    return;
}

// eth_call implementation
namespace
{
    Result<evmc::Result> eth_call_impl(
        Transaction const &txn, BlockHeader const &header,
        uint64_t const block_number, Address const &sender,
        BlockHashBuffer const &buffer,
        std::vector<std::filesystem::path> const &dbname_paths,
        monad_state_override_set *state_overrides)
    {
        constexpr evmc_revision rev = EVMC_SHANGHAI; // TODO
        MonadDevnet chain;
        MONAD_ASSERT(rev == chain.get_revision(header));

        Transaction enriched_txn{txn};

        // SignatureAndChain validation hacks
        enriched_txn.sc.chain_id = chain.get_chain_id();
        enriched_txn.sc.r = 1;
        enriched_txn.sc.s = 1;

        BOOST_OUTCOME_TRY(static_validate_transaction<rev>(
            enriched_txn, header.base_fee_per_gas, chain.get_chain_id()));

        // rodb is not thread safe
        thread_local mpt::Db db{
            mpt::ReadOnlyOnDiskDbConfig{.dbname_paths = dbname_paths}};
        thread_local TrieDb ro{db};

        ro.set_block_number(block_number);
        BlockState block_state{ro};
        // avoid conflict with block reward txn
        Incarnation incarnation{block_number, Incarnation::LAST_TX - 1u};
        State state{block_state, incarnation};

        for (auto const &[addr, state_delta] : state_overrides->override_sets) {
            // address
            Address address{};
            std::memcpy(address.bytes, addr.data(), sizeof(Address));

            // This would avoid seg-fault on storage override for non-existing
            // accounts
            auto const &account = state.recent_account(address);
            if (MONAD_UNLIKELY(!account.has_value())) {
                state.create_contract(address);
            }

            if (state_delta.balance.has_value()) {
                auto const balance = intx::be::unsafe::load<uint256_t>(
                    state_delta.balance.value().data());
                if (balance >
                    intx::be::load<uint256_t>(state.get_balance(address))) {
                    state.add_to_balance(
                        address,
                        balance - intx::be::load<uint256_t>(
                                      state.get_balance(address)));
                }
                else {
                    state.subtract_from_balance(
                        address,
                        intx::be::load<uint256_t>(state.get_balance(address)) -
                            balance);
                }
            }

            if (state_delta.nonce.has_value()) {
                state.set_nonce(address, state_delta.nonce.value());
            }

            if (state_delta.code.has_value()) {
                byte_string const code{
                    state_delta.code.value().data(),
                    state_delta.code.value().size()};
                state.set_code(address, code);
            }

            auto update_state =
                [&](std::map<std::vector<uint8_t>, std::vector<uint8_t>> const
                        &diff) {
                    for (auto const &[key, value] : diff) {
                        bytes32_t storage_key;
                        bytes32_t storage_value;
                        std::memcpy(
                            storage_key.bytes, key.data(), sizeof(bytes32_t));
                        std::memcpy(
                            storage_value.bytes,
                            value.data(),
                            sizeof(bytes32_t));

                        state.set_storage(address, storage_key, storage_value);
                    }
                };

            // Remove single storage
            if (!state_delta.state_diff.empty()) {
                // we need to access the account first before accessing its
                // storage
                (void)state.get_nonce(address);
                update_state(state_delta.state_diff);
            }

            // Remove all override
            if (!state_delta.state.empty()) {
                state.set_to_state_incarnation(address);
                update_state(state_delta.state);
            }
        }

        // nonce validation hack
        auto const &acct = state.recent_account(sender);
        enriched_txn.nonce = acct.has_value() ? acct.value().nonce : 0;

        BOOST_OUTCOME_TRY(validate_transaction(enriched_txn, acct));
        auto const tx_context = get_tx_context<rev>(
            enriched_txn, sender, header, chain.get_chain_id());
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

monad_evmc_result eth_call(
    bytes rlp_txn, uint64_t txn_len, bytes rlp_header, uint64_t header_len,
    bytes sender, uint64_t const block_number, char const *triedb_path,
    char const *blockdb_path, monad_state_override_set *state_overrides)
{
    cxx_bytes cxx_rlp_txn(rlp_txn, rlp_txn + txn_len);
    byte_string_view cxx_rlp_txn_view(cxx_rlp_txn.begin(), cxx_rlp_txn.end());
    auto const txn_result = rlp::decode_transaction(cxx_rlp_txn_view);
    MONAD_ASSERT(cxx_rlp_txn_view.empty());
    MONAD_ASSERT(!txn_result.has_error());
    auto const txn = txn_result.value();

    cxx_bytes cxx_rlp_header(rlp_header, rlp_header + header_len);
    byte_string_view cxx_rlp_header_view(
        cxx_rlp_header.begin(), cxx_rlp_header.end());
    auto const block_header_result =
        rlp::decode_block_header(cxx_rlp_header_view);
    MONAD_ASSERT(cxx_rlp_header_view.empty());
    MONAD_ASSERT(!block_header_result.has_error());
    auto const block_header = block_header_result.value();

    cxx_bytes cxx_sender(sender, sender + sizeof(Address));
    Address sender_addr{};
    std::memcpy(sender_addr.bytes, cxx_sender.data(), sizeof(Address));

    BlockHashBuffer buffer{};
    for (size_t i = block_number < 256 ? 1 : block_number - 255;
         i <= block_number;
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

    std::vector<std::filesystem::path> paths;
    if (std::filesystem::is_block_file(triedb_path)) {
        paths.emplace_back(triedb_path);
    }
    else {
        MONAD_ASSERT(std::filesystem::is_directory(triedb_path));
        for (auto const &file :
             std::filesystem::directory_iterator(triedb_path)) {
            paths.emplace_back(file.path());
        }
    }
    auto const result = eth_call_impl(
        txn,
        block_header,
        block_number,
        sender_addr,
        buffer,
        paths,
        state_overrides);
    monad_evmc_result ret;
    if (MONAD_UNLIKELY(result.has_error())) {
        ret.status_code = -1;
        std::string const error_message(
            result.assume_error().message().data(),
            result.assume_error().message().size());
        ret.message = new char[error_message.size() + 1];
        std::strcpy(ret.message, error_message.c_str());
    }
    else {
        int64_t const gas_used = static_cast<int64_t>(txn.gas_limit) -
                                 result.assume_value().gas_left;
        ret.status_code = result.assume_value().status_code;
        ret.output_size = result.assume_value().output_size;
        ret.output_data = new uint8_t[ret.output_size];
        std::memcpy(
            ret.output_data,
            result.assume_value().output_data,
            ret.output_size);
        ret.gas_used = gas_used;
        ret.gas_refund = result.assume_value().gas_refund;
    }
    return ret;
}

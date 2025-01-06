#include "test_db.hpp"

#include <monad/db/trie_db.hpp>

#include <cstdio>
#include <filesystem>
#include <string>

using namespace monad;

struct TestDb
{
    std::filesystem::path path;
    OnDiskMachine machine;
    mpt::Db db;
    TrieDb tdb;

    TestDb(std::filesystem::path const &path)
        : path{path}
        , db{machine, mpt::OnDiskDbConfig{.append = false, .dbname_paths = {path}}}
        , tdb{db}
    {
    }
};

TestDb *make_testdb()
{
    auto const path = [] {
        std::filesystem::path dbname(
            MONAD_ASYNC_NAMESPACE::working_temporary_directory() /
            "monad_eth_call_test2_XXXXXX");
        int const fd = ::mkstemp((char *)dbname.native().data());
        MONAD_ASSERT(fd != -1);
        MONAD_ASSERT(
            -1 !=
            ::ftruncate(fd, static_cast<off_t>(8ULL * 1024 * 1024 * 1024)));
        ::close(fd);
        return dbname;
    }();
    return new TestDb{path};
}

void testdb_load_callenv(TestDb *const db)
{
    StateDeltas state_deltas;
    Code code;
    code.emplace(
        0x8e0388ecf64cfa76b3a6af159f77451519a7f9bb862e4cce24175c791fdcb0df_bytes32,
        std::make_shared<CodeAnalysis>(analyze(
            evmc::from_hex(
                "0x60004381526020014681526020014181526020014881526020014"
                "481526020013281526020013481526020016000f3")
                .value())));
    state_deltas.emplace(
        0x9344b07175800259691961298ca11c824e65032d_address,
        StateDelta{
            .account = {
                std::nullopt,
                Account{
                    .code_hash =
                        0x8e0388ecf64cfa76b3a6af159f77451519a7f9bb862e4cce24175c791fdcb0df_bytes32,
                    .nonce = 1}}});
    db->tdb.commit(state_deltas, code, {});
}

void testdb_load_transfer(TestDb *const db)
{
    StateDeltas state_deltas;
    Code code;
    state_deltas.emplace(
        0x0000000000000000000001000000000000000000_address,
        StateDelta{
            .account = {
                std::nullopt, Account{.balance = 100'000u, .nonce = 1}}});

    db->tdb.commit(state_deltas, code, {});
}

void testdb_load_callcontract(TestDb *const db)
{
    StateDeltas state_deltas;
    Code code;
    code.emplace(
        0x975f732458c1f6c2dd22b866b031cc509c6d4f788b1f020e351c1cdba48dacca_bytes32,
        std::make_shared<CodeAnalysis>(analyze(
            evmc::from_hex(
                "0x366002146022577177726f6e672d63616c6c6461746173697a6560005260"
                "12600efd5b60003560f01c61ff01146047576d77726f6e672d63616c6c6461"
                "7461600052600e6012fd5b61ffee6000526002601ef3")
                .value())));
    code.emplace(
        0x5fe7f977e71dba2ea1a68e21057beebb9be2ac30c6410aa38d4f3fbe41dcffd2_bytes32,
        std::make_shared<CodeAnalysis>(
            analyze(evmc::from_hex("0x01").value())));

    state_deltas.emplace(
        0x17e7eedce4ac02ef114a7ed9fe6e2f33feba1667_address,
        StateDelta{
            .account = {
                std::nullopt,
                Account{
                    .code_hash =
                        0x975f732458c1f6c2dd22b866b031cc509c6d4f788b1f020e351c1cdba48dacca_bytes32,
                    .nonce = 1}}});

    state_deltas.emplace(
        0x000000000000000000000000000000000000000a_address,
        StateDelta{
            .account = {
                std::nullopt,
                Account{
                    .code_hash =
                        0x5fe7f977e71dba2ea1a68e21057beebb9be2ac30c6410aa38d4f3fbe41dcffd2_bytes32,
                    .nonce = 1}}});
    db->tdb.commit(state_deltas, code, {});
}

std::string testdb_path(TestDb const *const db)
{
    return db->path;
}

void destroy_testdb(TestDb *const db)
{
    delete db;
}

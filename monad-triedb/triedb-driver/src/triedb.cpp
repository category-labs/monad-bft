#include <filesystem>
#include <limits>
#include <optional>
#include <vector>

#include <monad/mpt/db.hpp>
#include <monad/mpt/ondisk_db_config.hpp>

#include "triedb.h"

using bytes32_t = uint8_t[32];

constexpr unsigned char state_nibble = 0;
constexpr unsigned char code_nibble = 1;

struct NopMachine : public monad::mpt::StateMachine
{
    std::unique_ptr<StateMachine> clone() const override
    {
        abort();
    }

    void down(unsigned char) override
    {
        abort();
    }

    void up(size_t) override
    {
        abort();
    }

    monad::mpt::Compute &get_compute() const override
    {
        abort();
    }

    bool cache() const override
    {
        abort();
    }

    bool compact() const override
    {
        abort();
    }
};

NopMachine machine;

struct triedb
{
    explicit triedb(std::vector<std::filesystem::path> dbname_paths)
        : db_{machine,
              monad::mpt::OnDiskDbConfig{
                  .append = true, // TODO delete once read-only
                  .dbname_paths = std::move(dbname_paths)
                  // TODO set read-only flag once it exists
              }}
    {
    }

    monad::mpt::Db db_;
};

int triedb_open(char const *dbdirpath, triedb **db)
{
    if (*db != nullptr) {
        return -1;
    }

    std::vector<std::filesystem::path> paths;
    for (auto const &file : std::filesystem::directory_iterator(dbdirpath)) {
        paths.emplace_back(file.path());
    }

    *db = new triedb{std::move(paths)};
    return 0;
}

int triedb_close(triedb *db)
{
    delete db;
    return 0;
}

int triedb_read(triedb *db, bytes key, uint8_t key_len_nibbles, bytes *value)
{
    // is there a reason this isn't const?
    auto result = db->db_.get(monad::mpt::NibblesView{0, key_len_nibbles, key});
    if (!result.has_value()) {
        return -1;
    }

    auto const &value_view = result.value();
    if ((value_view.size() >> std::numeric_limits<int>::digits) != 0) {
        // value length doesn't fit in return type
        return -2;
    }
    int const value_len = (int)value_view.size();
    *value = new uint8_t[value_len];
    memcpy((void *)*value, value_view.data(), value_len);
    return value_len;
}

int triedb_finalize(bytes value)
{
    delete value;
    return 0;
}

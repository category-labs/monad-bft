#include <filesystem>
#include <limits>
#include <optional>
#include <vector>
#include <iostream>
#include <cassert>

#include <monad/mpt/db.hpp>
#include <monad/mpt/ondisk_db_config.hpp>

#include "triedb.h"

struct triedb
{
    explicit triedb(std::vector<std::filesystem::path> dbname_paths)
        : db_{monad::mpt::ReadOnlyOnDiskDbConfig{
              .disable_mismatching_storage_pool_check = true,
              .dbname_paths = std::move(dbname_paths)}}
    {
    }

    monad::mpt::Db db_;
};

int triedb_open(char const *dbdirpath, triedb **db)
{
    if (*db != nullptr) {
        return -1;
    }

    std::error_code ec;
    std::vector<std::filesystem::path> paths;
    for (auto const &file :
         std::filesystem::directory_iterator(dbdirpath, ec)) {
        paths.emplace_back(file.path());
    }

    if (ec) {
        return -2;
    }

    *db = new triedb{std::move(paths)};
    return 0;
}

int triedb_close(triedb *db)
{
    delete db;
    return 0;
}

int triedb_read(triedb *db, bytes key, uint8_t key_len_nibbles, bytes *value, uint64_t block_id)
{
    if (!db->db_.is_latest()) {
        db->db_.load_latest();
    }
    auto result = db->db_.get(monad::mpt::NibblesView{0, key_len_nibbles, key}, block_id);
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

uint64_t triedb_latest_block(triedb *db)
{
    if (!db->db_.is_latest()) {
        db->db_.load_latest();
    }

    std::optional<uint64_t> latest_block_id = db->db_.get_latest_block_id();

    if (latest_block_id.has_value()) {
        return latest_block_id.value();
    } else {
        // no block has been produced
        return 0;
    }
}

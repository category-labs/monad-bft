#include <cstring>
#include <map>
#include <vector>

#include "triedb.h"

struct triedb
{
    std::map<std::vector<uint8_t>, std::vector<uint8_t>> db_{
        {{1, 2, 3}, {4, 5, 6}}, {{7, 8, 9}, {10, 11, 12}}};
};

extern "C"
{
int triedb_open(char const *dbdirpath, triedb **db)
{
    if (*db != nullptr) {
        return -1;
    }
    auto *mockdb = new triedb{};
    *db = mockdb;
    return 0;
}

int triedb_close(triedb *db)
{
    delete db;
    return 0;
}

int triedb_read(triedb *db, bytes key, uint8_t key_len_nibbles, bytes *value, uint64_t block_id)
{
    std::vector<uint8_t> key_vec{key, key + (key_len_nibbles + 1) / 2};
    auto const it = db->db_.find(key_vec);
    if (it == db->db_.end()) {
        return -1;
    }
    auto const &value_vec = it->second;
    int const value_len = (int)value_vec.size();
    *value = new uint8_t[value_len];
    memcpy((void *)*value, value_vec.data(), value_len);
    return value_len;
}

int triedb_finalize(bytes value)
{
    delete value;
    return 0;
}
} // end extern "C"

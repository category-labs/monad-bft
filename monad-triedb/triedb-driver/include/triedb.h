#ifdef __cplusplus
extern "C"
{
#endif

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct triedb triedb;
int triedb_open(char const *dbdirpath, triedb **);
int triedb_close(triedb *);

typedef uint8_t const *bytes;
// returns -1 if key not found
// if >= 0, returns length of value
int triedb_read(
    triedb *, bytes key, uint8_t key_len_nibbles, bytes *value,
    uint64_t block_id);
// calls (*completed) when read is
// complete. length is -1 if key not
// found. If >=0, returns length of
// value. Call triedb_finalize when
// done with the value.
void triedb_async_read(
    triedb *, bytes key, uint8_t key_len_nibbles, uint64_t block_id,
    void (*completed)(bytes value, int length, void *user), void *user);

// traverse the trie.
enum triedb_async_traverse_callback
{
    triedb_async_traverse_callback_value,
    triedb_async_traverse_callback_finished_normally,
    triedb_async_traverse_callback_finished_early
};

typedef void (*callback_func)(
    enum triedb_async_traverse_callback kind, void *context, bytes path,
    size_t path_len, bytes value, size_t value_len);
bool triedb_traverse(
    triedb *, bytes key, uint8_t key_len_nibbles, uint64_t block_id,
    void *context, callback_func callback);
void triedb_async_traverse(
    triedb *, bytes key, uint8_t key_len_nibbles, uint64_t block_id,
    void *context, callback_func callback);
void triedb_async_ranged_get(
    triedb *, bytes prefix_key, uint8_t prefix_len_nibbles, bytes min_key,
    uint8_t min_len_nibbles, bytes max_key, uint8_t max_len_nibbles,
    uint64_t block_id, void *context, callback_func callback);
// pumps async reads, processing no
// more than count maximum, returning
// how many were processed.
size_t triedb_poll(triedb *, bool blocking, size_t count);
int triedb_finalize(bytes value);

// returns MAX if doesn't exist
uint64_t triedb_latest_voted_block(triedb *);
// returns MAX if doesn't exist
uint64_t triedb_latest_voted_round(triedb *);
// returns MAX if doesn't exist
uint64_t triedb_latest_finalized_block(triedb *);
// returns MAX if doesn't exist
uint64_t triedb_latest_verified_block(triedb *);

// returns MAX if doesn't exist
uint64_t triedb_earliest_finalized_block(triedb *);

#pragma pack(push, 1)

typedef struct monad_validator
{
    uint8_t secp_pubkey[33];
    uint8_t bls_pubkey[48];
    uint8_t stake[32];
} monad_validator;

typedef struct monad_validator_set
{
    struct monad_validator *valset;
    uint64_t length;
} monad_validator_set;

#pragma pack(pop)

void monad_free_valset(monad_validator_set);

monad_validator_set monad_read_valset(triedb *, size_t block_num, bool get_next);

#ifdef __cplusplus
}
#endif

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
// traverse the trie
typedef void (*callback_func)(
    void *context, bytes path, size_t path_len, bytes value,
    size_t value_len);
void triedb_traverse(
    triedb *, bytes key, uint8_t key_len_nibbles, uint64_t block_id,
    void *context, callback_func callback);
// pumps async reads, processing no
// more than count maximum, returning
// how many were processed.
size_t triedb_poll(triedb *, bool blocking, size_t count);
int triedb_finalize(bytes value);

// returns MAX if doesn't exist
uint64_t triedb_latest_finalized_block(triedb *);
// returns MAX if doesn't exist
uint64_t triedb_latest_verified_block(triedb *);

// returns MAX if doesn't exist
uint64_t triedb_earliest_finalized_block(triedb *);

#ifdef __cplusplus
}
#endif

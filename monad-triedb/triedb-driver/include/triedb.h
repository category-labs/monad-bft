#ifdef __cplusplus
extern "C"
{
#endif

#include <stdint.h>

typedef struct triedb triedb;
int triedb_open(char const *dbdirpath, triedb **);
int triedb_close(triedb *);

typedef uint8_t const *bytes;
// returns -1 if key not found
// if >= 0, returns length of value
int triedb_read(triedb *, bytes key, uint8_t key_len_nibbles, bytes *value, uint64_t block_id);
int triedb_finalize(bytes value);
uint64_t triedb_latest_block(triedb *);

#ifdef __cplusplus
}
#endif

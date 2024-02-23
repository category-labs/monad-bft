#ifdef __cplusplus
extern "C"
{
#endif

typedef struct triedb triedb;
int triedb_open(char const *dbdirpath, triedb **);
int triedb_close(triedb *);

typedef unsigned char uint8_t;
typedef uint8_t const *bytes;
// returns -1 if key not found
// if >= 0, returns length of value
int triedb_read(triedb *, bytes key, uint8_t key_len_nibbles, bytes *value);
int triedb_finalize(bytes value);

#ifdef __cplusplus
}
#endif

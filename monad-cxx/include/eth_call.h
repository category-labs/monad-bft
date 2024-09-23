#ifdef __cplusplus
extern "C"
{
#endif

#include <stddef.h>
#include <stdint.h>

typedef uint8_t const *bytes;

// eth_call return type
struct monad_evmc_result
{
    int64_t status_code;
    uint8_t *output_data;
    // we need this because output_data is bytes
    uint64_t output_size;
    char *message;
    int64_t gas_used;
    int64_t gas_refund;
};

int64_t get_status_code(const struct monad_evmc_result *);
bytes get_output_data(const struct monad_evmc_result *);
uint64_t get_output_size(const struct monad_evmc_result *);
char const *get_message(const struct monad_evmc_result *);
int64_t get_gas_used(const struct monad_evmc_result *);
int64_t get_gas_refund(const struct monad_evmc_result *);

// state override set
struct monad_state_override_set;

struct monad_state_override_set *create_empty_state_override_set();
void add_override_address(struct monad_state_override_set *, bytes address);
void set_override_balance(
    struct monad_state_override_set *, bytes address, bytes balance,
    uint64_t balance_len);
void set_override_nonce(
    struct monad_state_override_set *, bytes address, uint64_t nonce);
void set_override_code(
    struct monad_state_override_set *, bytes address, bytes code,
    uint64_t code_len);
void set_override_state_diff(
    struct monad_state_override_set *, bytes address, bytes key, bytes value);
void set_override_state(
    struct monad_state_override_set *, bytes address, bytes key, bytes value);

struct monad_evmc_result eth_call(
    bytes rlp_txn, uint64_t txn_len, bytes rlp_header, uint64_t header_len,
    bytes rlp_sender, uint64_t const block_number, char const *triedb_path,
    char const *blockdb_path, struct monad_state_override_set *state_overrides);

#ifdef __cplusplus
}
#endif

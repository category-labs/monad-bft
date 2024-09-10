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
    int status_code;
    uint8_t *output_data;
    char *message;
    int64_t gas_used;
    int64_t gas_refund;

    int get_status_code() const;
    bytes get_output_data() const;
    char const *get_message() const;
    int64_t get_gas_used() const;
    int64_t get_gas_refund() const;
};

// state override set
typedef struct monad_state_override_set monad_state_override_set;

void add_override_address(monad_state_override_set *, bytes address);
void set_override_balance(
    monad_state_override_set *, bytes address, bytes balance);
void set_override_nonce(
    monad_state_override_set *, bytes address, uint64_t nonce);
void set_override_code(monad_state_override_set *, bytes address, bytes code);
void set_override_state_diff(
    monad_state_override_set *, bytes address, bytes key, bytes value);
void set_override_state(
    monad_state_override_set *, bytes address, bytes key, bytes value);

monad_evmc_result eth_call(
    bytes rlp_txn, bytes rlp_header, bytes rlp_sender,
    uint64_t const block_number, char const *triedb_path,
    char const *blockdb_path, monad_state_override_set const &state_overrides);

#ifdef __cplusplus
}
#endif

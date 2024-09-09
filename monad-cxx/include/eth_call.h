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
    bytes output_data;
    bytes message;
    int64_t gas_used;
    int64_t gas_refund;

    int get_status_code() const;
    bytes get_output_data() const;
    bytes get_message() const;
    int64_t get_gas_used() const;
    int64_t get_gas_refund() const;
};

// state override objects
typedef struct monad_state_override_object monad_state_override_object;
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

#ifdef __cplusplus
}
#endif

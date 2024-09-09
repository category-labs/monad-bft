use std::{
    io::{Error, ErrorKind},
    iter,
    num::NonZeroU16,
};

use crate::r10::{
    lt::MAX_TRIPLES,
    nonsystematic::decoder::{Buffer, BufferWeightMap, Decoder, IntermediateSymbol},
    CodeParameters, SOURCE_SYMBOLS_MAX, SOURCE_SYMBOLS_MIN,
};

impl Decoder {
    pub fn new(num_source_symbols: usize) -> Result<Decoder, Error> {
        if !(SOURCE_SYMBOLS_MIN..=SOURCE_SYMBOLS_MAX).contains(&num_source_symbols) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "number of source symbols {} not in range {}..={}",
                    num_source_symbols, SOURCE_SYMBOLS_MIN, SOURCE_SYMBOLS_MAX
                ),
            ));
        }

        let params = CodeParameters::new(num_source_symbols).map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "error creating CodeParameters for {} source symbols: {}",
                    num_source_symbols, err
                ),
            )
        })?;

        // Initialize buffer_state and intermediate_symbol_state for the constraint
        // symbol buffers that we start with, according to the upper part of the A
        // matrix from RFC 5053, where the rows correspond to buffers and the columns
        // to intermediate symbols:
        //
        //               K               S       H
        //   +-----------------------+-------+-------+
        //   |                       |       |       |
        // S |        G_LDPC         |  I_S  | 0_SxH |
        //   |                       |       |       |
        //   +-----------------------+-------+-------+
        //   |                               |       |
        // H |        G_Half                 |  I_H  |
        //   |                               |       |
        //   +-------------------------------+-------+

        let mut buffer_state: Vec<Buffer> = iter::repeat_with(Buffer::new)
            .take(params.num_ldpc_symbols() + params.num_half_symbols())
            .collect();

        let mut intermediate_symbol_state: Vec<IntermediateSymbol> =
            iter::repeat_with(IntermediateSymbol::new)
                .take(params.num_intermediate_symbols())
                .collect();

        // G_LDPC
        params.g_ldpc(|buffer_index, intermediate_symbol_id| {
            buffer_state[buffer_index].append_active_intermediate_symbol_id(intermediate_symbol_id);
            intermediate_symbol_state[intermediate_symbol_id].active_push(buffer_index);
        });

        // I_S
        #[allow(clippy::needless_range_loop)]
        for buffer_index in 0..params.num_ldpc_symbols() {
            let intermediate_symbol_id = params.num_source_symbols() + buffer_index;

            buffer_state[buffer_index].append_active_intermediate_symbol_id(intermediate_symbol_id);
            intermediate_symbol_state[intermediate_symbol_id].active_push(buffer_index);
        }

        // G_Half
        params.g_half(|i, intermediate_symbol_id| {
            let buffer_index = params.num_ldpc_symbols() + i;

            buffer_state[buffer_index].append_active_intermediate_symbol_id(intermediate_symbol_id);
            intermediate_symbol_state[intermediate_symbol_id].active_push(buffer_index);
        });

        // I_H
        for i in 0..params.num_half_symbols() {
            let buffer_index = params.num_ldpc_symbols() + i;
            let intermediate_symbol_id =
                params.num_source_symbols() + params.num_ldpc_symbols() + i;

            buffer_state[buffer_index].append_active_intermediate_symbol_id(intermediate_symbol_id);
            intermediate_symbol_state[intermediate_symbol_id].active_push(buffer_index);
        }

        let mut buffers_active_usable = BufferWeightMap::new(MAX_TRIPLES);

        for (i, buffer_state) in buffer_state.iter().enumerate() {
            buffers_active_usable
                .insert_buffer_weight(i, NonZeroU16::new(buffer_state.active_used_weight).unwrap());
        }

        let buffers_inactivated = BufferWeightMap::new(MAX_TRIPLES);

        let decoder = Decoder {
            params,
            buffer_state,
            intermediate_symbol_state,
            buffers_active_usable,
            buffers_inactivated,
            num_redundant_buffers: 0,
            num_source_symbols_paired: 0,
        };

        decoder.check();

        Ok(decoder)
    }
}

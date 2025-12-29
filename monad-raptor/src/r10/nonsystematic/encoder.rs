// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// Encoder for a non-systematic variant of the Raptor code described in RFC 5053.

use std::{
    io::{Error, ErrorKind},
    iter,
};

use rayon::prelude::*;

use crate::{
    r10::{CodeParameters, MAX_DEGREE, SOURCE_SYMBOLS_MAX, SOURCE_SYMBOLS_MIN},
    xor_eq,
    xor_eq::xor_eq,
};

#[derive(Debug)]
pub struct Encoder<'a> {
    // Reference to the provided source data, and the requested symbol length.
    src: &'a [u8],
    symbol_len: usize,

    // R10 parameters for K = ceil(src.len() / symbol_len).max(SOURCE_SYMBOLS_MIN).
    params: CodeParameters,

    // Intermediate symbols i < num_full_source_symbols are taken from `src`.  This
    // corresponds to the data contained in the initial part of `src` that is an integer
    // multiple of symbol_len bytes long.
    //
    // Intermediate symbols i >= num_full_source_symbols are taken from `derived_symbols`.
    // This includes, in this order:
    // - The last partial source symbol in `src`, if `src` is not an integer multiple
    //   of symbol_len bytes long, padded to symbol_len bytes.
    // - A sufficient number of NUL-filled symbols to bring the number of source symbols
    //   up to SOURCE_SYMBOLS_MIN, if there are fewer than that number of symbols in the
    //   (padded) source data.
    // - The computed LDPC symbols.
    // - The computed Half symbols.
    num_full_source_symbols: usize,
    derived_symbols: Vec<Box<[u8]>>,
}

impl Encoder<'_> {
    pub fn new(src: &[u8], symbol_len: usize) -> Result<Encoder<'_>, Error> {
        if symbol_len == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "symbol_len == 0".to_string(),
            ));
        }

        // The number of full source symbols in `src`.  Within this function, we use this
        // as a pivot to decide when to grab source symbol data from `src` and when to use
        // `shadowed_source_symbols`.
        let num_full_source_symbols = src.len() / symbol_len;

        let mut num_source_symbols = num_full_source_symbols;

        let mut shadowed_source_symbols: Vec<Box<[u8]>> = Vec::new();

        // If `src` is not an integer multiple of symbol_len bytes long, we stuff a NUL-padded
        // version of the last partial symbol in `src` into `shadowed_source_symbols`.
        {
            let last_source_symbol_len = src.len() % symbol_len;

            if last_source_symbol_len != 0 {
                let mut symbol = vec![0; symbol_len].into_boxed_slice();

                symbol[0..last_source_symbol_len]
                    .copy_from_slice(&src[num_full_source_symbols * symbol_len..]);

                shadowed_source_symbols.push(symbol);
                num_source_symbols += 1;
            }
        }

        // If there are fewer than the R10 minimum number of source symbols in `src`, we stuff
        // `shadowed_source_symbols` with a sufficient number of NUL-filled symbols to reach
        // that lower bound.
        if num_source_symbols < SOURCE_SYMBOLS_MIN {
            for _ in num_source_symbols..SOURCE_SYMBOLS_MIN {
                shadowed_source_symbols.push(vec![0; symbol_len].into_boxed_slice());
            }

            num_source_symbols = SOURCE_SYMBOLS_MIN;
        }

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

        // Create the G_LDPC and G_Half sub-matrices of the A matrix:
        //
        //               K               S       H
        //   +-----------------------+-------+-------+
        //   |                       |       |       |
        // S |        G_LDPC         |  I_S  | 0_SxH |
        //   |                       |       |       |
        //   + - - - - - - - - - - - +-------+-------+
        //   |                       :       |       |
        // H |        G_Half         :       |  I_H  |
        //   |                       :       |       |
        //   +-------------------------------+-------+
        //
        // We store the K leftmost columns of G_LPDC and G_Half in the same matrix
        // (g_ldpc_half), and the S rightmost columns of G_Half in a separate matrix
        // (g_half_right).
        let mut g_ldpc_half: Vec<Vec<u16>> = iter::repeat_with(|| {
            Vec::<u16>::with_capacity(params.num_source_symbols() + params.num_ldpc_symbols())
        })
        .take(params.num_ldpc_symbols() + params.num_half_symbols())
        .collect();

        let mut g_half_right: Vec<Vec<u16>> =
            iter::repeat_with(|| Vec::<u16>::with_capacity(params.num_ldpc_symbols()))
                .take(params.num_half_symbols())
                .collect();

        params.g_ldpc(|i, j| g_ldpc_half[i].push(j as u16));

        params.g_half(|i, j| {
            if j < params.num_source_symbols() {
                g_ldpc_half[params.num_ldpc_symbols() + i].push(j as u16);
            } else {
                g_half_right[i].push((j - params.num_source_symbols()) as u16);
            }
        });

        // We first multiply by g_ldpc_half, which we do in parallel if num_source_symbols is
        // large enough for parallelization to be a win, or serially otherwise.
        let make_redundant_symbol = |source_set: &Vec<u16>| {
            let mut symbol = vec![0; symbol_len].into_boxed_slice();

            for chunk in source_set
                .iter()
                .map(|index| {
                    let index = usize::from(*index);

                    if index < num_full_source_symbols {
                        &src[index * symbol_len..(index + 1) * symbol_len]
                    } else {
                        &shadowed_source_symbols[index - num_full_source_symbols]
                    }
                })
                .collect::<Vec<&[u8]>>()
                .chunks(xor_eq::MAX_SOURCES)
            {
                xor_eq(&mut symbol, chunk);
            }

            symbol
        };

        let mut ldpc_half_symbols: Vec<Box<[u8]>> = if num_source_symbols >= 1024 {
            g_ldpc_half.par_iter().map(make_redundant_symbol).collect()
        } else {
            g_ldpc_half.iter().map(make_redundant_symbol).collect()
        };

        // Now we multiply by g_half_right, which is a sufficiently small computation for
        // parallelization to not ever be a win, so we do this serially.
        {
            let (ldpc_symbols, half_symbols) =
                ldpc_half_symbols.split_at_mut(params.num_ldpc_symbols());

            (0..params.num_half_symbols()).for_each(|i| {
                for chunk in g_half_right[i]
                    .iter()
                    .map(|index| &*ldpc_symbols[usize::from(*index)])
                    .collect::<Vec<&[u8]>>()
                    .chunks(xor_eq::MAX_SOURCES)
                {
                    xor_eq(&mut half_symbols[i], chunk);
                }
            });
        }

        // Prepend any shadowed source symbols to the LDPC/Half symbols.
        let derived_symbols = if shadowed_source_symbols.is_empty() {
            ldpc_half_symbols
        } else {
            shadowed_source_symbols.append(&mut ldpc_half_symbols);
            shadowed_source_symbols
        };

        Ok(Encoder {
            src,
            symbol_len,
            params,
            num_full_source_symbols,
            derived_symbols,
        })
    }

    pub fn num_source_symbols(&self) -> usize {
        self.params.num_source_symbols()
    }

    fn buffer(&self, index: usize) -> &[u8] {
        if index < self.num_full_source_symbols {
            &self.src[index * self.symbol_len..(index + 1) * self.symbol_len]
        } else {
            &self.derived_symbols[index - self.num_full_source_symbols]
        }
    }

    // Expects the output buffer to be filled with NUL bytes on entry.
    pub fn encode_symbol(&self, dst: &mut [u8], encoding_symbol_id: usize) {
        assert_eq!(dst.len(), self.symbol_len);

        let mut indices = Vec::<usize>::with_capacity(MAX_DEGREE);

        self.params
            .lt_sequence_op(encoding_symbol_id, |el| indices.push(el));

        for chunk in indices
            .into_iter()
            .map(|index| self.buffer(index))
            .collect::<Vec<&[u8]>>()
            .chunks(xor_eq::MAX_SOURCES)
        {
            xor_eq(dst, chunk);
        }
    }
}

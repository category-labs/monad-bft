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

use std::io::Write;

use crate::{util::BoundedWriter, CompressionAlgo};

pub struct NopCompression;

#[derive(Debug)]
pub struct NopCompressionError(pub String);

impl std::fmt::Display for NopCompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for NopCompressionError {}

impl CompressionAlgo for NopCompression {
    type CompressError = NopCompressionError;
    type DecompressError = NopCompressionError;

    fn new(_quality: u32, _window_bits: u32, _custom_dictionary: Vec<u8>) -> Self {
        Self
    }

    fn compress(
        &self,
        input: &[u8],
        output: &mut BoundedWriter,
    ) -> Result<(), Self::CompressError> {
        output
            .write_all(input)
            .map_err(|e| NopCompressionError(e.to_string()))
    }

    fn decompress(
        &self,
        input: &[u8],
        output: &mut BoundedWriter,
    ) -> Result<(), Self::DecompressError> {
        output
            .write_all(input)
            .map_err(|e| NopCompressionError(e.to_string()))
    }
}

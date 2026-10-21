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

use alloy_rlp::{Decodable, Encodable, Header, encode_list, list_length};
use bytes::BufMut;

use crate::{sig::Signed, types::NodeId};

/// Pre-encoded RLP spliced into a list.
pub(crate) struct Raw(pub Vec<u8>);

impl Encodable for Raw {
    fn encode(&self, out: &mut dyn BufMut) {
        out.put_slice(&self.0);
    }

    fn length(&self) -> usize {
        self.0.len()
    }
}

struct SignedBy<'a, B>(&'a (NodeId, Signed<B>));

impl<B: Encodable> Encodable for SignedBy<'_, B> {
    fn encode(&self, out: &mut dyn BufMut) {
        let fields: [&dyn Encodable; 2] = [&self.0.0, &self.0.1];
        encode_list::<_, dyn Encodable>(&fields, out);
    }

    fn length(&self) -> usize {
        let fields: [&dyn Encodable; 2] = [&self.0.0, &self.0.1];
        list_length::<_, dyn Encodable>(&fields)
    }
}

/// `[[signer, signed], ..]`
pub(crate) fn signed_list<B: Encodable>(items: &[(NodeId, Signed<B>)]) -> Raw {
    let wrapped: Vec<SignedBy<'_, B>> = items.iter().map(SignedBy).collect();
    let mut out = Vec::new();
    encode_list(&wrapped, &mut out);
    Raw(out)
}

pub(crate) fn decode_signed_list<B: Decodable>(
    buf: &mut &[u8],
) -> alloy_rlp::Result<Vec<(NodeId, Signed<B>)>> {
    let mut payload = Header::decode_bytes(buf, true)?;
    let mut items = Vec::new();
    while !payload.is_empty() {
        let mut item = Header::decode_bytes(&mut payload, true)?;
        let signer = NodeId::decode(&mut item)?;
        let signed = Signed::<B>::decode(&mut item)?;
        if !item.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        items.push((signer, signed));
    }
    Ok(items)
}

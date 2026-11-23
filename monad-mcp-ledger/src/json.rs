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

// minimal json object writer for the human-readable ledger copies.

use std::fmt::{Display, Write};

pub(crate) struct Object {
    out: String,
    first: bool,
}

impl Object {
    pub(crate) fn new() -> Self {
        Self {
            out: String::from("{"),
            first: true,
        }
    }

    fn key(&mut self, key: &str) -> &mut String {
        if !self.first {
            self.out.push(',');
        }
        self.first = false;
        push_str(&mut self.out, key);
        self.out.push(':');
        &mut self.out
    }

    pub(crate) fn num(&mut self, key: &str, value: impl Display) -> &mut Self {
        let _ = write!(self.key(key), "{value}");
        self
    }

    pub(crate) fn opt_num(&mut self, key: &str, value: Option<impl Display>) -> &mut Self {
        match value {
            Some(v) => self.num(key, v),
            None => self.raw(key, "null"),
        }
    }

    pub(crate) fn bool(&mut self, key: &str, value: bool) -> &mut Self {
        self.raw(key, if value { "true" } else { "false" })
    }

    pub(crate) fn str(&mut self, key: &str, value: &str) -> &mut Self {
        push_str(self.key(key), value);
        self
    }

    pub(crate) fn hex(&mut self, key: &str, bytes: &[u8]) -> &mut Self {
        self.str(key, &format!("0x{}", hex::encode(bytes)))
    }

    pub(crate) fn opt_hex(&mut self, key: &str, bytes: Option<&[u8]>) -> &mut Self {
        match bytes {
            Some(b) => self.hex(key, b),
            None => self.raw(key, "null"),
        }
    }

    // `value` must already be valid json.
    pub(crate) fn raw(&mut self, key: &str, value: &str) -> &mut Self {
        self.key(key).push_str(value);
        self
    }

    pub(crate) fn finish(&mut self) -> String {
        self.out.push('}');
        std::mem::take(&mut self.out)
    }
}

pub(crate) fn array(items: impl IntoIterator<Item = String>) -> String {
    let mut out = String::from("[");
    for (i, item) in items.into_iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&item);
    }
    out.push(']');
    out
}

fn push_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_strings() {
        let nasty = "a\"b\\c\nd\re\tf\u{1}g\u{7f}h\u{e9}";
        let s = Object::new().str("k\"", nasty).finish();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["k\""], nasty);
    }

    #[test]
    fn renders_all_kinds() {
        let s = Object::new()
            .num("n", u128::MAX)
            .opt_num("none", None::<u64>)
            .opt_num("some", Some(7u32))
            .bool("t", true)
            .hex("h", &[0xab, 0x01])
            .opt_hex("nh", None)
            .raw("a", &array(["1".into(), "{}".into()]))
            .raw("e", &array([]))
            .finish();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert!(s.contains(&format!("\"n\":{}", u128::MAX)));
        assert!(v["n"].is_number());
        assert!(v["none"].is_null());
        assert_eq!(v["some"], 7);
        assert_eq!(v["t"], true);
        assert_eq!(v["h"], "0xab01");
        assert!(v["nh"].is_null());
        assert_eq!(v["a"], serde_json::json!([1, {}]));
        assert_eq!(v["e"], serde_json::json!([]));
        assert_eq!(Object::new().finish(), "{}");
    }
}

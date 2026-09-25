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

use std::{io::Write, sync::LazyLock};

use actix_web::mime::{self, Mime};
use flate2::{Compression, write::GzEncoder};
use monad_mcp_chorus::ledger::keccak256;

pub struct Asset {
    pub body: &'static str,
    // compressed once at the best level; the response middleware only uses a fast one.
    pub gzip: Vec<u8>,
    pub content_type: Mime,
    pub etag: String,
}

impl Asset {
    fn new(body: &'static str, content_type: Mime) -> Self {
        let mut gz = GzEncoder::new(Vec::new(), Compression::best());
        gz.write_all(body.as_bytes())
            .expect("writing to a vec cannot fail");
        Self {
            body,
            gzip: gz.finish().expect("writing to a vec cannot fail"),
            content_type,
            etag: hex::encode(&keccak256(body.as_bytes())[..8]),
        }
    }
}

pub static INDEX_HTML: LazyLock<Asset> =
    LazyLock::new(|| Asset::new(include_str!("../static/index.html"), mime::TEXT_HTML_UTF_8));
pub static APP_CSS: LazyLock<Asset> =
    LazyLock::new(|| Asset::new(include_str!("../static/app.css"), mime::TEXT_CSS_UTF_8));
pub static APP_JS: LazyLock<Asset> = LazyLock::new(|| {
    Asset::new(
        include_str!("../static/app.js"),
        mime::APPLICATION_JAVASCRIPT_UTF_8,
    )
});

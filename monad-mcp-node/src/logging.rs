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

use std::{
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use tracing::{Event, Level, Subscriber};
use tracing_subscriber::{
    EnvFilter,
    fmt::{
        FmtContext, FormatEvent, FormatFields, FormattedFields, format::Writer, time::FormatTime,
    },
    registry::LookupSpan,
};

// info and above unless RUST_LOG says otherwise. A line reads
// "time level node{id}: fields", the node in a color of its own.
pub fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .event_format(NodeLine)
        .init();
}

struct NodeLine;

impl<S, N> FormatEvent<S, N> for NodeLine
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut w: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let ansi = w.has_ansi_escapes();
        TimeOfDay.format_time(&mut w)?;
        write_level(&mut w, *event.metadata().level(), ansi)?;
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                if span.name() != "node" {
                    continue;
                }
                let extensions = span.extensions();
                let Some(fields) = extensions.get::<FormattedFields<N>>() else {
                    continue;
                };
                write_node(&mut w, fields, ansi)?;
            }
        }
        ctx.format_fields(w.by_ref(), event)?;
        writeln!(w)
    }
}

fn write_level(w: &mut Writer<'_>, level: Level, ansi: bool) -> fmt::Result {
    let code = match level {
        Level::ERROR => 31,
        Level::WARN => 33,
        Level::INFO => 32,
        Level::DEBUG => 34,
        Level::TRACE => 35,
    };
    if ansi {
        write!(w, " \x1b[{code}m{level:>5}\x1b[0m ")
    } else {
        write!(w, " {level:>5} ")
    }
}

// the span's one field is the id; its formatted form ends with the
// digits, whatever escapes surround them. The color follows the id.
fn write_node(w: &mut Writer<'_>, fields: &FormattedFields<impl Sized>, ansi: bool) -> fmt::Result {
    let formatted = fields.fields.as_str();
    let digits = formatted.trim_end_matches(|c: char| !c.is_ascii_digit());
    let digits_start = digits.trim_end_matches(|c: char| c.is_ascii_digit()).len();
    let id: u64 = digits[digits_start..].parse().unwrap_or(0);
    if ansi {
        let code = NODE_COLORS[id as usize % NODE_COLORS.len()];
        write!(w, "\x1b[1;{code}mnode{id}\x1b[0m: ")
    } else {
        write!(w, "node{id}: ")
    }
}

// text in an ansi color, for a value worth a glance
pub fn paint(code: u8, text: &str) -> String {
    format!("\x1b[{code}m{text}\x1b[0m")
}

pub const GREEN: u8 = 32;
pub const YELLOW: u8 = 33;

// the six standard and six bright colors, red through cyan
const NODE_COLORS: [u8; 12] = [31, 32, 33, 34, 35, 36, 91, 92, 93, 94, 95, 96];

struct TimeOfDay;

impl FormatTime for TimeOfDay {
    fn format_time(&self, w: &mut Writer<'_>) -> fmt::Result {
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is past 1970");
        let seconds = since_epoch.as_secs();
        write!(
            w,
            "{:02}:{:02}:{:02}.{:03}",
            seconds / 3600 % 24,
            seconds / 60 % 60,
            seconds % 60,
            since_epoch.subsec_millis()
        )
    }
}

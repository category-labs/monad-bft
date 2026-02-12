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

use std::fmt;

/// Classification of an archive error, enabling programmatic error handling
/// without sacrificing eyre's context-chaining ergonomics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Transient I/O or connectivity failure (typically retriable).
    Network,
    /// The requested resource does not exist.
    NotFound,
    /// Failed to parse, decode, or deserialize data.
    Parse,
    /// An input precondition was violated.
    Validation,
    /// A storage backend operational error.
    Storage,
    /// Unclassified internal error (default for bare conversions).
    Internal,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Network => write!(f, "network"),
            Self::NotFound => write!(f, "not found"),
            Self::Parse => write!(f, "parse"),
            Self::Validation => write!(f, "validation"),
            Self::Storage => write!(f, "storage"),
            Self::Internal => write!(f, "internal"),
        }
    }
}

/// A typed archive error combining a matchable [`ErrorKind`] with an
/// [`eyre::Report`] context chain for rich diagnostics.
pub struct Error {
    kind: ErrorKind,
    report: eyre::Report,
}

impl Error {
    pub fn new(kind: ErrorKind, report: eyre::Report) -> Self {
        Self { kind, report }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn report(&self) -> &eyre::Report {
        &self.report
    }

    pub fn into_report(self) -> eyre::Report {
        self.report
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {:?}", self.kind, self.report)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.report, f)
    }
}

/// Blanket conversion from an opaque `eyre::Report`. Defaults to
/// `ErrorKind::Internal` so that `bail!()`, `eyre!()`, and bare `?`
/// on eyre results continue to work when a function migrates to our
/// `Result<T>`.
impl From<eyre::Report> for Error {
    fn from(report: eyre::Report) -> Self {
        Self {
            kind: ErrorKind::Internal,
            report,
        }
    }
}

/// Reverse conversion: lets binaries and not-yet-migrated callers do `?`
/// on our `Result` in functions returning `eyre::Result`.
impl From<Error> for eyre::Report {
    fn from(err: Error) -> Self {
        err.into_report()
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Assign an [`ErrorKind`] to an `eyre::Result`, converting it into our
/// typed [`Result`].
///
/// ```ignore
/// client.get(key).await
///     .wrap_err("s3 get failed")     // still eyre::Result
///     .kind(ErrorKind::Network)?;    // now our Result, kind assigned
/// ```
pub trait ResultExt<T> {
    fn kind(self, kind: ErrorKind) -> Result<T>;
}

impl<T> ResultExt<T> for std::result::Result<T, eyre::Report> {
    fn kind(self, kind: ErrorKind) -> Result<T> {
        self.map_err(|report| Error { kind, report })
    }
}

/// Add context to our typed [`Result`] while preserving the [`ErrorKind`].
///
/// This mirrors eyre's `Context` trait but operates on our `Result<T, Error>`
/// instead of `Result<T, eyre::Report>`. Method dispatch is unambiguous
/// because our `Error` does not implement `std::error::Error`.
///
/// ```ignore
/// let block = parse_block(&data)
///     .wrap_err("in get_block")?;  // kind stays whatever it was
/// ```
pub trait WrapErr<T> {
    fn wrap_err<M: fmt::Display + Send + Sync + 'static>(self, msg: M) -> Result<T>;
    fn wrap_err_with<M: fmt::Display + Send + Sync + 'static, F: FnOnce() -> M>(
        self,
        f: F,
    ) -> Result<T>;
}

impl<T> WrapErr<T> for Result<T> {
    fn wrap_err<M: fmt::Display + Send + Sync + 'static>(self, msg: M) -> Result<T> {
        self.map_err(|e| Error {
            kind: e.kind,
            report: e.report.wrap_err(msg),
        })
    }

    fn wrap_err_with<M: fmt::Display + Send + Sync + 'static, F: FnOnce() -> M>(
        self,
        f: F,
    ) -> Result<T> {
        self.map_err(|e| Error {
            kind: e.kind,
            report: e.report.wrap_err(f()),
        })
    }
}

/// Convert an `Option<T>` into our typed [`Result<T>`] with a specific
/// [`ErrorKind`] and message.
///
/// ```ignore
/// let block = blocks.get(&hash)
///     .ok_or_kind(ErrorKind::NotFound, "block not found")?;
/// ```
pub trait OptionExt<T> {
    fn ok_or_kind(
        self,
        kind: ErrorKind,
        msg: impl fmt::Display + Send + Sync + 'static,
    ) -> Result<T>;
}

impl<T> OptionExt<T> for Option<T> {
    fn ok_or_kind(
        self,
        kind: ErrorKind,
        msg: impl fmt::Display + Send + Sync + 'static,
    ) -> Result<T> {
        self.ok_or_else(|| Error::new(kind, eyre::eyre!("{msg}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_eyre_report_defaults_to_internal() {
        let report = eyre::eyre!("something broke");
        let err = Error::from(report);
        assert_eq!(err.kind(), ErrorKind::Internal);
        assert!(err.to_string().contains("something broke"));
    }

    #[test]
    fn result_ext_assigns_kind() {
        let eyre_result: std::result::Result<(), eyre::Report> =
            Err(eyre::eyre!("connection refused"));
        let our_result = eyre_result.kind(ErrorKind::Network);
        let err = our_result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Network);
        assert!(err.to_string().contains("connection refused"));
    }

    #[test]
    fn result_ext_preserves_ok() {
        let eyre_result: std::result::Result<u32, eyre::Report> = Ok(42);
        let our_result = eyre_result.kind(ErrorKind::Network);
        assert_eq!(our_result.unwrap(), 42);
    }

    #[test]
    fn wrap_err_preserves_kind() {
        let result: Result<()> = Err(Error::new(ErrorKind::Network, eyre::eyre!("timeout")));
        let wrapped = result.wrap_err("while fetching block");
        let err = wrapped.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Network);
        assert!(err.to_string().contains("while fetching block"));
    }

    #[test]
    fn wrap_err_with_preserves_kind() {
        let result: Result<()> = Err(Error::new(ErrorKind::Parse, eyre::eyre!("bad rlp")));
        let wrapped = result.wrap_err_with(|| format!("decoding block {}", 42));
        let err = wrapped.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Parse);
        assert!(err.to_string().contains("decoding block 42"));
    }

    #[test]
    fn kind_survives_multiple_wraps() {
        let result: Result<()> = Err(Error::new(ErrorKind::NotFound, eyre::eyre!("key missing")));
        let wrapped = result
            .wrap_err("in get_block")
            .wrap_err("in handle_request");
        let err = wrapped.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn debug_includes_kind() {
        let err = Error::new(ErrorKind::Storage, eyre::eyre!("disk full"));
        let debug = format!("{:?}", err);
        assert!(debug.contains("[storage]"));
        assert!(debug.contains("disk full"));
    }

    #[test]
    fn question_mark_converts_eyre_report() {
        fn inner() -> Result<()> {
            let _: () = Err(eyre::eyre!("oops"))?;
            Ok(())
        }
        let err = inner().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Internal);
    }

    #[test]
    fn std_error_via_eyre_defaults_to_internal() {
        use eyre::Context;
        fn inner() -> Result<()> {
            let _: Vec<u8> =
                std::fs::read("/nonexistent/path/that/does/not/exist").wrap_err("read failed")?;
            Ok(())
        }
        let err = inner().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Internal);
    }

    #[test]
    fn error_converts_to_eyre_report() {
        let err = Error::new(ErrorKind::Network, eyre::eyre!("timeout"));
        let report: eyre::Report = err.into();
        assert!(report.to_string().contains("timeout"));
    }

    #[test]
    fn question_mark_in_eyre_fn_from_our_error() {
        fn lib_fn() -> Result<()> {
            Err(Error::new(ErrorKind::NotFound, eyre::eyre!("missing")))
        }
        fn bin_fn() -> std::result::Result<(), eyre::Report> {
            lib_fn()?;
            Ok(())
        }
        let err = bin_fn().unwrap_err();
        assert!(err.to_string().contains("missing"));
    }

    #[test]
    fn option_ext_ok_or_kind_some() {
        let opt: Option<u32> = Some(42);
        let result = opt.ok_or_kind(ErrorKind::NotFound, "not found");
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn option_ext_ok_or_kind_none() {
        let opt: Option<u32> = None;
        let result = opt.ok_or_kind(ErrorKind::NotFound, "block not found");
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert!(err.to_string().contains("block not found"));
    }
}

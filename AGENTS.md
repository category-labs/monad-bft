# AGENTS.md for /home/dd/work/monad-bft

**Tests**
Use `cargo nextest` for running tests.

```bash
# Example: run tests for a crate
cargo nextest run -p monad-wireauth
```

**Formatting**
Use nightly rustfmt with the pinned toolchain.

```bash
# Check formatting
cargo +nightly-2025-12-09 fmt --all --check

# Apply formatting
cargo +nightly-2025-12-09 fmt --all
```

**Clippy**
Run clippy with the project’s standard lint flags.

```bash
cargo clippy --all-targets --all-features -- \
  -D clippy::suspicious \
  -D clippy::style \
  -D clippy::clone_on_copy \
  -D clippy::redundant_clone \
  -D clippy::iter_kv_map \
  -D clippy::iter_nth \
  -D clippy::unnecessary_cast \
  -D clippy::filter_next \
  -D clippy::needless_lifetimes \
  -D clippy::useless_conversion \
  -D clippy::useless_vec \
  -D clippy::needless_question_mark \
  -D clippy::bool_comparison \
  -D unused_imports \
  -D unused_parens \
  -D deprecated \
  -A clippy::type_complexity \
  -A clippy::int_plus_one \
  -A clippy::uninlined-format-args \
  -A clippy::enum-variant-names \
  -A clippy::mutable_key_type \
  -A clippy::large_enum_variant \
  -A clippy::doc-overindented-list-items
```

```bash
# Example: run clippy for a crate
cargo clippy -p monad-wireauth --all-targets --all-features -- \
  -D clippy::suspicious \
  -D clippy::style \
  -D clippy::clone_on_copy \
  -D clippy::redundant_clone \
  -D clippy::iter_kv_map \
  -D clippy::iter_nth \
  -D clippy::unnecessary_cast \
  -D clippy::filter_next \
  -D clippy::needless_lifetimes \
  -D clippy::useless_conversion \
  -D clippy::useless_vec \
  -D clippy::needless_question_mark \
  -D clippy::bool_comparison \
  -D unused_imports \
  -D unused_parens \
  -D deprecated \
  -A clippy::type_complexity \
  -A clippy::int_plus_one \
  -A clippy::uninlined-format-args \
  -A clippy::enum-variant-names \
  -A clippy::mutable_key_type \
  -A clippy::large_enum_variant \
  -A clippy::doc-overindented-list-items
```

**Commits**
Use title-only commit messages formatted as `crate-name: short summary` (for example, `monad-leanudp: tighten fragment validation`). Only add a commit body if the user explicitly asks for one.

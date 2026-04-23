# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Sequence` trait on `Borrowed` views for fast sequential iteration; `Repeats` and `Lookbacks` skip per-element `rank()` via incremental counters and cached bitvector words, yielding ~11x faster iteration. Composition through tuples and `#[derive(Columnar)]` structs propagates the fast path automatically. Iterators consume the `Copy` borrowed view by value so the iterator lifetime is the borrowed data's inner lifetime, not any outer shell — `container.borrow().seq_iter()` works without temp-borrow dangling.

### Removed

- `Index::Cursor<'a>` GAT, `Index::cursor`, `Index::index_iter`, `Index::into_index_iter`, `DefaultCursor`, `impl_default_cursor!`, `CursorOf<'a, C>`. Replaced by `Sequence` on the `Borrowed` side. Callers that consumed a `&Container` iterator can construct `common::IterOwn::new(0, &container)` directly.

## [0.12.1](https://github.com/frankmcsherry/columnar/compare/columnar-v0.12.0...columnar-v0.12.1) - 2026-03-29

### Other

- Correct Stash::length_in_bytes

## [0.12.0](https://github.com/frankmcsherry/columnar/compare/columnar-v0.11.1...columnar-v0.12.0) - 2026-03-25

### Added

- Structured decoding via `DecodedStore`, a zero-allocation random-access view into indexed-encoded data, with constant-instruction-count field access regardless of tuple width ([#78](https://github.com/frankmcsherry/columnar/pull/78), [#79](https://github.com/frankmcsherry/columnar/pull/79))
- `FromBytes::validate` implementations for `Fixeds`, `Strides`, `Empties`, `Bools`, and `Discriminant`; `Stash::try_from_bytes` as the validated constructor ([#85](https://github.com/frankmcsherry/columnar/pull/85))
- `Discriminant::is_heterogenous()` and homogeneous enum optimization: enum containers now skip per-element discriminant and offset metadata when all elements share the same variant
- `no_std` support (with `alloc`) ([#94](https://github.com/frankmcsherry/columnar/pull/94))
- `BorrowedOf` type alias, mirroring the existing `ContainerOf` ([#81](https://github.com/frankmcsherry/columnar/pull/81))
- `Strides::pop` for unsealing lists at merge boundaries ([#98](https://github.com/frankmcsherry/columnar/pull/98))
- `Strings::get_str()` convenience method for when you want `&str` with explicit validation ([#88](https://github.com/frankmcsherry/columnar/pull/88))
- Trait implementations for `Repeats` and `Lookback` types ([#91](https://github.com/frankmcsherry/columnar/pull/91))
- Additional `Bytes`/`Stash` properties and methods ([#97](https://github.com/frankmcsherry/columnar/pull/97))

### Changed

- `Strings::Ref` changed from `&str` to `&[u8]` — UTF-8 validation at index time caused up to 17x slowdown and blocked compiler optimizations ([#86](https://github.com/frankmcsherry/columnar/pull/86))
- `element_sizes` now returns `Result`, defaulting to `Err` for unimplemented types — previously missing implementations silently accepted any byte length ([#80](https://github.com/frankmcsherry/columnar/pull/80))
- Decoding pipeline preserves `u64` alignment throughout, making field casts infallible and enabling dead-code elimination of unused fields ([#78](https://github.com/frankmcsherry/columnar/pull/78))
- Refreshed `Trees` container and its JSON example to current conventions ([#92](https://github.com/frankmcsherry/columnar/pull/92), [#93](https://github.com/frankmcsherry/columnar/pull/93))
- Removed vestigial `const N: usize` generic from `Repeats` ([#91](https://github.com/frankmcsherry/columnar/pull/91))

### Removed

- `HeapSize` trait, replaced by `AsBytes` which exposes actual byte slices rather than capacity metrics ([#87](https://github.com/frankmcsherry/columnar/pull/87))
- `EncodeDecode` trait and `Sequence` encoding format, superseded by the `indexed` module ([#78](https://github.com/frankmcsherry/columnar/pull/78))
- `from_u64s`, `decode_u64s`, and `from_byte_slices` methods, replaced by `DecodedStore`/`from_store` ([#79](https://github.com/frankmcsherry/columnar/pull/79))
- `inspect` module

## [0.11.1](https://github.com/frankmcsherry/columnar/compare/columnar-v0.11.0...columnar-v0.11.1) - 2026-01-17

### Other

- Re-add broken roaring, to avoid semver break
- Split lib.rs into many files
- Remove bytes.rs
- Remove rmp-serde as problematic
- Introduce Stash container
- Bump actions/checkout from 5 to 6

## [0.11.0](https://github.com/frankmcsherry/columnar/compare/columnar-v0.10.2...columnar-v0.11.0) - 2025-09-29

### Other

- Merge pull request #62 from frankmcsherry/dependabot/github_actions/actions/checkout-5
- Update src/lib.rs
- Introduce Borrow trait

## [0.10.2](https://github.com/frankmcsherry/columnar/compare/columnar-v0.10.1...columnar-v0.10.2) - 2025-09-16

### Other

- Rust 1.79 compat
- pub mod
- Just a single newtype
- Support boxed types

## [0.10.1](https://github.com/frankmcsherry/columnar/compare/columnar-v0.10.0...columnar-v0.10.1) - 2025-08-15

### Other

- Merge pull request #65 from antiguru/support_char
- Update lib.rs
- Support chars
- Fix name collisions and unprefixed columnar mentions
- Encode u128/u128 as [u8; 16]
- Update to Rust 1.89

## [0.10.0](https://github.com/frankmcsherry/columnar/compare/columnar-v0.9.0...columnar-v0.10.0) - 2025-08-05

### Other

- Sync columnar and columnar_derive versions
- Add Container::reserve_for

## [0.8.0](https://github.com/frankmcsherry/columnar/compare/columnar-v0.7.0...columnar-v0.8.0) - 2025-07-05

### Other

- Specialize Vecs::Push<Slice>
- Remove Columnar::Ref and replace by type defs
- Optimize non-shifting case
- Tidy extend_from_self
- Update _extend benchmark
- Add Container::extend_from_self

## [0.7.0](https://github.com/frankmcsherry/columnar/compare/columnar-v0.6.0...columnar-v0.7.0) - 2025-07-05

### Other

- Update MSRV to 1.79
- Add Container::reborrow_ref
- Introduce PushIndexAs trait
- Minimize as Container use
- Remove type argument from Container
- Introduce Container::Ref type
- Migrate Push<Ref> constraint
- Migrate Clear constraint
- Migrate Len constraint
- Migrate Default constraint
- Migrate Clone constraint
- Migrate Send constraint
- Clippy clean-up
- Revert D: Display Push due to perf, but with note
- Unify Strings::push implementations
- Update ops benchmark

## [0.6.0](https://github.com/frankmcsherry/columnar/compare/columnar-v0.5.1...columnar-v0.6.0) - 2025-06-20

### Other

- Polishing
- Reborrow for containers and references

## [0.5.1](https://github.com/frankmcsherry/columnar/compare/columnar-v0.5.0...columnar-v0.5.1) - 2025-06-13

### Other

- Make Ref Copy, add as_slice
- Optimization to help Rust do the right thing
- Add iterator slice hint and implement ExactSizeIterator
- Support for smallvec

## [0.5.0](https://github.com/frankmcsherry/columnar/compare/columnar-v0.4.1...columnar-v0.5.0) - 2025-05-09

### Other

- Reorganize demonstration as examples with dev dependencies

## [0.4.1](https://github.com/frankmcsherry/columnar/compare/columnar-v0.4.0...columnar-v0.4.1) - 2025-03-24

### Other

- Rust 1.78 tested

## [0.4.0](https://github.com/frankmcsherry/columnar/compare/columnar-v0.3.0...columnar-v0.4.0) - 2025-03-24

### Other

- Documentation, better formatting
- Custom chains to force inlining
- Extend Container trait bounds
- Merge pull request #29 from frankmcsherry/iterators_formatters
- Respond to PR feedback
- Add support for iterators and formatters

## [0.3.0](https://github.com/frankmcsherry/columnar/compare/columnar-v0.2.2...columnar-v0.3.0) - 2025-02-09

### Other

- Improve byte slice encoding
- Update benches, rework length names for clarity
- Allow for general byte slice serialization
- Update benchmarks
- RustNYC presentation
- Correct alignment requirements

## [0.2.2](https://github.com/frankmcsherry/columnar/compare/columnar-v0.2.1...columnar-v0.2.2) - 2025-01-15

### Other

- Merge pull request [#22](https://github.com/frankmcsherry/columnar/pull/22) from antiguru/inline_as_bytes
- Revert a change to into_iter

## [0.2.1](https://github.com/frankmcsherry/columnar/compare/columnar-v0.2.0...columnar-v0.2.1) - 2025-01-15

### Other

- Merge pull request [#19](https://github.com/frankmcsherry/columnar/pull/19) from frankmcsherry/alignment_err_bench_tidy
- Update benchmarks
- Tidy up alignment and error messages
- Correct encode logic
- Back out support for Box<[T]> and Rc<[T]>
- Support additional source types
- Update Cargo.tomls

## [0.1.1](https://github.com/frankmcsherry/columnar/compare/v0.1.0...v0.1.1) - 2024-11-30

### Other

- Added missing implementations and support methods

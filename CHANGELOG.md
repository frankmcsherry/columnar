# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A `Bounds` trait for the bounds of `Vecs` and `Strings`, unifying the read side of list extents: `bounds(index)` reports each list's extent ("select"), `rank(offset)` reports the list containing a value position, `extent(range)` the positions spanned by a range of lists, `extents(ranges)` its bulk in-place form over ascending disjoint ranges (one cursor pass on the bitvec containers), and `total()` the summed lengths. Implemented by `Uppers` (the new default bounds container), `Strides`, `Fixeds`, and (read-only) `Vec<u64>`/`[u64]` for raw cumulative-offset arrays
- A `BoundsContainer` trait (explicit opt-in) marking bounds containers writable by `Vecs` and `Strings`, with provided `seal(upper)` (conclude the current list at an absolute position; `Uppers` records it directly) and `extend_with_extent(other, range, extent)` (extend by a range whose extent the caller already computed, sparing bit-packed implementors a re-derivation costing a select or two per call; `Vecs`/`Strings` pass down the extent they compute for the values copy); and a `BoundsBorrow` composite for types whose borrowed form answers `Bounds` queries
- Two `RankSelect`-backed bounds containers: `NeverEmpty` for non-empty lists (one bit per value, set at each list's last value) and `MaybeEmpty` for possibly-empty lists (unary: a zero per value, then a one). Their `rank` is a bitvector rank rather than a search, and their `extend_from_self` splices bit ranges word-at-a-time and catches up the count summaries, rather than re-encoding via integers
- `RankSelect::select_zero`, the position of the k-th unset bit, mirroring `select` via complemented chunk counts; `RankSelect::select_from`, the next set bit at or after a position with a known rank (a word scan with a `select` fallback, so `bounds(i)` costs one select plus typically one word read); `RankSelect::push_bits` and `RankSelect::extend_from_bits` for bulk bit appends that maintain the count summaries; and `Bools::read_bits`, `Bools::push_bits`, and `Bools::extend_from_bits` word-level splicing primitives beneath them
- `BoundsCursor` (via `NeverEmpty::cursor()` / `MaybeEmpty::cursor()`): a bidirectional `seek(index)` over list extents that remembers where the last query ended, so increasing queries cost in proportion to the distance stepped — sub-nanosecond when consecutive, crossing over from-scratch `bounds(index)` only past jumps of a few hundred lists. `RankSelectCursor::seek_to_rank` gained a middle path (bounded word scan to the chunk boundary before the chunk binary search), and the cursor's hot methods are now `#[inline(always)]`, which benchmarks showed register-residency of cursor state depends on. A `benches/bounds.rs` suite and `examples/seek_probe.rs` density-sweep accompany these

### Changed

- Bounds containers now present list *lengths* as their container contract (`Index`, `Push`, `extend_from_self`); cumulative storage is an implementation detail. Consequently `extend_from_self` on a bounds container rebases by construction, and `Vecs`/`Strings` no longer perform offset arithmetic when extending — each bounds implementor can specialize (e.g. `Uppers` memcpys offsets when aligned)
- The default bounds parameter of `Vecs` and `Strings` is `Uppers` rather than `Vec<u64>`. `Uppers` wraps a `Vec<u64>` of cumulative upper bounds and shares its serialized layout, so persisted data is unaffected. Breaking for code that named `Vecs<TC, Vec<u64>>` explicitly or pushed absolute offsets into `.bounds`: reads of raw `Vec<u64>` bounds still compile, writes must migrate to `Uppers`
- `Strides` and `Fixeds` `Index`/`Push` now traffic in list lengths rather than absolute upper bounds, per the `Bounds` contract; `Strides::rank` divides while strided, and the `Deref`-based `Strides::bounds` helper is replaced by the `Bounds` implementation
- `Strides<BC>` is now generic over its spill: any `BoundsContainer` works (default `Uppers`, but e.g. `Strides<MaybeEmpty>` composes the stride fast path with bit-packed overflow). The spill stores the post-stride lists' bounds *relative to the end of the strided prefix* rather than absolute offsets — a serialized-format change for spilled `Strides` data — and `Strides::extend_from_self` bulk-extends conforming strided heads and delegates spill-to-spill, so the spill's own bulk extension (memcpy, bit splice) is used

### Removed

- `Vecs::push_iter`, redundant with the `Push<I: IntoIterator>` implementation: `vecs.push(iter)` does the same

## [0.13.0](https://github.com/frankmcsherry/columnar/compare/columnar-v0.12.1...columnar-v0.13.0) - 2026-05-23

### Added

- `RankSelect` forward cursor (`RankSelectCursor`, obtained via `RankSelect::cursor()`): in-order traversal that caches the current word and running rank, so a single word load serves many operations instead of re-probing `counts`. Methods are `next_one` (next set-bit position), `step` (bit-by-bit), `seek_to_pos`, and `seek_to_rank` ([#109](https://github.com/frankmcsherry/columnar/pull/109))

### Changed

- `AsBytes` is now random-access: implementors provide `const SLICE_COUNT` and `get_byte_slice(index)`, and `as_bytes()` becomes a provided method iterating `0..SLICE_COUNT`. Encoders index slices by position rather than consuming an iterator, letting the dispatch constant-fold. Breaking for external implementors of `AsBytes` ([#104](https://github.com/frankmcsherry/columnar/pull/104))

### Removed

- Public `chain` and `chain_one` helpers and the `chain_mod` module, no longer needed now that `AsBytes` does not build chained iterators ([#104](https://github.com/frankmcsherry/columnar/pull/104))

### Fixed

- `RankSelect::select` returned incorrect positions, from an off-by-one in the per-word scan and a chunk-count miscalculation; `select(0)` now yields the first set bit and `select(rank(p)) == p` holds for set-bit positions. Reimplemented with a binary search over `counts` and a branch-free in-word select ([#109](https://github.com/frankmcsherry/columnar/pull/109))

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

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

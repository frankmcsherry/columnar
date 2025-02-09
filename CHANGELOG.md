# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

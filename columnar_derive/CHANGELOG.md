# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.13.0](https://github.com/frankmcsherry/columnar/compare/columnar_derive-v0.12.1...columnar_derive-v0.13.0) - 2026-03-30

### Other

- Use indexed access to byte slices in `AsBytes` ([#104](https://github.com/frankmcsherry/columnar/pull/104))

## [0.12.0](https://github.com/frankmcsherry/columnar/compare/columnar_derive-v0.11.1...columnar_derive-v0.12.0) - 2026-03-25

### Added

- Derive support for `from_store` (structured decoding) ([#79](https://github.com/frankmcsherry/columnar/pull/79))
- Derive support for `extend_for_self` on enum types ([#82](https://github.com/frankmcsherry/columnar/pull/82))

### Changed

- Generated code uses fully scoped paths (`::core::result::Result`, etc.) to avoid naming conflicts and support `no_std` ([#96](https://github.com/frankmcsherry/columnar/pull/96))
- Generated `FromBytes` implementations use compile-time-constant byte slice counts ([#78](https://github.com/frankmcsherry/columnar/pull/78))
- Consolidated scalar fields and streamlined naming in generated code ([#88](https://github.com/frankmcsherry/columnar/pull/88))

## [0.11.0](https://github.com/frankmcsherry/columnar/compare/columnar_derive-v0.10.2...columnar_derive-v0.11.0) - 2025-09-29

### Other

- Introduce Borrow trait

## [0.10.1](https://github.com/frankmcsherry/columnar/compare/columnar_derive-v0.10.0...columnar_derive-v0.10.1) - 2025-08-15

### Other

- Export bytemuck crate
- Fix name collisions and unprefixed columnar mentions

## [0.10.0](https://github.com/frankmcsherry/columnar/compare/columnar_derive-v0.3.0...columnar_derive-v0.10.0) - 2025-08-05

### Other

- Add Container::reserve_for

## [0.2.7](https://github.com/frankmcsherry/columnar/compare/columnar_derive-v0.2.6...columnar_derive-v0.2.7) - 2025-07-05

### Other

- Remove Columnar::Ref and replace by type defs
- Add Container::extend_from_self

## [0.2.6](https://github.com/frankmcsherry/columnar/compare/columnar_derive-v0.2.5...columnar_derive-v0.2.6) - 2025-07-05

### Other

- Add Container::reborrow_ref
- Introduce PushIndexAs trait
- Remove type argument from Container
- Introduce Container::Ref type
- Migrate Push<Ref> constraint

## [0.2.5](https://github.com/frankmcsherry/columnar/compare/columnar_derive-v0.2.4...columnar_derive-v0.2.5) - 2025-06-20

### Other

- Polishing
- Reborrow for containers and references

## [0.2.4](https://github.com/frankmcsherry/columnar/compare/columnar_derive-v0.2.3...columnar_derive-v0.2.4) - 2025-06-13

### Other

- Optimization to help Rust do the right thing

## [0.2.3](https://github.com/frankmcsherry/columnar/compare/columnar_derive-v0.2.2...columnar_derive-v0.2.3) - 2025-03-24

### Other

- Custom chains to force inlining

## [0.2.2](https://github.com/frankmcsherry/columnar/compare/columnar_derive-v0.2.1...columnar_derive-v0.2.2) - 2025-01-15

### Other

- Always inline as_bytes

## [0.2.1](https://github.com/frankmcsherry/columnar/compare/columnar_derive-v0.2.0...columnar_derive-v0.2.1) - 2025-01-15

### Other

- Improve implementation, support enums.
- Add ability to specify attributes on reference types
- Support additional source types

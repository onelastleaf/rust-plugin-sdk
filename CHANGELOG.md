# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-08-22

### Added

- Initial crates.io release of the onelastleaf Rust plugin SDK.
- Asynchronous action registration with bounded concurrent job execution and
  cooperative cancellation.
- Host-managed configuration and document calls, structured logging, and
  verified streaming artifact transfers.
- Ordered, backpressured gRPC sessions with heartbeat, graceful shutdown, and
  parent-process liveness handling.
- Generated protobuf types using the canonical wire-compatible plugin protocol.
- Unit, cross-platform CI, and shared black-box conformance coverage.

[Unreleased]: https://github.com/onelastleaf/rust-plugin-sdk/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/onelastleaf/rust-plugin-sdk/releases/tag/v0.1.0

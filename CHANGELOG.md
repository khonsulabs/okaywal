# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.3.0

### Added

- `WriteAheadLog::checkpoint_active()` is a new function that checkpoints the
  currently stored data, regardless of whether the configured thresholds are
  met. This function returns after the active file has been rotated and the
  checkpointing thread has been notified of the file to checkpoint. Thanks to
  @blakesmith for implementing this in [#11][11]

[11]: https://github.com/khonsulabs/okaywal/pull/11

## v0.2.0

### Breaking Changes

- `LogManager::checkpoint_to` now has an additional parameter, `wal:
  &WriteAheadLog`. This is provided for convenience because it may be necessary
  to randomly access information in the WAL while performing a checkpointing
  operation.

### Added

- `LogPosition::serialize_to`/`LogPosition::deserialize_from` provide methods
  for reading and writing a `LogPosition` from an arbitrary `Write`/`Read`
  implementor (respectively). This uses a fixed-length serialization with a
  length of `LogPosition::SERIALIZED_LENGTH` -- 16 bytes.

### Changed

- `WriteAheadLog::shutdown()` now no longer requires all instances of
  `WriteAheadLog` to be dropped to succeed.

### Fixed

- When the WAL recycles a segment file, the `LogPosition` returned is now
  correct. Previously, returned `LogPosition`s would contain the segment file's
  old id, causing those positions to be unreadable.
- When reading from a `LogPosition`, if the data has not been flushed or
  synchronized to disk yet, the read will be blocked until the sync operation
  finishes.

## v0.1.0

- Initial public preview release. No stability guarantees are being made at this
  stage. Feedback is welcome.

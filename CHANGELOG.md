# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.16.0] - 2022-09-22
### Removed
- Support for Python 2.x

### Added
- Support for Python 3.x

## [1.15.1] - 2021-07-01
### Added
- Added `pre-commit` hooks.

### Changed
- Official code style is now Black.

### Fixed
- CLI now actually caches directory entries for quicker tab completion.

## [1.15.0] - 2021-06-17
### Added
- Added `IN2P3-CC-XRD-disk` and `IN2P3-CC-XRD-tape`.
- `check` will now check whether a file has no replicas at all.

## [1.14.0] - 2021-05-17
### Changed
- `UKI-SOUTHGRID-OX-HEP-disk` is marked as broken, since the SE will be switched off

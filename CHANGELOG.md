# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- Basepath string for IN2P3-CC-XRD-tape and IN2P3-CC-XRD-disk is corrected (// is replaced with // for an exact string match with the host name string).
- BackendException for cases where the get_SE method returns None because the host name string is not contained inside the file URL are added.

## [1.17.5] - 2022-02-26
### Changed
- Updated pythonpublish.yml to fix the API token error that kept popping up when attempting to upload releases from GitHub to PyPi.

## [1.17.4] - 2024-02-21
### Changed
- Updated pythonpublish.yml which is used for the upload Python package workflow run.

## [1.17.3] - 2024-02-21
### Changed
- Updated the RAL-LCG2-T2K-tape, IN2P3-CC-XRD-disk and IN2P3-CC-XRD-tape SE properties, and replace the deprecated gfal-legacy-bringonline command that was used for tape SEs.

## [1.17.2] - 2022-11-09
### Changed
- Fixed a Python 3 specific bug for T2K Data Manager Maid.

## [1.17.1] - 2022-11-06
### Changed
- Updated the RAL-LCG2-T2K-tape SE properties, and added instructions on how to obtain a Grid certificate in README.

## [1.17.0] - 2022-09-22
### Changed
- Storage at TRIUMF is now marked as broken, since we no longer use it.

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

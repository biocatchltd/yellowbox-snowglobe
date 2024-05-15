# Yellowbox Snowglobe Changelog
## 0.2.5
### Added
* official support for python 3.12
### Removed
* dropped support for python 3.7
### Internal
* linting changed to ruff
* github workflows updated actions to avoid node deprecation warning
* added CI
## 0.2.4
### Added
* added support for json queries
## 0.2.3
### Fixed
* bug for empty queries when using async execute
## 0.2.2
### Fixed
* SnowType converter now supports None as well
## 0.2.1
### Fixed
* a bug with concurrent sessions
## 0.2.0
### Changed
* the service now contains a postgresql service rather than inheriting from it.
* the service now dynamically renames result columns based on the column mode given at construction.
* Add support for sampled queries
## 0.1.1
### Fixed
* Field names in api response are in uppercase.
### Internal
* Added support to python datetime conversion.
## 0.1.0
* initial

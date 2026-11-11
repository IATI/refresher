# Changelog

<!-- markdownlint-disable MD024 -->

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and for code components this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The subheadings `CHANGELOG`s can use are: Added, Changed, Deprecated, Fixed, Removed, Security.

## 2026-11-11

### Changed

- Updated `certifi` to 2026.10.23 to distrust the compromised eMudhra certificates.
- Updated `lxml` to 6.1.3.
- Made calls to create XML parses explicitly set `resolve_entities="internal"`.

### Fixed

- Insecure usage of `lxml` 4.9.3

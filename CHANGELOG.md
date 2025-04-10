# Changelog

All notable changes to the Fluxus project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2025-04-10

### Added
- Added new MultiError and MapItemError type for better error handling
- Added new Map stage for processing items in parallel 
- Added more examples to the documentation


### Changed
- N/A (initial release)

### Removed
- N/A (initial release)

[1.1.0]: https://github.com/synoptiq/go-fluxus/releases/tag/v1.1.0

## [1.0.0] - 2025-04-10

### Added
- Initial release of the Fluxus pipeline orchestration library
- Core pipeline components: `Stage`, `Chain`, `Pipeline`
- Fan-out/fan-in patterns for parallel processing
- Retry mechanism with backoff strategies
- Buffer for batch processing
- Timeout handling for stages
- Comprehensive test suite
- Benchmark suite for performance analysis
- Structured error types for better error handling
- Metrics collection interface for monitoring pipeline performance
- Prometheus metrics implementation
- Circuit breaker pattern for increased resilience
- Rate limiter functionality for controlled resource usage
- Fuzzing tests to catch edge cases
- ETL pipeline example showcasing library features
- GitHub Actions CI/CD workflow
- Makefile with common development tasks
- Documentation improvements

### Changed
- N/A (initial release)

### Removed
- N/A (initial release)

[1.0.0]: https://github.com/synoptiq/go-fluxus/releases/tag/v1.0.0
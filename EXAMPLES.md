# Fluxus Examples Documentation

This document provides an overview of the examples included in the go-fluxus library, demonstrating various pipeline patterns and functionalities.

## Buffer (buffer.go)

Demonstrates batch processing using buffers, which allow processing data in efficient batches instead of one item at a time:

- **Basic Buffer Demo**: Shows how to process records in batches with a specified batch size
- **Variable Batch Size Demo**: Tests different batch sizes to compare performance
- **Pooled Buffer Demo**: Shows how to use pooled buffers for better performance
- **Error Handling Demo**: Demonstrates error handling strategies for buffer processing

## Circuit Breaker (circuit.go)

Implements the circuit breaker pattern to prevent cascading failures when calling unreliable services:

- Simulates a service with configurable failure rate
- Shows how the circuit opens after consecutive failures
- Demonstrates half-open state and test requests
- Provides a simple HTTP server to try the circuit breaker interactively

## Dependency Injection (db.go, db_test.go)

Shows how to structure applications with dependency injection patterns:

- Defines a clean interface for data access (UserRepository)
- Implements concrete repository with SQLite and a mock for testing
- Shows how to use the Map stage to process multiple items concurrently
- Includes comprehensive unit tests for both stages and full pipelines

## ETL Pipeline (etl.go)

Demonstrates Extract-Transform-Load (ETL) pipeline for data processing:

- Reads CSV data, parses records, and transforms into a summary
- Shows rate limiting to control processing flow
- Implements error handling strategies for different failure cases
- Compares performance with and without rate limiting

## Fan Out (fanout.go)

Shows how to process a single input through multiple stages concurrently:

- Basic fan-out with multiple transformation functions
- Concurrency control to limit parallel execution
- Error handling in fan-out scenarios

## Metrics (metrics.go)

Demonstrates collecting and reporting metrics during pipeline processing:

- Implements a custom metrics collector for pipeline stages
- Tracks metrics for different pipeline operations (stages, fan-out, fan-in)
- Shows how to wrap stages with metrics collection
- Provides detailed reporting on execution statistics

## Pipeline Composition (pipeline.go)

Shows how to build pipelines by chaining multiple stages:

- Text analysis pipeline with multiple transformation stages
- Shows how to build different pipeline variations (simple vs. resilient)
- Demonstrates error handling, timeout, and retry mechanisms
- Processes sample texts to demonstrate the pipeline in action

## Object Pooling (pooling.go)

Demonstrates the use of object pools to reduce memory allocations and GC pressure:

- Shows processing with and without object pooling
- Compares performance metrics (allocation, GC pressure, duration)
- Demonstrates how to create and use pooled resources effectively
- Includes profiling setup for deeper performance analysis

## Rate Limiting (ratelimit.go)

Shows how to control the flow of requests to a service using rate limiting:

- Wraps service calls with configurable rate limits
- Demonstrates burst capacity handling
- Shows concurrent request handling with rate limiting
- Demonstrates dynamically changing rate limits during execution

## Retry Mechanism (retry.go)

Implements retry logic for handling transient failures:

- Distinguishes between permanent and transient errors
- Uses exponential backoff with jitter for retries
- Shows how service reliability improves over time with retries
- Provides detailed statistics on retry attempts and outcomes

## Stream Processing (stream.go)

Demonstrates building a stream processing pipeline for real-time data:

- Processes event streams with multiple stages
- Shows concurrent processing of stream elements
- Implements filtering, enrichment, and transformation of events
- Uses channels for asynchronous processing

## Timeout Handling (timeout.go)

Shows how to handle timeouts in service calls:

- Wraps service calls with configurable timeouts
- Compares different timeout settings and their impact
- Demonstrates timeout handling in chained pipelines
- Shows how timeouts prevent slow operations from blocking the pipeline

## Distributed Tracing (tracing.go)

Integrates OpenTelemetry tracing with pipeline stages:

- Sets up a tracer with OTLP exporter
- Wraps pipeline stages with tracing capabilities
- Shows span creation, attributes, and error recording
- Simulates API calls with detailed tracing information
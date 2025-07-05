# Configuration Documentation

**Fluxus supports YAML-based pipeline configuration, enabling you to define complex data processing pipelines declaratively without writing Go code.**

This feature allows you to:
- Define pipelines using YAML configuration files
- Register custom stage executors and functions
- Configure observability (metrics and tracing) at pipeline and stage levels
- Build both classic and streaming pipelines from configuration
- Validate configuration before runtime

## Table of Contents

- [Quick Start](#quick-start)
- [Pipeline Configuration](#pipeline-configuration)
- [Stage Types](#stage-types)
- [Observability Configuration](#observability-configuration)
- [Registry and Executors](#registry-and-executors)
- [Complete Examples](#complete-examples)
- [Best Practices](#best-practices)

## Quick Start

Here's a simple example to get you started:

**pipeline.yaml**
```yaml
version: "1.0.0"
pipeline_name: "simple_example"
pipeline_type: "classic"

stages:
  - name: "double_numbers"
    type: "custom"
    properties:
      factory: "doubler_factory"
```

**main.go**
```go
package main

import (
    "context"
    "github.com/synoptiq/go-fluxus"
)

func main() {
    // Load configuration from file
    config, err := fluxus.LoadPipelineConfigFromFile("pipeline.yaml")
    if err != nil {
        panic(err)
    }

    // Create registry and register custom executor
    registry := fluxus.DefaultRegistry()
    err = registry.RegisterExecutor("doubler_factory", func(config map[string]any) (interface{}, error) {
        return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (any, error) {
            if num, ok := input.(int); ok {
                return num * 2, nil
            }
            return input, nil
        }), nil
    })
    if err != nil {
        panic(err)
    }

    // Build pipeline from configuration
    pipelineInterface, err := fluxus.BuildPipelineFromConfig(config, registry)
    if err != nil {
        panic(err)
    }

    pipeline := pipelineInterface.(*fluxus.Pipeline[any, any])
    
    // Use the pipeline
    pipeline.Start(context.Background())
    result, err := pipeline.Process(context.Background(), 5)
    if err != nil {
        panic(err)
    }
    
    println(result.(int)) // Output: 10
}
```

## Pipeline Configuration

The top-level pipeline configuration defines the overall structure and settings.

### Required Fields

```yaml
version: "1.0.0"                    # Configuration version
pipeline_name: "my_pipeline"        # Unique pipeline identifier
pipeline_type: "classic"            # Pipeline type: "classic" or "streaming"
stages:                            # Array of stage configurations
  - name: "stage1"
    type: "map"
    # ... stage configuration
```

### Optional Fields

```yaml
# Metrics configuration
metrics:
  enabled: true                    # Enable/disable metrics collection
  type: "prometheus"               # Metrics backend: "prometheus", "mongodb", "influxdb", "noop"
  endpoint: "http://localhost:9090" # Metrics endpoint URL

# Tracing configuration
tracing:
  enabled: true                    # Enable/disable distributed tracing
  type: "zipkin"                   # Tracing backend: "zipkin", "jaeger", "otlp", "noop"
  endpoint: "http://localhost:9411/api/v2/spans" # Tracing endpoint URL

# Streaming pipeline configuration (only for pipeline_type: "streaming")
streaming:
  buffer_size: 1000               # Buffer size for streaming pipeline
  concurrency: 4                  # Number of concurrent workers
  logger: "custom_logger"         # Custom logger executor name

# Debug configuration
debug:
  enabled: false                  # Enable debug mode (logs configuration without execution)
```

### Pipeline Types

#### Classic Pipeline
- Processes single items through a sequence of stages
- Uses `fluxus.Pipeline[any, any]` type
- Best for traditional ETL workloads and simple processing chains

#### Streaming Pipeline
- Processes streams of data with backpressure support
- Uses `fluxus.StreamPipeline` type
- Best for high-throughput, real-time data processing

## Stage Types

Fluxus supports multiple built-in stage types, each with specific configuration properties.

### Map Stage

Applies a function to transform input data.

```yaml
- name: "transform_data"
  type: "map"
  metrics: true                    # Enable stage-level metrics
  tracing: false                   # Disable stage-level tracing
  properties:
    map_function: "my_transformer"  # Required: executor name
    concurrency: 4                 # Optional: parallel processing workers
    collect_errors: true           # Optional: collect errors instead of failing
    error_handler: "error_handler"  # Optional: custom error handler
```

### Buffer Stage

Batches items for efficient bulk processing.

```yaml
- name: "batch_processor"
  type: "buffer"
  properties:
    batch_size: 100                # Required: number of items per batch
    processor: "batch_handler"     # Required: batch processing function
    error_handler: "error_handler" # Optional: error handler
```

### Filter Stage

Filters items based on a predicate function.

```yaml
- name: "filter_valid"
  type: "filter"
  properties:
    filter_function: "is_valid"    # Required: predicate function
    error_handler: "error_handler" # Optional: error handler
```

### Fan-Out Stage

Distributes input to multiple parallel stages.

```yaml
- name: "parallel_processing"
  type: "fan_out"
  properties:
    concurrency: 8                 # Optional: parallel execution limit
    error_handler: "error_handler" # Optional: error handler
    stages:                        # Required: array of nested stages
      - name: "process_a"
        type: "custom"
        properties:
          factory: "processor_a"
      - name: "process_b"
        type: "custom"
        properties:
          factory: "processor_b"
```

### Fan-In Stage

Aggregates results from multiple inputs.

```yaml
- name: "combine_results"
  type: "fan_in"
  properties:
    aggregator: "combine_function" # Required: aggregation function
    error_handler: "error_handler" # Optional: error handler
```

### Router Stage

Routes items to different stages based on selection criteria.

```yaml
- name: "route_by_type"
  type: "router"
  properties:
    selector_function: "type_selector" # Required: route selection function
    concurrency: 4                     # Optional: parallel processing
    error_handler: "error_handler"     # Optional: error handler
    routes:                           # Required: named routes
      numbers:
        name: "process_numbers"
        type: "custom"
        properties:
          factory: "number_processor"
      strings:
        name: "process_strings"
        type: "custom"
        properties:
          factory: "string_processor"
```

### Map-Reduce Stage

Implements the map-reduce pattern for distributed processing.

```yaml
- name: "word_count"
  type: "map_reduce"
  properties:
    mapper_function: "word_mapper"     # Required: map function
    reducer_function: "word_reducer"   # Required: reduce function
    parallelism: 8                     # Optional: parallel processing level
```

### Join-By-Key Stage

Groups items by a key function.

```yaml
- name: "group_by_category"
  type: "join_by_key"
  properties:
    key_function: "category_key"      # Required: key extraction function
    error_handler: "error_handler"    # Optional: error handler
```

### Window Stages

#### Tumbling Count Window
```yaml
- name: "count_window"
  type: "tumbling_count_window"
  properties:
    size: 100                        # Required: window size in items
```

#### Tumbling Time Window
```yaml
- name: "time_window"
  type: "tumbling_time_window"
  properties:
    duration: 5000                   # Required: window duration in milliseconds
```

#### Sliding Count Window
```yaml
- name: "sliding_count"
  type: "sliding_count_window"
  properties:
    size: 100                        # Required: window size in items
    slide: 50                        # Required: slide interval in items
```

#### Sliding Time Window
```yaml
- name: "sliding_time"
  type: "sliding_time_window"
  properties:
    duration: 5000                   # Required: window duration in milliseconds
    slide: 1000                      # Required: slide interval in milliseconds
```

### Resilience Stages

#### Circuit Breaker
```yaml
- name: "protected_service"
  type: "circuit_breaker"
  properties:
    failure_threshold: 5             # Required: failures before opening
    reset_timeout: 30000             # Required: reset timeout in milliseconds
    success_threshold: 3             # Optional: successes to close circuit
    half_open_max: 10               # Optional: max requests in half-open state
    stage:                          # Required: wrapped stage
      name: "service_call"
      type: "custom"
      properties:
        factory: "service_factory"
```

#### Retry
```yaml
- name: "retry_operation"
  type: "retry"
  properties:
    attempts: 3                      # Required: maximum retry attempts
    stage:                          # Required: wrapped stage
      name: "unreliable_operation"
      type: "custom"
      properties:
        factory: "operation_factory"
    backoff: "exponential_backoff"   # Optional: backoff function
    should_retry: "retry_predicate"  # Optional: retry condition function
    error_handler: "error_handler"   # Optional: error handler
```

#### Timeout
```yaml
- name: "timeout_operation"
  type: "timeout"
  properties:
    timeout: 10000                   # Required: timeout in milliseconds
    stage:                          # Required: wrapped stage
      name: "slow_operation"
      type: "custom"
      properties:
        factory: "slow_factory"
    error_handler: "error_handler"   # Optional: error handler
```

#### Dead Letter Queue
```yaml
- name: "dlq_operation"
  type: "dead_letter_queue"
  properties:
    stage:                          # Required: wrapped stage
      name: "risky_operation"
      type: "custom"
      properties:
        factory: "risky_factory"
    handler: "dlq_handler"           # Required: dead letter handler
    should_dql_function: "dlq_predicate" # Optional: DLQ condition function
    error_logger: "error_logger"     # Optional: error logging function
```

### Custom Stage

For implementing custom logic that doesn't fit built-in stage types.

```yaml
- name: "custom_logic"
  type: "custom"
  properties:
    factory: "custom_stage_factory"  # Required: factory function name
    config:                         # Optional: custom configuration map
      threshold: 100
      mode: "advanced"
      settings:
        - option1
        - option2
```

## Observability Configuration

### Metrics Configuration

#### Prometheus
```yaml
metrics:
  enabled: true
  type: "prometheus"
  endpoint: "http://localhost:9090"
```

#### InfluxDB
```yaml
metrics:
  enabled: true
  type: "influxdb"
  endpoint: "http://localhost:8086"
```

#### MongoDB
```yaml
metrics:
  enabled: true
  type: "mongodb"
  endpoint: "mongodb://localhost:27017"
```

#### No-op (Disabled)
```yaml
metrics:
  enabled: false
  # or
  type: "noop"
```

### Tracing Configuration

#### Zipkin
```yaml
tracing:
  enabled: true
  type: "zipkin"
  endpoint: "http://localhost:9411/api/v2/spans"
```

#### OTLP (OpenTelemetry)
```yaml
tracing:
  enabled: true
  type: "otlp"
  endpoint: "localhost:4317"
```

#### Jaeger (via OTLP)
```yaml
tracing:
  enabled: true
  type: "jaeger"
  endpoint: "localhost:14268/api/traces"
```

### Stage-Level Observability

Enable metrics and tracing for individual stages:

```yaml
stages:
  - name: "monitored_stage"
    type: "map"
    metrics: true          # Enable metrics for this stage
    tracing: true          # Enable tracing for this stage
    properties:
      map_function: "my_function"
```

## Registry and Executors

The registry system allows you to register custom functions that can be referenced in YAML configuration.

### Types of Executors

1. **Stage Functions** - Transform data (`Stage[any, any]`)
2. **Predicate Functions** - Filter data (`PredicateFunc[any]`)
3. **Aggregator Functions** - Combine multiple inputs
4. **Custom Stage Factories** - Create complex custom stages
5. **Error Handlers** - Handle errors
6. **Utility Functions** - Backoff, logging, etc.

### Registration Examples

```go
registry := fluxus.DefaultRegistry()

// Register a simple stage function
err := registry.RegisterExecutor("doubler", fluxus.StageFunc[any, any](
    func(ctx context.Context, input any) (any, error) {
        if num, ok := input.(int); ok {
            return num * 2, nil
        }
        return input, nil
    },
))

// Register a predicate function
err = registry.RegisterExecutor("positive_only", fluxus.PredicateFunc[any](
    func(ctx context.Context, input any) (bool, error) {
        if num, ok := input.(int); ok {
            return num > 0, nil
        }
        return false, nil
    },
))

// Register an aggregator function
err = registry.RegisterExecutor("sum_aggregator", func(inputs []any) (any, error) {
    sum := 0
    for _, input := range inputs {
        if num, ok := input.(int); ok {
            sum += num
        }
    }
    return sum, nil
})

// Register a custom stage factory
err = registry.RegisterExecutor("advanced_processor", func(config map[string]any) (interface{}, error) {
    threshold, _ := config["threshold"].(int)
    return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (any, error) {
        if num, ok := input.(int); ok && num > threshold {
            return num * 2, nil
        }
        return input, nil
    }), nil
})

// Register an error handler
err = registry.RegisterExecutor("log_errors", func(err error) error {
    log.Printf("Pipeline error: %v", err)
    return err // or return nil to suppress the error
})
```

## Complete Examples

### ETL Pipeline Example

```yaml
version: "1.0.0"
pipeline_name: "etl_pipeline"
pipeline_type: "classic"

metrics:
  enabled: true
  type: "prometheus"
  endpoint: "http://localhost:9090"

tracing:
  enabled: true
  type: "zipkin"
  endpoint: "http://localhost:9411/api/v2/spans"

stages:
  # Extract stage
  - name: "extract_data"
    type: "custom"
    metrics: true
    properties:
      factory: "data_extractor"
      config:
        source: "database"
        query: "SELECT * FROM users"

  # Transform stage with validation
  - name: "validate_records"
    type: "filter"
    metrics: true
    properties:
      filter_function: "record_validator"

  # Transform stage with parallel processing
  - name: "transform_records"
    type: "map"
    metrics: true
    tracing: true
    properties:
      map_function: "record_transformer"
      concurrency: 8
      collect_errors: true

  # Batch for efficient loading
  - name: "batch_records"
    type: "buffer"
    properties:
      batch_size: 1000
      processor: "batch_loader"

  # Load stage with retry logic
  - name: "load_with_retry"
    type: "retry"
    properties:
      attempts: 3
      backoff: "exponential_backoff"
      stage:
        name: "load_data"
        type: "custom"
        properties:
          factory: "data_loader"
          config:
            destination: "warehouse"
```

### Real-time Processing Pipeline

```yaml
version: "1.0.0"
pipeline_name: "realtime_processor"
pipeline_type: "streaming"

streaming:
  buffer_size: 10000
  concurrency: 16

metrics:
  enabled: true
  type: "influxdb"
  endpoint: "http://localhost:8086"

stages:
  # Parse incoming events
  - name: "parse_events"
    type: "map"
    metrics: true
    properties:
      map_function: "event_parser"
      concurrency: 8

  # Filter valid events
  - name: "filter_valid"
    type: "filter"
    metrics: true
    properties:
      filter_function: "event_validator"

  # Route by event type
  - name: "route_by_type"
    type: "router"
    metrics: true
    properties:
      selector_function: "event_type_selector"
      concurrency: 4
      routes:
        user_events:
          name: "process_user_events"
          type: "custom"
          properties:
            factory: "user_event_processor"
        system_events:
          name: "process_system_events"
          type: "custom"
          properties:
            factory: "system_event_processor"

  # Aggregate in time windows
  - name: "time_window"
    type: "tumbling_time_window"
    properties:
      duration: 60000  # 1 minute windows

  # Final aggregation
  - name: "aggregate_metrics"
    type: "map"
    properties:
      map_function: "metrics_aggregator"
```

### Complex Processing with Fan-out/Fan-in

```yaml
version: "1.0.0"
pipeline_name: "complex_processor"
pipeline_type: "classic"

stages:
  # Initial processing
  - name: "preprocess"
    type: "map"
    properties:
      map_function: "preprocessor"

  # Parallel processing branches
  - name: "parallel_analysis"
    type: "fan_out"
    properties:
      concurrency: 6
      stages:
        - name: "sentiment_analysis"
          type: "map"
          properties:
            map_function: "sentiment_analyzer"
        - name: "entity_extraction"
          type: "map"
          properties:
            map_function: "entity_extractor"
        - name: "classification"
          type: "map"
          properties:
            map_function: "classifier"

  # Combine results
  - name: "combine_analysis"
    type: "fan_in"
    properties:
      aggregator: "analysis_combiner"

  # Final processing with circuit breaker
  - name: "final_processing"
    type: "circuit_breaker"
    properties:
      failure_threshold: 3
      reset_timeout: 30000
      stage:
        name: "enrichment"
        type: "custom"
        properties:
          factory: "data_enricher"
```

## Best Practices

### Configuration Organization

1. **Use meaningful stage names** that describe the operation being performed
2. **Group related configurations** in separate YAML files for complex systems
3. **Use environment-specific configurations** for different deployment environments
4. **Version your configuration files** alongside your code

### Performance Optimization

1. **Set appropriate concurrency levels** based on your system resources
2. **Use buffering** for batch operations to improve throughput
3. **Enable metrics and tracing** selectively to avoid overhead
4. **Configure timeouts** for external service calls

### Error Handling

1. **Always configure error handlers** for stages that might fail
2. **Use circuit breakers** for external service dependencies
3. **Implement retry logic** with appropriate backoff strategies
4. **Set up dead letter queues** for critical data that must not be lost

### Testing

1. **Test with small datasets** first to validate configuration
2. **Use debug mode** to validate configuration without execution
3. **Monitor metrics** to ensure pipelines are performing as expected
4. **Validate YAML syntax** and configuration before deployment

### Security

1. **Don't include sensitive data** in configuration files
2. **Use environment variables** for credentials and endpoints
3. **Validate all inputs** in custom executor functions
4. **Implement proper authentication** for metrics and tracing endpoints

### Maintenance

1. **Document custom executors** and their expected inputs/outputs
2. **Use semantic versioning** for configuration schemas
3. **Plan for backward compatibility** when updating configurations
4. **Monitor and alert** on pipeline failures and performance degradation

---

For more information, see the other documentation files:
- [Pipeline Documentation](PIPELINE.md) - Core pipeline concepts
- [Observability Documentation](OBSERVABILITY.md) - Detailed observability setup
- [Examples Documentation](EXAMPLES.md) - Additional practical examples
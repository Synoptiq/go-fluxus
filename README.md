# Fluxus - Go Pipeline Library
<p align="center">
<img alt="GitHub license" src="https://img.shields.io/github/license/synoptiq/go-fluxus">
<img alt="GitHub tag check runs" src="https://img.shields.io/github/check-runs/synoptiq/go-fluxus/v1.1.1">
<img alt="GitHub Issues or Pull Requests" src="https://img.shields.io/github/issues/synoptiq/go-fluxus">
<img alt="GitHub repo size" src="https://img.shields.io/github/repo-size/synoptiq/go-fluxus">
</p>




<p align="center">
  <img src="logo.svg" alt="Fluxus Logo" width="100" height="200">
</p>

**Fluxus is a modern, type-safe pipeline orchestration library for Go that makes complex data processing elegant and efficient.**

A lightweight yet powerful framework for building flexible data processing pipelines with built-in support for parallel execution, error handling, and advanced flow control. Designed for performance-critical applications where reliability and type safety matter.

## Features

- ⚡ **High-performance** parallel processing with fine-grained concurrency control
- 🔄 **Fan-out/fan-in** patterns for easy parallelization
- 🧬 **Type-safe** pipeline construction using Go generics
- 🛡️ **Robust error handling** with custom error strategies
- ⏱️ **Context-aware** operations with proper cancellation support
- 🔁 **Retry mechanisms** with configurable backoff strategies
- 📦 **Batch processing** capabilities for efficient resource utilization
- 📊 **Metrics collection** with customizable collectors
- 🔍 **OpenTelemetry tracing** for observability
- 🧯 **Circuit breaker** pattern for fault tolerance
- 🚦 **Rate limiting** to control throughput
- 🧠 **Memory pooling** for reduced allocations
- 🧪 **Thoroughly tested** with comprehensive examples
- 🔗 **Chain stages** with different input/output types

Perfect for ETL workloads, data processing services, API orchestration, and any application that needs to chain operations with reliable error handling and parallel execution.

## Installation

```bash
go get github.com/synoptiq/go-fluxus
```

## Quick Start

Check out the [examples](examples) directory for practical use cases.

## Core Components

### Stage

A `Stage` is the basic unit of processing in a pipeline. It takes an input, performs some operation, and produces an output.

```go
// Create a stage using a function
stage := fluxus.StageFunc[Input, Output](func(ctx context.Context, input Input) (Output, error) {
    // Process input and return output
    return output, nil
})
```

### Chain

`Chain` combines two stages where the output of the first becomes input to the second. This allows you to create pipelines with stages of different input/output types.

```go
// Chain two stages with compatible types
stage1 := fluxus.StageFunc[string, int](/* ... */)
stage2 := fluxus.StageFunc[int, bool](/* ... */)

// Chain them together
chainedStage := fluxus.Chain(stage1, stage2)

// The chained stage takes a string input and returns a bool output
result, err := chainedStage.Process(ctx, "input")
```

### Pipeline

A `Pipeline` represents a sequence of processing stages encapsulated in a single stage.

```go
// Create a pipeline with a single stage (which may be a chained stage)
pipeline := fluxus.NewPipeline(stage)

// Add custom error handling
pipeline.WithErrorHandler(func(err error) error {
    log.Printf("Pipeline error: %v", err)
    return err
})

// Process input
result, err := pipeline.Process(ctx, input)
```

## Patterns

### FanOut

`FanOut` processes an input using multiple stages in parallel and collects the results.

```go
// Create a fan-out stage with multiple processing functions
fanOut := fluxus.NewFanOut(stage1, stage2, stage3)

// Limit concurrency
fanOut.WithConcurrency(5)

// Process input
results, err := fanOut.Process(ctx, input)
```

### FanIn

`FanIn` aggregates the results from multiple parallel stages.

```go
// Create a fan-in stage with an aggregator function
fanIn := fluxus.NewFanIn(func(inputs []Result) (Output, error) {
    // Aggregate results
    return combinedResult, nil
})

// Process multiple inputs
result, err := fanIn.Process(ctx, inputs)
```

### Parallel

`Parallel` combines fan-out and fan-in to create a stage that processes an input in parallel and aggregates the results.

```go
// Create a parallel stage
parallel := fluxus.Parallel[Input, Result, Output](
    []fluxus.Stage[Input, Result]{stage1, stage2, stage3},
    func(results []Result) (Output, error) {
        // Aggregate results
        return combinedResult, nil
    },
)

// Process input
result, err := parallel.Process(ctx, input)
```

### Map

`Map` applies a single `Stage[I, O]` concurrently to each element of an input slice `[]I`, producing an output slice `[]O`. This is useful for parallelizing the same operation across multiple data items.

```go
// Stage that processes a single item (e.g., int to string)
processItem := fluxus.StageFunc[int, string](func(ctx context.Context, item int) (string, error) {
    // ... process item ...
    return processedString, nil
})

// Create a Map stage using the item processor
mapStage := fluxus.NewMap(processItem).
    WithConcurrency(runtime.NumCPU()). // Set concurrency limit
    WithCollectErrors(true)            // Collect all errors instead of failing fast

// Process a slice of inputs
inputSlice := []int{1, 2, 3, 4, 5}
results, err := mapStage.Process(ctx, inputSlice)

// If WithCollectErrors(true), err might be a *fluxus.MultiError
// and results will contain successes and zero values for errors.
// If WithCollectErrors(false) (default), err is the first error encountered
// and results will be nil.
```

### Buffer

`Buffer` collects items and processes them in batches. This is useful for optimizing operations that are more efficient when performed on multiple items at once (e.g., bulk database inserts).
 - Takes a `batchSize` and a `processor func(ctx context.Context, batch []I) ([]O, error)`.
 - The processor function receives a slice of buffered items (`batch`) when the buffer reaches `batchSize` or when the input stream ends.
 - It returns a slice of results (`[]O`) corresponding to the processed batch.

```go
// Example: Process integers in batches of 10
batchProcessor := fluxus.NewBuffer[int, string](10,
    func(ctx context.Context, batch []int) ([]string, error) {
        results := make([]string, len(batch))
        log.Printf("Processing batch of %d items", len(batch))
        for i, item := range batch {
            // Simulate processing
            results[i] = fmt.Sprintf("processed:%d", item)
            // Check context if processing is long
            if ctx.Err() != nil {
                return nil, ctx.Err()
            }
        }
        return results, nil
    },
)

// Assume 'inputItems' is a slice of integers []int{1, 2, ..., 25}
// The processor func will be called 3 times:
// 1. batch = {1..10}
// 2. batch = {11..20}
// 3. batch = {21..25}
allResults, err := batchProcessor.Process(ctx, inputItems)
// allResults will be []string{"processed:1", ..., "processed:25"}
```

### MapReduce

`MapReduce` implements the classic MapReduce pattern for distributed data processing, adapted for in-process pipelines.

 - Takes a `MapperFunc` and a `ReducerFunc`.
 - *Map Phase*: The `MapperFunc[I, K, V]` processes each input item I and emits an intermediate key-value pair `KeyValue[K, V]`. This phase can run in parallel using `WithParallelism(n)`.
 - *Shuffle Phase*: Intermediate key-value pairs are automatically grouped by key `K`.
 - *Reduce Phase*: The `ReducerFunc[K, V, R]` processes each unique key `K` along with all its associated values `[]V` (as `ReduceInput[K, V]`) and produces a final result `R`.
 - The final output of the `MapReduce` stage is a slice `[]R` containing the results from all reducers.

```go
// Example: Word Count
type WordCountResult struct {
    Word  string
    Count int
}

// Mapper: Emits (word, 1) for each word in a line
mapper := func(ctx context.Context, line string) (fluxus.KeyValue[string, int], error) {
    // In a real scenario, split line into words properly
    // For simplicity, assume line is a single word here
    word := strings.ToLower(strings.TrimSpace(line))
    if word == "" {
        // Skip empty lines/words - return zero value and nil error
        return fluxus.KeyValue[string, int]{}, nil
    }
    // Emit word and count 1
    return fluxus.KeyValue[string, int]{Key: word, Value: 1}, nil
}

// Reducer: Sums counts for each word
reducer := func(ctx context.Context, input fluxus.ReduceInput[string, int]) (WordCountResult, error) {
    sum := 0
    for _, count := range input.Values {
        sum += count
    }
    return WordCountResult{Word: input.Key, Count: sum}, nil
}

// Create the MapReduce stage
mapReduceStage := fluxus.NewMapReduce(mapper, reducer).
    WithParallelism(runtime.NumCPU()) // Parallelize the map phase

// Input data (e.g., lines from a file)
lines := []string{"hello world", "hello fluxus", "fluxus example"}

// Process the lines
// output is []WordCountResult
output, err := mapReduceStage.Process(ctx, lines)
// Example output (order not guaranteed):
// [ {Word:"hello", Count:2}, {Word:"world", Count:1}, {Word:"fluxus", Count:2}, {Word:"example", Count:1} ]
```

## Flow Control

### Filter

`Filter` conditionally passes items through, allowing you to create stages that only process items that meet certain criteria.
 - Takes a `PredicateFunc[T] func(ctx context.Context, item T) (bool, error)`.
 - If the predicate returns `true`, the item passes.
 - If `false`, the stage returns the item along with `fluxus.ErrItemFiltered`.
 - If the predicate errors, that error is propagated.

```go
// Example: Only allow even numbers
filterEven := fluxus.NewFilter(func(_ context.Context, i int) (bool, error) {
    return i%2 == 0, nil
})

// Usage in a chain:
// processEven := fluxus.Chain(filterEven, nextStage)
// output, err := processEven.Process(ctx, 5) // err will be ErrItemFiltered
// output, err := processEven.Process(ctx, 4) // err will be nil (if nextStage succeeds)
```

### Router

`Router` conditionally routes an item to one or more downstream stages concurrently (like a conditional `FanOut`).
 - Takes a `SelectorFunc[I] func(ctx context.Context, item I) ([]int, error)` which returns the indices of the `routes` to execute.
 - `routes` is a slice of `fluxus.Route[I, O]{ Name string, Stage Stage[I, O] }`.
 - If the selector returns an empty slice or `nil`, the stage returns `fluxus.ErrNoRouteMatched`.
 - The output is `[]O`, containing results from the executed stages in the order they were selected.
 - Use `WithConcurrency(n)` to limit concurrent stage executions.

```go
// Example: Route strings based on prefix
stageA := fluxus.StageFunc[string, string](...) // Processes "A:" prefixed strings
stageB := fluxus.StageFunc[string, string](...) // Processes "B:" prefixed strings

router := fluxus.NewRouter(
    func(_ context.Context, item string) ([]int, error) {
        if strings.HasPrefix(item, "A:") {
            return []int{0}, nil // Route to stageA (index 0)
        }
        if strings.HasPrefix(item, "B:") {
            return []int{1}, nil // Route to stageB (index 1)
        }
        if strings.HasPrefix(item, "BOTH:") {
             return []int{0, 1}, nil // Route to both A and B
        }
        return nil, nil // No match
    },
    fluxus.Route[string, string]{Name: "Processor A", Stage: stageA},
    fluxus.Route[string, string]{Name: "Processor B", Stage: stageB},
).WithConcurrency(2)

// output is []string
// output, err := router.Process(ctx, "A:data") // Executes stageA -> output: ["resultA"]
// output, err := router.Process(ctx, "B:data") // Executes stageB -> output: ["resultB"]
// output, err := router.Process(ctx, "BOTH:data") // Executes stageA & stageB -> output: ["resultA", "resultB"]
// output, err := router.Process(ctx, "C:data") // err is ErrNoRouteMatched
```

### JoinByKey

`JoinByKey` groups items from an input slice `[]I` into a `map[K][]I` based on a key extracted by `keyFunc`.
 - Takes a `KeyFunc[I, K] func(ctx context.Context, item I) (K, error)`.
 - Useful for collecting and grouping results after a `FanOut` or `Router` stage.

```go
type Result struct {
    GroupID int
    Data    string
}

// Assume 'results' is []Result from a previous FanOut/Router stage
// results := []Result{ {1, "a"}, {2, "b"}, {1, "c"} }

joiner := fluxus.NewJoinByKey(func(_ context.Context, r Result) (int, error) {
    return r.GroupID, nil
})

// outputMap is map[int][]Result
outputMap, err := joiner.Process(ctx, results)
// outputMap would be: map[int][]Result{
//  1: {{1, "a"}, {1, "c"}},
//  2: {{2, "b"}},
// }
```

## Resiliency

### Circuit Breaker

Circuit breaker prevents cascading failures by automatically stopping calls to a failing service.
 - Takes a `Stage` to protect, a failure threshold, and a reset timeout.
 - If the failure threshold is reached, the circuit opens and all calls to the stage will fail immediately until the reset timeout expires.
 - After the reset timeout, the circuit enters a half-open state where a limited number of requests are allowed to test if the service has recovered.

```go
// Create a circuit breaker that will open after 5 failures
// and attempt to reset after 10 seconds
circuitBreaker := fluxus.NewCircuitBreaker(
    stage,              // The stage to protect
    5,                  // Failure threshold
    10*time.Second,     // Reset timeout
    fluxus.WithSuccessThreshold[Input, Output](3),        // Require 3 successes to close
    fluxus.WithHalfOpenMaxRequests[Input, Output](2),     // Allow 2 test requests when half-open
)

// Process with circuit breaker protection
result, err := circuitBreaker.Process(ctx, input)
// If circuit is open, err will be fluxus.ErrCircuitOpen
```

### Retry with Backoff

The `Retry` stage automatically retries a failed stage up to a specified number of times, with an optional backoff strategy between attempts.
 - Takes a `Stage` to wrap and the maximum total number of attempts (initial + retries).
 - You can customize the backoff strategy (e.g., exponential, constant) using `WithBackoff`.
 - You can specify which errors should trigger a retry using `WithShouldRetry`.

```go
// Create a stage that retries on failure, making up to 3 total attempts
retry := fluxus.NewRetry(stage, 3)

// Add custom backoff strategy (exponential backoff: 100ms, 200ms delay)
retry.WithBackoff(func(attempt int) int { // attempt is 0-based index of the *retry*
    return 100 * (1 << attempt)
})

// Only retry specific errors (e.g., temporary network issues)
retry.WithShouldRetry(func(err error) bool {
    // Replace io.ErrTemporary with your actual retryable error check
    return errors.Is(err, io.ErrTemporary)
})
```

### Timeout

The `Timeout` stage wraps another stage to limit its execution time. If the wrapped stage does not complete within the specified duration, its context is cancelled, and the `Timeout` stage typically returns a `TimeoutError`.
 - Takes a `Stage` to wrap and a `time.Duration` for the timeout.

```go
// Create a stage that will time out after 5 seconds
timeoutStage := fluxus.NewTimeout(stage, 5*time.Second)

// Process input with the timeout applied
result, err := timeoutStage.Process(ctx, input)
// If timeout occurs, err will likely be *fluxus.TimeoutError
```

### Dead Letter Queue (DLQ)

`DeadLetterQueue` wraps another stage to capture items that consistently fail processing, even after potential retries. It sends these problematic items and their associated errors to a configured `DLQHandler` for logging, inspection, or later reprocessing, preventing them from blocking the main pipeline flow indefinitely.

 - Wraps an existing `Stage[I, O]`.
 - Takes a `DLQHandler[I]` implementation via `WithDLQHandler`.
 - Uses a `shouldDLQ func(error) bool` predicate (configurable via `WithShouldDLQ`) to determine which errors trigger the DLQ (by default, excludes context errors, `ErrCircuitOpen`, `ErrItemFiltered`, etc.).
 - Logs errors occurring within the `DLQHandler` itself (configurable via `WithDLQErrorLogger`).
 - The original error from the wrapped stage is always returned by the `DeadLetterQueue` stage's `Process` method.

```go
// Example: Log failed strings to a DLQ after retries
flakyStage := fluxus.StageFunc[string, string](/* ... might fail ... */)
retryStage := fluxus.NewRetry(flakyStage, 3) // Retry 3 times

// Simple DLQ handler that logs the failure
loggingDLQ := fluxus.DLQHandlerFunc[string](func(ctx context.Context, item string, processingError error) error {
    log.Printf("[DLQ] Item: %q failed permanently with error: %v", item, processingError)
    return nil // Indicate DLQ handling succeeded
})

// Wrap the retry stage with the DLQ
dlqEnabledStage := fluxus.NewDeadLetterQueue(
    retryStage, // Wrap the stage *after* retries
    fluxus.WithDLQHandler[string, string](loggingDLQ),
)

// Process an item that will eventually fail all retries
_, err := dlqEnabledStage.Process(ctx, "input-that-fails")
// err will be the error from retryStage (e.g., RetryExhaustedError)
// The DLQ handler will have logged the failure details.
```

## Advanced Features

### Rate Limiting

`RateLimiter` wraps a stage to control the rate at which it processes items. It uses a token bucket algorithm to enforce limits on requests per second and burst capacity, preventing downstream services from being overwhelmed.

 - Takes a `Stage` to wrap, a `rate.Limit` (requests per second), and a burst size.
 - Can be configured with a `WithLimiterTimeout` to fail fast if waiting for a token takes too long.
 - Limits can be adjusted dynamically after creation using `SetLimit` and `SetBurst`.

```go
// Create a rate limiter with 10 requests per second and burst of 5
limiter := fluxus.NewRateLimiter(
    stage,
    rate.Limit(10),  // 10 requests/second
    5,               // Burst of 5
    fluxus.WithLimiterTimeout[Input, Output](100*time.Millisecond),  // Timeout after 100ms
)

// Dynamically adjust rate limits
limiter.SetLimit(rate.Limit(5))  // Change to 5 requests/second
limiter.SetBurst(10)             // Change burst to 10
```

### Memory Pooling

Fluxus provides utilities for memory pooling to reduce garbage collection pressure in high-throughput scenarios by reusing objects and slices.

 - `NewObjectPool[T]` creates a pool for custom objects using a factory function. Objects are retrieved with `Get()` and returned with `Put()`.
 - `NewSlicePool[T]` creates a pool specifically for slices, allowing efficient reuse. Slices can be retrieved with `Get()` or `GetWithCapacity()`.
 - Pools can be configured with names and capacities and can be pre-warmed using `PreWarmPool`.

```go
// Create a memory pool for reducing allocations
pool := fluxus.NewObjectPool(
    func() MyObject { return MyObject{} },  // Factory function
    fluxus.WithPoolName[MyObject]("my-objects"),
    fluxus.WithMaxCapacity[MyObject](100),
)

// Get an object from the pool
obj := pool.Get()

// Return to the pool when done
pool.Put(obj)

// Use specialized slice pool
slicePool := fluxus.NewSlicePool[int](
    10,  // Initial capacity
    fluxus.WithPoolName[[]int]("int-slices"),
)

// Get a slice with specific capacity
slice := slicePool.GetWithCapacity(20)
```

### Optimized Pooled Buffer

`PooledBuffer` is a specialized, high-performance version of `Buffer` that integrates with the memory pooling system. It reuses internal batch slices, further reducing allocations during batch processing, making it suitable for performance-critical applications handling large volumes of data.

 - Takes a `batchSize` and a processor `func(ctx context.Context, batch []I) ([]O, error)`.
 - Automatically uses slice pooling for its internal buffers

```go
// Create a high-performance buffer with object pooling
pooledBuffer := fluxus.NewPooledBuffer[Item, Result](
    10,  // Batch size
    func(ctx context.Context, batch []Item) ([]Result, error) {
        // Process batch
        return results, nil
    },
    fluxus.WithBufferName[Item, Result]("pooled-buffer"),
)
```

## Metrics and Tracing

### Metrics Collection

Fluxus allows integrating custom metrics collection to monitor pipeline performance and behavior.

 - Define a type that implements the `MetricsCollector` interface (e.g., for Prometheus, StatsD).
 - Wrap stages with `NewMetricatedStage`, providing a stage name (`WithStageName`) and optionally a specific collector instance (`WithMetricsCollector`). If no collector is specified, it uses `fluxus.DefaultMetricsCollector`.
 - The collector receives callbacks for events like stage start, end, success, and failure.

```go
// Implement the MetricsCollector interface for your metrics system
type PrometheusMetrics struct{}

func (p *PrometheusMetrics) StageStarted(ctx context.Context, stageName string) {
    // Increment counter in Prometheus
}

// ... implement other methods ...

// Use the metrics collector
collector := &PrometheusMetrics{}
fluxus.DefaultMetricsCollector = collector

// Create a metricated stage
metricated := fluxus.NewMetricatedStage(
    stage,
    fluxus.WithStageName[Input, Output]("my-stage"),
    fluxus.WithMetricsCollector[Input, Output](collector),
)
```

### OpenTelemetry Tracing

Built-in support for distributed tracing using OpenTelemetry allows you to visualize request flows across your pipeline and potentially other services.

 - Wrap stages with `NewTracedStage`, providing a tracer name (`WithTracerName`) and optional attributes (`WithTracerAttributes`). It uses the globally configured OpenTelemetry tracer provider.
 - Specialized wrappers like `NewTracedFanOut` provide more context for specific patterns.
 - Spans are automatically created for stage execution, capturing duration and errors.

```go
// Create a traced stage
traced := fluxus.NewTracedStage(
    stage,
    fluxus.WithTracerName[Input, Output]("my-stage"),
    fluxus.WithTracerAttributes[Input, Output](
        attribute.String("service", "my-service"),
    ),
)

// Specialized tracing for fan-out
tracedFanOut := fluxus.NewTracedFanOut(
    fanOut,
    "parallel-processing",
    attribute.String("operation", "data-transformation"),
)
```

## Best Practices

### Chaining Stages with Different Types

Use the `Chain` function to connect stages with different input/output types:

```go
// Create stages with different input/output types
parseStage := fluxus.StageFunc[string, []int](/* parse string to integers */)
sumStage := fluxus.StageFunc[[]int, int](/* sum integers */)
formatStage := fluxus.StageFunc[int, string](/* format result */)

// Chain them together
processStage := fluxus.Chain(parseStage, 
                    fluxus.Chain(sumStage, formatStage))

// Create a pipeline with the chained stage
pipeline := fluxus.NewPipeline(processStage)
```

### Use Context for Cancellation

Use `context.Context` to manage cancellation and timeouts effectively:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
result, err := pipeline.Process(ctx, input)
```

### Handle Errors Appropriately

Use custom error handlers to manage errors in a pipeline gracefully:

```go
pipeline.WithErrorHandler(func(err error) error {
    if errors.Is(err, SomeSpecificError) {
        // Handle specific error
        return nil  // Ignore error
    }
    return err  // Propagate other errors
})
```

### Control Resource Usage

Use `WithConcurrency()` to limit the number of concurrent executions in a stage or pipeline. This is especially important for I/O-bound tasks to avoid overwhelming resources:

```go
fanOut.WithConcurrency(runtime.NumCPU())  // Limit concurrency to CPU count
```

### Use Generics for Type Safety

Use generics to create type-safe stages and pipelines. This ensures that the input and output types are consistent throughout the pipeline:

```go
stage := fluxus.StageFunc[CustomInput, CustomOutput](func(ctx context.Context, input CustomInput) (CustomOutput, error) {
    // Type-safe processing
})
```

### Enhance Observability

Combine metrics and tracing for comprehensive monitoring:

```go
// Start with a basic stage
baseStage := fluxus.StageFunc[Input, Output](/* ... */)

// Add metrics
metricated := fluxus.NewMetricatedStage(baseStage, /* ... */)

// Add tracing on top of metrics
traced := fluxus.NewTracedStage(metricated, /* ... */)

// Use it in a pipeline
pipeline := fluxus.NewPipeline(traced)
```

### Pre-warm Pools for Performance

Pre-warm object pools to reduce the overhead of creating new objects during high-load periods. This is especially useful for frequently created objects in performance-critical applications.

```go
// Create the pool
pool := fluxus.NewObjectPool(/* ... */)

// Pre-warm with 100 objects before high-load periods
fluxus.PreWarmPool(pool, 100)
```

## Error Types

Fluxus provides specialized error types for different scenarios:

- `StageError`: Identifies which specific stage in a pipeline failed.
- `FanOutError`: Contains errors from multiple failed stages in fan-out.
- `MultiError`: Wraps multiple errors, often returned by `Map` when `WithCollectErrors(true)` is used.
- `RetryExhaustedError`: Indicates all retry attempts were exhausted.
- `TimeoutError`: Indicates a stage timed out.
- `BufferError`: Provides details about batch processing failures.
- `ErrCircuitOpen`: Indicates a circuit breaker is open and rejecting requests.
- `ErrItemFiltered`: Returned by `Filter` when an item does not meet the predicate criteria.
- `ErrNoRouteMatched`: Returned by `Router` when the selector function doesn't match any configured routes.
- `RateLimitWaitError`: Returned by `RateLimiter` if waiting for a token exceeds the configured timeout (`WithLimiterTimeout`).

## Performance Benchmarks

Fluxus is designed with performance in mind. Here's how to run the benchmarks and what they measure:

```bash
# Run all benchmarks
go test -bench=Benchmark -benchmem ./...

# Run a specific benchmark
go test -bench=BenchmarkChainedPipeline -benchmem ./...

# Run benchmarks with more iterations for better statistical significance
go test -bench=Benchmark -benchmem -count=5 ./...
```

### Benchmark Results

See [BENCHMARK.md](BENCHMARK.md) for detailed results.

### Performance Optimization Tips

1. **Limit Concurrency**: While unlimited concurrency can be faster for CPU-bound tasks, setting an appropriate concurrency limit is crucial for I/O-bound tasks to avoid resource exhaustion. Use `WithConcurrency()` to tune this.

2. **Buffer Size**: For batch processing, choose a buffer size that balances memory usage and processing efficiency. Too small and you lose efficiency, too large and you waste memory.

3. **Chain Depth**: Deeper chains incur more overhead. Consider flattening chains if extreme performance is needed.

4. **Error Handling**: Custom error handlers add minimal overhead but provide significant benefits in production environments.

5. **Context Cancellation**: Use contexts to cancel operations early when results are no longer needed.

6. **Object Pooling**: For frequently created objects, use the provided pooling mechanisms to reduce GC pressure.

7. **Rate Limiting**: Apply rate limiting at appropriate points to prevent overloading downstream services.

8. **Circuit Breakers**: Place circuit breakers around unreliable services to fail fast when necessary.

9. **Metrics & Tracing**: In production environments, the overhead of metrics and tracing is usually negligible compared to their benefits, but use the noop implementations in performance-critical paths if needed.

10. **Memory Management**: For large data processing, consider using the `PooledBuffer` to reduce allocations.


## Use Cases

Fluxus is well-suited for a variety of data processing and orchestration tasks, including:

-   **ETL Pipelines:** Extracting data from various sources, transforming it concurrently, and loading it into destinations.
-   **Real-time Data Processing:** Handling streams of data, applying filters, aggregations, and enrichments in sequence or parallel.
-   **API Aggregation & Orchestration:** Calling multiple downstream APIs concurrently, processing their responses, and aggregating results.
-   **Background Job Processing:** Building robust workers that process jobs from a queue with retries, timeouts, and error handling.
-   **Image/Video Processing:** Creating pipelines to resize, encode, or analyze media files in parallel.
-   **Complex Workflows:** Implementing multi-step business logic with conditional routing and fault tolerance.


## Contributing

Contributions are welcome! Whether it's reporting a bug, suggesting a feature, or submitting code, your help is appreciated.

**Reporting Issues:**
-   Use the GitHub Issues tracker to report bugs or suggest enhancements.
-   Please provide clear steps to reproduce the issue or a detailed description of the feature request.

**Pull Requests:**
-   Fork the repository and create a new branch for your changes.
-   Ensure your code adheres to Go best practices and includes relevant tests.
-   Run `go fmt` and `go vet` before submitting.
-   Keep pull requests focused on a single issue or feature.
-   Provide a clear description of the changes in your pull request.

We aim to review contributions promptly. Thank you for helping improve Fluxus!

## License

MIT License. See [LICENSE](LICENSE) for details.
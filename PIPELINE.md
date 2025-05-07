# Fluxus Pipeline Documentation

The Pipeline is the core building block of Fluxus. It allows you to chain operations together, creating a coherent flow of data processing with strong type guarantees.

## Core Components

### Stage

A `Stage` is the basic unit of processing in a pipeline. It takes an input of type `I`, performs an operation on it, and produces an output of type `O`. A stage can also return an error if processing fails.

```go
// Stage interface definition
type Stage[I, O any] interface {
	Process(ctx context.Context, input I) (O, error)
}
```

#### Creating Stages

The most common way to create a stage is using `StageFunc`:

```go
// Create a stage using a function
stringToIntStage := fluxus.StageFunc[string, int](func(ctx context.Context, input string) (int, error) {
    // Convert string to int
    value, err := strconv.Atoi(input)
    if err != nil {
        return 0, fmt.Errorf("failed to convert string to int: %w", err)
    }
    return value, nil
})
```

You can also create custom stage implementations by implementing the `Stage` interface:

```go
// Custom stage implementation
type DoubleStage struct{}

func (d *DoubleStage) Process(_ context.Context, input int) (int, error) {
    return input * 2, nil
}

// Usage
stage := &DoubleStage{}
```

### Extended Stage Lifecycle Interfaces

Stages can optionally implement various lifecycle interfaces to manage resources, initialize state, perform cleanup, reset, or report health. The `Pipeline` (and `StreamPipeline`) will automatically call these methods on any of its managed stages that implement them.

```go
package fluxus
import "context"

// Initializer allows a stage to perform one-time setup or initialization tasks
// when the pipeline starts.
type Initializer interface {
    Setup(ctx context.Context) error
}

// Closer allows a stage to release resources or perform final cleanup
// when the pipeline stops.
type Closer interface {
    Close(ctx context.Context) error
}

// Resettable allows a stage's internal state to be reset to an initial condition.
type Resettable interface {
    Reset(ctx context.Context) error
}

// HealthCheckable allows a stage to report its current operational health.
// Returning an error typically indicates an unhealthy status.
type HealthCheckable interface {
    HealthStatus(ctx context.Context) error
}
```

When you use a Pipeline:
 - `pipeline.Start(ctx)` will call `Setup(ctx)` on all `Initializer` stages.
 - `pipeline.Stop(ctx)` will call `Close(ctx)` on all `Closer` stages.
  
The `pipeline.Reset(ctx)` and `pipeline.HealthStatus(ctx)` methods also interact with these interfaces, as described below.

### Pipeline Lifecycle and State Management

The `Pipeline` provides methods to control its lifecycle and manage its state, including the state of its constituent stages.

`Start`, `Stop`, `Reset`, and `HealthStatus`

These methods manage the pipeline's operational state:

```go
// Example context
ctx := context.Background()

// Start the pipeline and any sub-stages implementing Initializer
if err := pipeline.Start(ctx); err != nil {
    log.Fatalf("Failed to start pipeline: %v", err)
}

// ... pipeline processing ...

// Reset the pipeline and any sub-stages implementing Resettable
if err := pipeline.Reset(ctx); err != nil {
    log.Printf("Error resetting pipeline: %v", err)
}

// Check the health of the pipeline and any sub-stages implementing HealthCheckable
// The exact return type for HealthStatus might be a custom struct or an error.
// For example, if it returns an error, nil might mean healthy.
if healthErr := pipeline.HealthStatus(ctx); healthErr != nil {
    log.Printf("Pipeline health check failed: %v", healthErr)
}

// Stop the pipeline and any sub-stages implementing Closer
if err := pipeline.Stop(ctx); err != nil {
    log.Printf("Error stopping pipeline: %v", err)
}
```

 - `Start(ctx)`: Initializes and starts the pipeline. It calls `Setup()` on `Initializer` stages. Process will return a `fluxus.ErrPipelineNotStarted` error if `Start` was not called.
 - `Stop(ctx)`: Stops the pipeline. It calls `Close()` on `Closer` stages.
 - `Reset(ctx)`: Resets the pipeline. This typically involves calling `Reset()` on all stages within the pipeline that implement the `Resettable` interface, allowing them to clear or re-initialize their internal state.
 - `HealthStatus(ctx)`: Checks the health of the pipeline. This involves calling `HealthStatus()` on all stages that implement the `HealthCheckable` interface. The pipeline may aggregate these statuses to provide an overall health indication (e.g., returning an error if any stage is unhealthy).

### Chain

`Chain` combines two stages where the output of the first becomes input to the second. This allows creating pipelines with stages that have different input and output types.

```go
// Chain two stages with compatible types
stage1 := fluxus.StageFunc[string, int](/* string to int */)
stage2 := fluxus.StageFunc[int, bool](/* int to bool */)

// Chain them together - inputs string, outputs bool
chainedStage := fluxus.Chain(stage1, stage2)

// Execute the chain
result, err := chainedStage.Process(ctx, "42")
```

You can chain multiple stages using nested calls to `Chain`:

```go
// Chain multiple stages
stage1 := fluxus.StageFunc[string, []string](/* split string */)
stage2 := fluxus.StageFunc[[]string, int](/* count words */)
stage3 := fluxus.StageFunc[int, string](/* format output */)

// Chain them all together
pipeline := fluxus.Chain(stage1, fluxus.Chain(stage2, stage3))
```

Or use `ChainMany` to chain multiple stages of compatible types:

```go
// Chain multiple stages using ChainMany
pipeline := fluxus.ChainMany[string, string](stage1, stage2, stage3)
```

### Pipeline

A `Pipeline` wraps a stage (which can be a chained stage) and provides additional functionality like error handling and lifecycle management.

Note: This basic `Pipeline[I, O]` processes items individually via its `Process` method. For continuous stream processing using channels, see the generic `StreamPipeline[I, O]` documented in STREAMPIPELINE.md.

```go
// Create a pipeline with a chained stage
pipeline := fluxus.NewPipeline(chainedStage)

// Add custom error handling
pipeline.WithErrorHandler(func(err error) error {
    log.Printf("Pipeline error: %v", err)
    return err
})

// Start the pipeline (if the contained stage implements Starter)
if err := pipeline.Start(ctx); err != nil {
    log.Fatalf("Failed to start pipeline: %v", err)
}
defer func() {
    if stopErr := pipeline.Stop(ctx); stopErr != nil {
        log.Printf("Error stopping pipeline: %v", stopErr)
    }
}()

// Process input through the pipeline
result, err := pipeline.Process(ctx, "input")
```

### Windowing (Stream Pipelines)

For stream processing, Fluxus provides built-in `StreamStage` implementations for windowing patterns. These stages group items from a stream into windows based on item count or time, which can then be processed as a batch. Common types include:

-   **Tumbling Windows**: Fixed-size, non-overlapping windows.
    -   `TumblingCountWindow[T,any]`: Groups by item count.
    -   `TumblingTimeWindow[T,any]`: Groups by time duration.
-   **Sliding Windows**: Fixed-size, potentially overlapping windows.
    -   `SlidingCountWindow[T,any]`: Emits a window of the last `size` items every `slide` items.
    -   `SlidingTimeWindow[T,any]`: Emits a window of items from the last `duration` every `slide` interval.

These stages typically output `[]T` (a slice of the windowed items).

Refer to the Stream Pipeline Documentation for detailed examples and usage of these windowing stages.

## Error Handling

Fluxus provides several mechanisms for error handling in pipelines:

### Error Handlers

Add custom error handling to a pipeline using `WithErrorHandler`:

```go
pipeline.WithErrorHandler(func(err error) error {
    // Log the error
    log.Printf("Pipeline error: %v", err)
    
    // You can transform or wrap the error
    return fmt.Errorf("pipeline failed: %w", err)
    
    // Or return nil to suppress the error
    // return nil
})
```

### Error Types

Fluxus provides specialized error types for different scenarios:

- `StageError`: Identifies which specific stage in a pipeline failed
- `MultiError`: Wraps multiple errors from different stages
- `MapItemError`: Error from a specific item in a Map operation
- `FanOutError`: Contains errors from multiple failed stages in fan-out
- `BufferError`: Provides details about batch processing failures
- `PipelineConfigurationError` : Error in pipeline creation or validation
- `PipelineLifecycleError` : Error from Start or Stop method.

## Context Handling

All Fluxus stages and pipelines respect Go's `context.Context` for cancellation, timeouts, and value propagation:

```go
// Creating a context with timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// Process with context
result, err := pipeline.Process(ctx, input)
if errors.Is(err, context.DeadlineExceeded) {
    // Handle timeout
}
```

## Best Practices

### Chaining Stages with Different Types

Use the `Chain` function to connect stages with different input/output types:

```go
parseStage := fluxus.StageFunc[string, []int](/* parse string to integers */)
sumStage := fluxus.StageFunc[[]int, int](/* sum integers */)
formatStage := fluxus.StageFunc[int, string](/* format result */)

// Chain them together
processStage := fluxus.Chain(parseStage, 
                  fluxus.Chain(sumStage, formatStage))
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

### Enhance Observability

Add metrics and tracing to your pipelines:

```go
// Add metrics
metricated := fluxus.NewMetricatedStage(baseStage, /* ... */)

// Add tracing on top of metrics
traced := fluxus.NewTracedStage(metricated, /* ... */)

// Use it in a pipeline
pipeline := fluxus.NewPipeline(traced)
```

## Performance Considerations

- Pipeline overhead is generally minimal compared to the actual processing work.
- The main performance overhead comes from extra allocations when chaining stages.
- For high-performance applications, consider using memory pools and buffers.
- Benchmark your pipelines to identify bottlenecks.

```go
// Benchmark a pipeline
func BenchmarkSimplePipeline(b *testing.B) {
    stage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
        return strings.ToUpper(input), nil
    })
    pipeline := fluxus.NewPipeline(stage)
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = pipeline.Process(context.Background(), "hello world")
    }
}
```
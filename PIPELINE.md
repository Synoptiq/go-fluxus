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

### Starter and Stopper Stages

Stages can optionally implement `Starter` and `Stopper` interfaces to manage resources:

```go
// Corrected example
package fluxus
import "context"

type Starter interface {
	Start(ctx context.Context) error
}

type Stopper interface {
	Stop(ctx context.Context) error
}
```

The pipeline will call these methods when you call `pipeline.Start` and `pipeline.Stop`.

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

### `Start` and `Stop`

The `Start` and `Stop` methods manage the pipeline's lifecycle, particularly if the underlying stage has setup/teardown requirements.

```go
// Start the pipeline and any sub-stages implementing Starter
if err := pipeline.Start(ctx); err != nil {
    log.Fatalf("Failed to start pipeline: %v", err)
}

// Stop the pipeline and any sub-stages implementing Stopper
if err := pipeline.Stop(ctx); err != nil {
    log.Printf("Error stopping pipeline: %v", err)
}
```

`Process` will return a `fluxus.ErrPipelineNotStarted` error if Start was not called.

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
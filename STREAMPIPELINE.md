# Fluxus Stream Pipeline Documentation

The Stream Pipeline is an advanced component of Fluxus that enables continuous processing of data streams with backpressure support. It's designed for handling potentially unbounded data sources efficiently.

## Stream Processing Concepts

Unlike standard pipelines that process a single input to produce a single output, stream pipelines:

- Process continuous streams of data items
- Use channels to connect stages
- Support backpressure to handle varying processing rates
- Allow for concurrent processing
- Properly handle cancellation and cleanup
- Support lifecycle control through explicit `Start`, `Wait` and `Stop` methods.

## Core Components

### StreamStage

The `StreamStage` interface is the foundation of stream processing:

```go
// StreamStage defines the interface for a continuous stream processing stage
type StreamStage[I, O any] interface {
    // ProcessStream reads items from 'in', processes them, and sends results to 'out'.
    // It should run until 'in' is closed or 'ctx' is cancelled.
    //
    // Implementations MUST ensure 'out' is closed before returning.
    ProcessStream(ctx context.Context, in <-chan I, out chan<- O) error
}
```

### StreamAdapter

The `StreamAdapter` converts a regular `Stage` into a `StreamStage`, adding concurrency and error handling:

```go
// Create a regular stage
stage := fluxus.StageFunc[int, string](func(ctx context.Context, input int) (string, error) {
    return strconv.Itoa(input), nil
})

// Wrap it with a StreamAdapter with various options
adapter := fluxus.NewStreamAdapter(
    stage,
    fluxus.WithAdapterConcurrency[int, string](4),      // Process up to 4 items concurrently
    fluxus.WithAdapterErrorStrategy[int, string](fluxus.SkipOnError), // Skip items that cause errors
    fluxus.WithAdapterBufferSize[int, string](10),      // Buffer size for internal channels
    fluxus.WithAdapterLogger[int, string](logger),      // Logger for errors and warnings
)
```

#### Error Handling Strategies

StreamAdapter supports different error handling strategies:

```go
// SkipOnError logs the error (default behavior) and continues with the next item
adapter := fluxus.NewStreamAdapter(stage, 
    fluxus.WithAdapterErrorStrategy[int, string](fluxus.SkipOnError))

// StopOnError logs the error and returns it, stopping the stage
adapter := fluxus.NewStreamAdapter(stage, 
    fluxus.WithAdapterErrorStrategy[int, string](fluxus.StopOnError))

// SendToErrorChannel sends the item and error to a dedicated error channel
errChan := make(chan fluxus.ProcessingError[int], 10)
adapter := fluxus.NewStreamAdapter(stage,
    fluxus.WithAdapterErrorStrategy[int, string](fluxus.SendToErrorChannel),
    fluxus.WithAdapterErrorChannel[int, string](errChan))
```

### StreamPipelineBuilder

The `StreamPipelineBuilder` facilitates the type-safe construction of a stream pipeline using builder pattern:

```go
// Create a new stream pipeline builder
builder := fluxus.NewStreamPipeline[InputType](
    fluxus.WithStreamLogger(logger),
    fluxus.WithStreamBufferSize(10),
    fluxus.WithStreamConcurrency(4),
    fluxus.WithStreamTracerProvider(tracerProvider), // Optional: Set up OpenTelemetry tracing
	fluxus.WithStreamMetricsCollector(metricsCollector) // Optional: Set up metrics collection
)

// Add stages, maintaining type safety
b2 := fluxus.AddStage(builder, "stage-1", stage1)
b3 := fluxus.AddStage(b2, "stage-2", stage2)
b4 := fluxus.AddStage(b3, "stage-3", stage3)

// Add a custom StreamStage directly
// b5 := fluxus.AddStreamStage(b4, "stream-stage", customStreamStage)

// Finalize the pipeline
// Specify the pipeline's overall Input and final Output types
pipeline, err := fluxus.Finalize[InputType, OutputType](b4)
if err != nil {
    log.Fatalf("Failed to build pipeline: %v", err)
}
```

### Lifecycle Management

Stream pipelines now have explicit `Start()`, `Wait()`, and `Stop()` methods to control their lifecycle:

```go
pipeline := /* ... build pipeline ... */

// Start the pipeline (non-blocking)
err := pipeline.Start(ctx, source, sink)
if err != nil {
    log.Fatalf("Failed to start pipeline: %v", err)
}

// Wait for the pipeline to complete or encounter an error (blocking)
if err := pipeline.Wait(); err != nil {
    log.Printf("Pipeline finished with error: %v", err)
}

// Gracefully stop the pipeline (can be called even if not Wait-ing)
if err := pipeline.Stop(ctx); err != nil {
	log.Printf("Error stopping pipeline: %v", err)
}
```

*Key points*:

 - `Start`: Initializes the pipeline and launches processing goroutines (non-blocking).
 - `Wait`: Blocks until processing finishes (after the source channel is closed) or an error occurs.
 - `Stop`: Gracefully shuts down the pipeline, cancels any running goroutines, and calls `Stop` on any stages implementing the `Stopper` interface.
 - The global `Run` function is a helper that calls these three methods sequentially.

 Errors during these lifecycle operations (like starting an already started pipeline or stopping with a timeout) are reported using specific types like `fluxus.ErrPipelineAlreadyStarted`, `fluxus.ErrPipelineNotStarted`, or `fluxus.PipelineLifecycleError`.

### Starter and Stopper Stages

Stages can optionally implement `Starter` and `Stopper` interfaces to manage resources:

```go
type Starter interface {
	Start(ctx context.Context) error
}

type Stopper interface {
	Stop(ctx context.Context) error
}
```

The pipeline will call these methods when you call `pipeline.Start` and `pipeline.Stop`.

## Running Stream Pipelines

To run a stream pipeline, you need to provide source and sink channels:

Note: `pipeline.Start()` and `pipeline.Run()` now accept typed channels (`<-chan I`, `chan<- O`), providing compile-time safety for the pipeline's input and output. Ensure the pipeline variable and channels match the types specified during `Finalize[I, O]`.

```go
// Create channels
source := make(chan int)
sink := make(chan OutputType)

// Create a context
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// Start the pipeline (non-blocking)
if err := pipeline.Start(ctx, source, sink); err != nil {
	log.Fatalf("Failed to start pipeline: %v", err)
}

// Feed data to the source in a goroutine
go func() {
    defer close(source) // IMPORTANT: Always close the source when done feeding
    for i := 0; i < 10; i++ {
        select {
        case source <- i:
            // Item sent successfully
        case <-ctx.Done():
            // Context cancelled, stop sending
            return
        }
    }
}()

// Process results from the sink in the main goroutine (or another one)
go func(){
    for result := range sink {
        // Do something with the result
        fmt.Println(result)
    }
}()

// Wait for pipeline to complete or error out (blocking)
if err := pipeline.Wait(); err != nil {
    log.Printf("Pipeline error: %v", err)
}

// Gracefully stop the pipeline if it's still running
if err := pipeline.Stop(ctx); err != nil {
	log.Printf("Error stopping pipeline: %v", err)
}
```

The `Run` function simplifies this pattern:

```go
// Create channels
source := make(chan int)
sink := make(chan OutputType) // Replace OutputType with the actual output type

// Create a context
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// Run the pipeline in a goroutine. This combines Start, Wait and Stop
var runErr error
var wg sync.WaitGroup
wg.Add(1)
go func() {
    defer wg.Done()
    // Run blocks until the pipeline finishes or the context is cancelled
    // pipeline must be *fluxus.StreamPipeline[int, OutputType]
    // source must be chan int (or <-chan int)
    // sink must be chan OutputType (or chan<- OutputType)
    runErr = pipeline.Run(ctx, source, sink)
    if runErr != nil && !errors.Is(runErr, context.Canceled) {
        log.Printf("Pipeline Run error: %v", runErr)
    }
}()

// Feed data to the source in another goroutine
go func() {
    defer close(source) // IMPORTANT: Always close the source when done feeding
    for i := 0; i < 10; i++ {
        select {
        case source <- i:
            // Item sent successfully
        case <-ctx.Done():
            // Context cancelled, stop sending
            log.Println("Source feeding cancelled.")
            return
        }
    }
    log.Println("Source feeding finished.")
}()

// Process results from the sink in the main goroutine (or another one)
// This goroutine will finish when the sink channel is closed by Run
go func() {
    for result := range sink {
        // Do something with the result
        fmt.Println(result)
    }
    log.Println("Sink channel closed, finished processing results.")
}()

// Wait for the pipeline Run goroutine to finish
wg.Wait()
log.Println("Pipeline Run has completed.")

// Note: The sink processing goroutine will finish automatically
// because Run closes the sink channel upon completion or error.
```

## Advanced Usage

### Observability

Stream pipelines can be configured for observability using OpenTelemetry:

-   `WithStreamTracerProvider`: Enables distributed tracing for the overall pipeline run and for individual stages wrapped by `StreamAdapter`.
-   `WithStreamMetricsCollector`: Enables metrics collection for pipeline start/completion and default metrics (like items processed/skipped, concurrency) for stages wrapped by `StreamAdapter`.

Note that custom `StreamStage` implementations added via `AddStreamStage` need to incorporate their own tracing and metrics reporting if desired.

### Handling Backpressure

Stream pipelines handle backpressure automatically - if a downstream stage processes items slower than an upstream stage produces them, the producer will naturally slow down as channel operations block.

You can tune buffer sizes to manage backpressure:

```go
// Configure buffer sizes
builder := fluxus.NewStreamPipeline[InputType](
    fluxus.WithStreamBufferSize(100), // Default buffer size across the pipeline
)

// Individual stage can override buffer size
b2 := fluxus.AddStage(builder, stage1, 
    fluxus.WithAdapterBufferSize[InputType, IntermediateType](200))
```

### Context Cancellation

All stream stages respect context cancellation for proper cleanup:

```go
// Create a cancellable context
ctx, cancel := context.WithCancel(context.Background())

// Start the pipeline
go pipeline.Run(ctx, source, sink)

// Cancel the pipeline gracefully (e.g., after timeout)
time.AfterFunc(30*time.Second, func() {
    log.Println("Cancelling pipeline...")
    cancel()
})
```

### Mixing Stream and Regular Stages

You can use `AddStreamStage` to add a native `StreamStage` without wrapping it in an adapter:

```go
// Create a custom StreamStage implementation
type CustomStreamStage struct{}

func (s *CustomStreamStage) ProcessStream(ctx context.Context, in <-chan int, out chan<- string) error {
    defer close(out) // IMPORTANT: always close 'out' before returning
    
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case input, ok := <-in:
            if !ok {
                return nil // 'in' is closed, normal exit
            }
            
            // Process and send output
            select {
            case out <- fmt.Sprintf("Processed %d", input):
                // Successfully sent
            case <-ctx.Done():
                return ctx.Err()
            }
        }
    }
}

// Add the stream stage directly
builder := fluxus.NewStreamPipeline[int]()
b2 := fluxus.AddStreamStage(builder, &CustomStreamStage{})
```

## Performance Tuning

### Concurrency Control

Stream pipelines provide fine-grained concurrency control:

```go
// Pipeline-wide setting (defaults for stages that don't specify)
builder := fluxus.NewStreamPipeline[InputType](
    fluxus.WithStreamConcurrency(runtime.NumCPU()),
)

// Per-stage concurrency control
b2 := fluxus.AddStage(builder, stage1, 
    fluxus.WithAdapterConcurrency[InputType, IntermediateType](2))

// Another stage with different concurrency
b3 := fluxus.AddStage(b2, stage2, 
    fluxus.WithAdapterConcurrency[IntermediateType, OutputType](8))
```

### Buffer Sizes

Choose buffer sizes based on your workload characteristics:

- Larger buffers can improve throughput by reducing blocking, but increase memory usage
- Smaller buffers provide tighter backpressure but may increase context-switching overhead
- For bursty workloads, larger buffers help absorb spikes
- For memory-constrained environments, use smaller buffers

```go
// Different buffer sizes for different stages
b2 := fluxus.AddStage(builder, stage1, 
    fluxus.WithAdapterBufferSize[InputType, IntermediateType](500))

b3 := fluxus.AddStage(b2, stage2, 
    fluxus.WithAdapterBufferSize[IntermediateType, OutputType](50))
```

### Reflection Considerations

The stream pipeline implementation uses reflection to create channels with the correct types dynamically. This introduces a small performance overhead, which is usually negligible for most applications.

If you need absolute maximum performance:

1. Measure first to confirm reflection is the bottleneck
2. Consider using code generation to create type-specific pipeline implementations
3. For high-performance sections, implement custom `StreamStage` components without reflection

## Example: Complete Stream Pipeline

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "strings"
    "sync"

    "github.com/synoptiq/go-fluxus"
)

func main() {
    // Set up logger
    logger := log.New(os.Stdout, "StreamPipeline: ", log.LstdFlags)

    // Create stages
    tokenizeStage := fluxus.StageFunc[string, []string](func(_ context.Context, input string) ([]string, error) {
        return strings.Fields(input), nil
    })

    countStage := fluxus.StageFunc[[]string, int](func(_ context.Context, words []string) (int, error) {
        return len(words), nil
    })

    // Build the pipeline
    builder := fluxus.NewStreamPipeline[string](
        fluxus.WithStreamLogger(logger),
        fluxus.WithStreamBufferSize(10),
        fluxus.WithStreamConcurrency(4),
    )
    b2 := fluxus.AddStage(builder, tokenizeStage)
    b3 := fluxus.AddStage(b2, countStage)
    
    pipeline, err := fluxus.Finalize[string, int](b3)
    if err != nil {
        log.Fatalf("Failed to build pipeline: %v", err)
    }

    // Create channels
    source := make(chan string)
    sink := make(chan int)

    // Create a context that can be cancelled
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Run the pipeline
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        // pipeline is *fluxus.StreamPipeline[string, int]
        // source is chan string, sink is chan int
        if err := pipeline.Run(ctx, source, sink); err != nil {
            log.Printf("Pipeline error: %v", err)
        }
    }()

    // Feed data to the source
    go func() {
        defer close(source)
        sentences := []string{
            "Hello world",
            "Stream processing is powerful",
            "Fluxus makes it easy",
            "Type-safe pipelines for the win",
        }
        
        for _, sentence := range sentences {
            select {
            case source <- sentence:
                fmt.Printf("Sent: %s\n", sentence)
            case <-ctx.Done():
                return
            }
        }
    }()

    // Process results from the sink
    for count := range sink {
        fmt.Printf("Word count: %d\n", count)
    }

    // Wait for pipeline completion
    wg.Wait()
    fmt.Println("Pipeline completed")
}
```
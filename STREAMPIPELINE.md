# Fluxus Stream Pipeline Documentation

The Stream Pipeline is an advanced component of Fluxus that enables continuous processing of data streams with backpressure support. It's designed for handling potentially unbounded data sources efficiently.

## Stream Processing Concepts

Unlike standard pipelines that process a single input to produce a single output, stream pipelines:

- Process continuous streams of data items
- Use channels to connect stages
- Support backpressure to handle varying processing rates
- Allow for concurrent processing
- Properly handle cancellation and cleanup

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
)

// Add stages, maintaining type safety
b2 := fluxus.AddStage(builder, stage1)
b3 := fluxus.AddStage(b2, stage2)
b4 := fluxus.AddStage(b3, stage3)

// Finalize the pipeline
pipeline, err := fluxus.Finalize(b4)
if err != nil {
    log.Fatalf("Failed to build pipeline: %v", err)
}
```

## Running Stream Pipelines

To run a stream pipeline, you need to provide source and sink channels:

```go
// Create channels
source := make(chan int)
sink := make(chan OutputType)

// Run the pipeline in a goroutine
var wg sync.WaitGroup
wg.Add(1)
go func() {
    defer wg.Done()
    if err := fluxus.Run(ctx, pipeline, source, sink); err != nil {
        log.Printf("Pipeline error: %v", err)
    }
}()

// Feed data to the source
go func() {
    defer close(source) // Always close the source when done feeding
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

// Process results from the sink
for result := range sink {
    // Do something with the result
    fmt.Println(result)
}

// Wait for pipeline completion
wg.Wait()
```

## Advanced Usage

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
go fluxus.Run(ctx, pipeline, source, sink)

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
    
    pipeline, err := fluxus.Finalize(b3)
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
        if err := fluxus.Run(ctx, pipeline, source, sink); err != nil {
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
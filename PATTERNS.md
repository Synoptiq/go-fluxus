# Fluxus Patterns Documentation

Fluxus provides powerful pattern implementations to solve common data processing challenges. These patterns help structure your pipelines for specific workloads.

## Map

The `Map` pattern applies a stage to each element of an input slice, potentially in parallel.

### Key Features

- Process slice elements concurrently with configurable parallelism
- Collect errors or fail fast on the first error
- Preserves input/output ordering

### Basic Usage

```go
// Create a stage that processes a single item
processItem := fluxus.StageFunc[int, string](func(ctx context.Context, item int) (string, error) {
    return fmt.Sprintf("Item %d", item * 2), nil
})

// Create a Map stage that applies processItem to a slice in parallel
mapStage := fluxus.NewMap(processItem).
    WithConcurrency(runtime.NumCPU())

// Process a slice
inputs := []int{1, 2, 3, 4, 5}
results, err := mapStage.Process(ctx, inputs)
// results would be []string{"Item 2", "Item 4", "Item 6", "Item 8", "Item 10"}
```

### Error Handling Options

```go
// Default: Fail-fast - returns on first error
mapStage := fluxus.NewMap(processItem)

// Collect errors - process all items, return MultiError
mapStage := fluxus.NewMap(processItem).
    WithCollectErrors(true)

// Custom error handler - transform or enrich errors
mapStage := fluxus.NewMap(processItem).
    WithErrorHandler(func(err error) error {
        return fmt.Errorf("map processing failed: %w", err)
    })
```

## Buffer

The `Buffer` pattern processes items in batches for higher efficiency.

### Key Features

- Collects items into batches of a specified size
- Processes the last batch even if smaller than the batch size
- Optimized for memory reuse and reduced allocations

### Basic Usage

```go
// Create a buffer with batch size 10
// The processor function handles entire batches
bufferStage := fluxus.NewBuffer[int, string](10,
    func(ctx context.Context, batch []int) ([]string, error) {
        results := make([]string, 0, len(batch))
        for _, item := range batch {
            results = append(results, fmt.Sprintf("Processed %d", item))
        }
        return results, nil
    })

// Process items
inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
results, err := bufferStage.Process(ctx, inputs)
// Processing happens in two batches: [1-10] and [11-12]
```

### PooledBuffer for Performance

For high-performance scenarios, use `PooledBuffer` to reduce allocations:

```go
pooledBuffer := fluxus.NewPooledBuffer[int, string](10,
    func(ctx context.Context, batch []int) ([]string, error) {
        results := make([]string, 0, len(batch))
        for _, item := range batch {
            results = append(results, fmt.Sprintf("Processed %d", item))
        }
        return results, nil
    },
    fluxus.WithBufferName[int, string]("my-buffer"))
```

## FanOut

The `FanOut` pattern executes multiple stages in parallel for a single input.

### Key Features

- Process one input through multiple stages concurrently
- Collect all outputs in a slice
- Configurable concurrency limit

### Basic Usage

```go
// Create multiple stages
upperStage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
    return strings.ToUpper(input), nil
})

reverseStage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
    runes := []rune(input)
    for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
        runes[i], runes[j] = runes[j], runes[i]
    }
    return string(runes), nil
})

// Create a fan-out with both stages
fanOut := fluxus.NewFanOut(upperStage, reverseStage).
    WithConcurrency(2)  // Optional, defaults to running all stages concurrently

// Process input
results, err := fanOut.Process(ctx, "hello")
// results would be []string{"HELLO", "olleh"}
```

## FanIn

The `FanIn` pattern aggregates results from multiple sources.

### Key Features

- Takes a slice of inputs and produces a single output
- Uses an aggregator function to combine results

### Basic Usage

```go
// Create an aggregator function
aggregator := func(inputs []string) (string, error) {
    return strings.Join(inputs, ", "), nil
}

// Create a fan-in stage
fanIn := fluxus.NewFanIn(aggregator)

// Process multiple inputs
results, err := fanIn.Process(ctx, []string{"hello", "world", "fluxus"})
// result would be "hello, world, fluxus"
```

## Parallel

The `Parallel` pattern combines FanOut and FanIn for parallel processing of a single input through multiple stages.

### Basic Usage

```go
// Create stages for parallel processing
countChars := fluxus.StageFunc[string, int](func(_ context.Context, input string) (int, error) {
    return len(input), nil
})

countWords := fluxus.StageFunc[string, int](func(_ context.Context, input string) (int, error) {
    return len(strings.Fields(input)), nil
})

// Combine results with an aggregator
aggregator := func(results []int) (string, error) {
    return fmt.Sprintf("Characters: %d, Words: %d", results[0], results[1]), nil
}

// Create a parallel stage
parallel := fluxus.Parallel[string, int, string](
    []fluxus.Stage[string, int]{countChars, countWords},
    aggregator,
)

// Process input
result, err := parallel.Process(ctx, "hello world")
// result would be "Characters: 11, Words: 2"
```

## MapReduce

The `MapReduce` pattern implements the classic MapReduce algorithm for distributed data processing.

### Key Concepts

- **Map Phase**: Each input item is processed to emit a key-value pair
- **Shuffle Phase**: Values are grouped by key
- **Reduce Phase**: Each key with all its values is processed into a result

### Basic Usage

```go
// Define a mapper function that emits (word, 1) for each word
mapper := func(ctx context.Context, line string) (fluxus.KeyValue[string, int], error) {
    // Skip empty lines
    word := strings.ToLower(strings.TrimSpace(line))
    if word == "" {
        return fluxus.KeyValue[string, int]{}, nil
    }
    // Emit word and count 1
    return fluxus.KeyValue[string, int]{Key: word, Value: 1}, nil
}

// Define a reducer function that sums counts for each word
reducer := func(ctx context.Context, input fluxus.ReduceInput[string, int]) (fluxus.KeyValue[string, int], error) {
    sum := 0
    for _, count := range input.Values {
        sum += count
    }
    return fluxus.KeyValue[string, int]{Key: input.Key, Value: sum}, nil
}

// Create a MapReduce stage
mapReduceStage := fluxus.NewMapReduce(mapper, reducer).
    WithParallelism(runtime.NumCPU()) // Optional: parallelize the map phase

// Process input lines
lines := []string{"hello world", "hello fluxus", "fluxus example"}
results, err := mapReduceStage.Process(ctx, lines)

// Results would be (order may vary):
// [{Key:"hello", Value:2}, {Key:"world", Value:1}, {Key:"fluxus", Value:2}, {Key:"example", Value:1}]
```

## Flow Control Patterns

### Filter

The `Filter` pattern conditionally passes items through based on a predicate:

```go
// Create a filter for even numbers
evenFilter := fluxus.NewFilter(func(_ context.Context, item int) (bool, error) {
    return item%2 == 0, nil // Keep even numbers
})

// Process - if the item fails the filter, it returns ErrItemFiltered
result, err := evenFilter.Process(ctx, 7)
if errors.Is(err, fluxus.ErrItemFiltered) {
    // Item was filtered out
}

// Typically used in a pipeline where ErrItemFiltered is handled
pipeline := fluxus.NewPipeline(fluxus.Chain(evenFilter, nextStage)).
    WithErrorHandler(func(err error) error {
        if errors.Is(err, fluxus.ErrItemFiltered) {
            return nil // Suppress the error
        }
        return err
    })
```

### Router

The `Router` pattern sends an input to one or more downstream stages based on a selector function:

```go
// Create stages for different processing paths
stageA := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
    return "Path A: " + input, nil
})

stageB := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
    return "Path B: " + input, nil
})

// Create a router with a selector function
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
        return nil, nil // No match - will return ErrNoRouteMatched
    },
    fluxus.Route[string, string]{Name: "Processor A", Stage: stageA},
    fluxus.Route[string, string]{Name: "Processor B", Stage: stageB},
).WithConcurrency(2)

// Process an item
results, err := router.Process(ctx, "BOTH:test")
// results would be ["Path A: BOTH:test", "Path B: BOTH:test"]

// If no routes match
_, err = router.Process(ctx, "C:test")
if errors.Is(err, fluxus.ErrNoRouteMatched) {
    // Handle no matching routes
}
```

### JoinByKey

The `JoinByKey` pattern groups items from a slice by a key:

```go
// Define a type with a grouping key
type Result struct {
    GroupID int
    Data    string
}

// Create a joiner that extracts GroupID as the key
joiner := fluxus.NewJoinByKey(func(_ context.Context, item Result) (int, error) {
    return item.GroupID, nil
})

// Group items
items := []Result{
    {GroupID: 1, Data: "a"},
    {GroupID: 2, Data: "b"},
    {GroupID: 1, Data: "c"},
}

groups, err := joiner.Process(ctx, items)
// groups would be map[int][]Result{
//   1: {{GroupID:1, Data:"a"}, {GroupID:1, Data:"c"}},
//   2: {{GroupID:2, Data:"b"}},
// }
```

## Combining Patterns

One of the strengths of Fluxus is that patterns can be combined to build complex pipelines:

### Example: ETL Pipeline

```go
// 1. Extract stage - read records from a data source
extractStage := fluxus.StageFunc[string, []Record](/* ... */)

// 2. Transform stage - process the records in parallel using Map
transformSingleRecord := fluxus.StageFunc[Record, ProcessedRecord](/* ... */)
transformStage := fluxus.NewMap(transformSingleRecord).
    WithConcurrency(runtime.NumCPU())

// 3. Filter stage - remove invalid records
filterStage := fluxus.NewFilter(func(_ context.Context, record ProcessedRecord) (bool, error) {
    return record.IsValid(), nil
})

// 4. Load stage - buffer records for batch insertion
loadStage := fluxus.NewBuffer[ProcessedRecord, DBResult](10,
    func(ctx context.Context, batch []ProcessedRecord) ([]DBResult, error) {
        return batchInsert(ctx, batch)
    })

// Chain everything together
pipeline := fluxus.NewPipeline(
    fluxus.Chain(extractStage,
        fluxus.Chain(transformStage,
            fluxus.Chain(filterStage,
                loadStage))))

// Custom error handling
pipeline.WithErrorHandler(func(err error) error {
    if errors.Is(err, fluxus.ErrItemFiltered) {
        // Ignore filtered items
        return nil
    }
    return err
})

// Process the pipeline
results, err := pipeline.Process(ctx, sourceData)
```

### Example: Parallel API Aggregation

```go
// Create stages for different API calls
weatherAPI := fluxus.StageFunc[string, WeatherData](/* ... */)
trafficAPI := fluxus.StageFunc[string, TrafficData](/* ... */)
eventsAPI := fluxus.StageFunc[string, EventsData](/* ... */)

// Combine with Parallel pattern
aggregator := func(results []any) (DashboardData, error) {
    weather := results[0].(WeatherData)
    traffic := results[1].(TrafficData)
    events := results[2].(EventsData)
    
    return DashboardData{
        Weather: weather,
        Traffic: traffic,
        Events:  events,
    }, nil
}

// Create stages with adapters for common output type
parallelStage := fluxus.Parallel[string, any, DashboardData](
    []fluxus.Stage[string, any]{
        adaptToAny(weatherAPI),
        adaptToAny(trafficAPI),
        adaptToAny(eventsAPI),
    },
    aggregator,
)

// Process in parallel for a location
dashboard, err := parallelStage.Process(ctx, "New York")
```

## Best Practices

### Pattern Selection Guidelines

- Use `Map` when you need to apply the same operation to many items
- Use `Buffer` for efficient batch processing
- Use `FanOut` when you need different operations on the same input
- Use `FanIn` when you need to combine multiple inputs
- Use `Parallel` for parallel processing with result aggregation
- Use `MapReduce` for complex data transformations with grouping
- Use `Filter` for conditional processing
- Use `Router` for dynamic routing

### Performance Considerations

- Choose the right concurrency level by benchmarking your specific workload
- For CPU-bound tasks, `runtime.NumCPU()` is usually optimal
- For I/O-bound tasks, higher concurrency can be more efficient
- Buffer sizes should be tuned based on memory constraints and throughput needs
- Be mindful of memory usage in MapReduce with large datasets
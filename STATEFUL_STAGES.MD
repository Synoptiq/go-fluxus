# Building Stateful Stages in Fluxus

While many pipeline stages can be stateless (their output depends solely on their current input and configuration), some stages need to maintain state across multiple `Process` calls or across pipeline runs. This state might include:

- Cached data to avoid redundant computations or I/O.
- Aggregated values or statistics.
- Connections to external resources (e.g., database connections, clients).
- Configuration loaded at startup.

Building stateful stages requires careful consideration of:

 - **Thread Safety:** If a stage is used in a concurrent environment (like `StreamAdapter` or custom parallel processing), its internal state must be accessed in a thread-safe manner.
- **Initialization:** State often needs to be initialized once when the stage (or pipeline) starts. This initialization might be fallible.
- **Cleanup:** Resources held by the state (e.g., network connections) might need to be released when the stage (or pipeline) stops.
- **Resetting State:** For long-lived pipelines or for testing, it's often useful to reset a stage's state to its initial condition without recreating the entire pipeline.

Fluxus provides the `StatefulStageHelper` utility to simplify the creation of robust stateful stages by addressing these common concerns.

## The `StatefulStageHelper`

The `StatefulStageHelper` is a struct designed to be embedded into your custom stage implementations. It provides:

- A `sync.RWMutex` for thread-safe access to custom state defined within your stage.
- A mechanism for one-time, fallible initialization (`DoOnceWithError`).
- An internal, thread-safe key-value store for managing dynamic state.

### Embedding and Initialization

Embed `StatefulStageHelper` into your stage struct:

```go
import (
    "context"
    "fmt"
    "sync" // For custom state, if needed beyond the helper's map
    "time"

    "github.com/synoptiq/go-fluxus"
)

type MyStatefulStage struct {
    fluxus.StatefulStageHelper // Embed the helper

    // Custom state specific to this stage, if any,
    // can be protected by helper.Lock/Unlock
    customCounter int
    configValue   string
}

// NewMyStatefulStage creates an instance of MyStatefulStage.
func NewMyStatefulStage() *MyStatefulStage {
    return &MyStatefulStage{
        StatefulStageHelper: *fluxus.NewStatefulStageHelper(), // Initialize the helper
        // Initialize other fields
    }
}
```

While the helper's methods are designed to work safely if it's embedded as a zero value (it will lazily initialize its internal map), using the `fluxus.NewStatefulStageHelper()` constructor as shown above is recommended for clarity and explicit initialization.

### API Overview

#### 1. Mutex for Custom State

If your stage has state fields beyond what you store in the helper's internal map, you can use the helper's mutex to protect them:

- `Lock()`: Acquires a write lock.
- `Unlock()`: Releases a write lock.
- `RLock()`: Acquires a read lock.
- `RUnlock()`: Releases a read lock.

```go
func (s *MyStatefulStage) IncrementCustomCounter() {
    s.Lock() // Acquire write lock from StatefulStageHelper
    defer s.Unlock()
    s.customCounter++
}

func (s *MyStatefulStage) GetCustomCounter() int {
    s.RLock() // Acquire read lock
    defer s.RUnlock()
    return s.customCounter
}
```

#### 2. One-time Initialization (`DoOnceWithError` and `GetSetupError`)

Often, a stage needs to perform setup tasks (e.g., loading configuration, establishing connections) exactly once when it starts. The `DoOnceWithError` method facilitates this, especially when the setup can fail.

- `DoOnceWithError(f func() error) error`: Executes the function f exactly once. If f returns an error, this error is stored and returned by all subsequent calls to DoOnceWithError. If f succeeds, subsequent calls return nil without re-executing f.
- `GetSetupError()` error: Returns the error captured by DoOnceWithError, if any.

This is ideal for implementing the `fluxus.Initializer` interface:


```go
// MyStatefulStage implementing fluxus.Initializer
func (s *MyStatefulStage) Setup(ctx context.Context) error {
    return s.DoOnceWithError(func() error {
        // This block runs only once
        s.Lock() // Protect access to s.configValue if needed
        defer s.Unlock()

        fmt.Println("MyStatefulStage: Performing one-time setup...")
        // Simulate loading configuration that might fail
        if time.Now().Nanosecond()%2 == 0 { // Simulate potential failure
            s.configValue = "default_from_setup"
            // Initialize internal state via helper
            s.SetState("initial_timestamp", time.Now())
            return nil
        }
        return fmt.Errorf("simulated setup failure")
    })
}
```

#### 3. Internal Key-Value State Store

The `StatefulStageHelper` provides a built-in, thread-safe map to store arbitrary key-value pairs. This is useful for dynamic state that doesn't need to be a fixed field on your stage struct.

- `SetState(key string, value any)`: Stores a value associated with a key.
- `GetState(key string) (any, bool)`: Retrieves a value and a boolean indicating if the key exists.
- `DeleteState(key string)`: Removes a key-value pair.
- `ClearAllState()`: Removes all key-value pairs from the internal map. This is very useful for implementing `fluxus.Resettable`. Note: This does not reset the error state from `DoOnceWithError`.
- `GetAllState()` map[string]any: Returns a shallow copy of the internal state map.


```go
func (s *MyStatefulStage) Process(ctx context.Context, input string) (string, error) {
    // Ensure setup has run successfully
    if err := s.GetSetupError(); err != nil {
        return "", fmt.Errorf("stage not initialized: %w", err)
    }
    // Or, if Setup is guaranteed by pipeline.Start:
    // if s.GetSetupError() != nil { /* ... */ }

    s.Lock()
    config := s.configValue // Access custom state
    s.Unlock()

    // Use internal state
    s.SetState("last_input", input)
    s.SetState("process_count", s.getProcessCount()+1)

    processedValue := fmt.Sprintf("Input: %s, Config: %s, Count: %d",
        input, config, s.getProcessCount())

    if val, ok := s.GetState("initial_timestamp"); ok {
        if ts, okTs := val.(time.Time); okTs {
            processedValue += fmt.Sprintf(", Initialized: %s", ts.Format(time.RFC3339))
        }
    }
    return processedValue, nil
}

func (s *MyStatefulStage) getProcessCount() int {
    s.RLock() // RLock because GetState itself is thread-safe, but we might do more.
              // Or, if only accessing helper's state, RLock might not be needed around GetState.
              // Here, it's more for demonstrating combined access.
    defer s.RUnlock()
    val, ok := s.GetState("process_count")
    if !ok {
        return 0
    }
    count, _ := val.(int) // Add type assertion error handling in real code
    return count
}

// MyStatefulStage implementing fluxus.Resettable
func (s *MyStatefulStage) Reset(ctx context.Context) error {
    s.Lock()
    defer s.Unlock()

    fmt.Println("MyStatefulStage: Resetting state...")
    s.customCounter = 0
    s.configValue = "" // Reset custom fields

    s.ClearAllState() // Clear the helper's internal key-value store

    // Note: Reset does not automatically re-run DoOnceWithError.
    // If re-initialization is needed after a reset, the stage or
    // pipeline logic must handle calling Setup again if appropriate.
    // For StatefulStageHelper, setupError remains. If you need to clear it,
    // you'd need a custom method or re-instantiate the helper/stage.
    return nil
}

// MyStatefulStage implementing fluxus.Closer
func (s *MyStatefulStage) Close(ctx context.Context) error {
    s.Lock()
    defer s.Unlock()
    fmt.Println("MyStatefulStage: Closing resources...")
    // Perform any cleanup of resources held by the stage
    // e.g., close network connections, flush buffers
    s.DeleteState("last_input") // Example cleanup
    return nil
}

// MyStatefulStage implementing fluxus.HealthCheckable
func (s *MyStatefulStage) HealthStatus(ctx context.Context) error {
    if err := s.GetSetupError(); err != nil {
        return fmt.Errorf("stage setup failed: %w", err)
    }
    // Add more specific health checks if needed
    // For example, check connection to an external service
    fmt.Println("MyStatefulStage: Health check OK.")
    return nil
}
```

## Example: Using the Stateful Stage

```go
func main() {
    stage := NewMyStatefulStage()
    ctx := context.Background()

    // Simulate pipeline lifecycle
    fmt.Println("--- Initializing Stage ---")
    if err := stage.Setup(ctx); err != nil {
        fmt.Printf("Initial setup failed: %v\n", err)
        // In a real pipeline, this might halt startup
    } else {
        fmt.Println("Initial setup successful.")
    }

    // Subsequent calls to Setup will use the DoOnceWithError mechanism
    fmt.Println("\n--- Second Setup Call (should be no-op or return stored error) ---")
    if err := stage.Setup(ctx); err != nil {
        fmt.Printf("Second setup call reported error: %v\n", err)
    } else {
        fmt.Println("Second setup call successful (no re-execution).")
    }

    fmt.Println("\n--- Processing ---")
    if stage.GetSetupError() == nil { // Only process if setup was okay
        output, err := stage.Process(ctx, "hello")
        if err != nil {
            fmt.Printf("Error processing: %v\n", err)
        } else {
            fmt.Printf("Processed: %s\n", output)
        }

        output, err = stage.Process(ctx, "world")
        if err != nil {
            fmt.Printf("Error processing: %v\n", err)
        } else {
            fmt.Printf("Processed: %s\n", output)
        }
    } else {
        fmt.Println("Skipping processing due to setup failure.")
    }


    fmt.Println("\n--- Checking State ---")
    allState := stage.GetAllState()
    fmt.Println("All internal states:")
    for k, v := range allState {
        fmt.Printf("  %s: %v\n", k, v)
    }
    fmt.Printf("Custom counter: %d\n", stage.GetCustomCounter())


    fmt.Println("\n--- Checking Health ---")
    if err := stage.HealthStatus(ctx); err != nil {
        fmt.Printf("Health check failed: %v\n", err)
    } else {
        fmt.Println("Health check successful.")
    }

    fmt.Println("\n--- Resetting Stage ---")
    if err := stage.Reset(ctx); err != nil {
        fmt.Printf("Error resetting stage: %v\n", err)
    }
    allStateAfterReset := stage.GetAllState()
    fmt.Println("All internal states after reset:")
    for k, v := range allStateAfterReset {
        fmt.Printf("  %s: %v\n", k, v)
    }
    fmt.Printf("Custom counter after reset: %d\n", stage.GetCustomCounter())
    // Note: setupError from DoOnceWithError is NOT cleared by Reset.
    // If setup failed initially, GetSetupError() will still return that error.

    fmt.Println("\n--- Closing Stage ---")
    if err := stage.Close(ctx); err != nil {
        fmt.Printf("Error closing stage: %v\n", err)
    }
    fmt.Println("Stage closed.")
}
```

By embedding `StatefulStageHelper` and leveraging its features, you can significantly reduce the boilerplate and complexity associated with building correct and robust stateful stages in your Fluxus pipelines. Remember to combine its use with the lifecycle interfaces (`Initializer`, `Closer`, `Resettable`, `HealthCheckable`) for full integration with the pipeline's management capabilities.

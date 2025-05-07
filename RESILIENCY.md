# Fluxus Resiliency Documentation

Building resilient data pipelines is crucial for production systems. Fluxus provides several components to handle failures gracefully, limit the impact of errors, and ensure your pipelines recover automatically from transient issues.

## Circuit Breaker

The Circuit Breaker pattern prevents cascading failures by automatically stopping calls to a failing service. It's inspired by electrical circuit breakers and helps your system fail fast and recover gracefully.

### Key Features

- Prevents overloading failing services
- Automatically detects when a service recovers
- Three circuit states: Closed (normal), Open (preventing calls), HalfOpen (testing recovery)

### Basic Usage

```go
// Create a circuit breaker around a stage
circuitBreaker := fluxus.NewCircuitBreaker(
    stage,            // The stage to protect
    3,                // Failure threshold - open circuit after 3 consecutive failures
    5*time.Second,    // Reset timeout - wait this long before testing if service recovered
)

// Optional configurations
circuitBreaker := fluxus.NewCircuitBreaker(
    stage, 
    3, 
    5*time.Second,
    fluxus.WithSuccessThreshold[Input, Output](2),        // Require 2 consecutive successes to close circuit
    fluxus.WithHalfOpenMaxRequests[Input, Output](3),     // Allow 3 test requests when half-open
)

// Use the circuit breaker
result, err := circuitBreaker.Process(ctx, input)
if errors.Is(err, fluxus.ErrCircuitOpen) {
    // Circuit is open, handle accordingly
}
```

### Circuit States

1. **Closed** (normal state):
   - All requests pass through to the protected stage
   - Failures are counted
   - When failures exceed threshold, circuit opens

2. **Open** (protective state):
   - All requests are immediately rejected with `ErrCircuitOpen`
   - After reset timeout, circuit transitions to half-open

3. **Half-Open** (testing state):
   - Limited requests allowed through to test if underlying service has recovered
   - On success, success counter increments
   - When success threshold is met, circuit closes
   - On failure, circuit immediately opens again

### Monitoring and Management

```go
// Check current state
state := circuitBreaker.State()
if state == fluxus.CircuitOpen {
    log.Println("Circuit is currently open")
}

// Get the last error that caused the circuit to open
lastErr := circuitBreaker.LastError()

// Force reset the circuit
circuitBreaker.Reset()
```

## Retry

The Retry pattern automatically retries failed operations with configurable strategies.

### Key Features

- Configurable maximum retry attempts
- Optional backoff strategy to prevent overwhelming systems
- Predicate function to determine which errors should trigger retries
- Custom error handling

### Basic Usage

```go
// Create a retry stage with max 3 attempts
retryStage := fluxus.NewRetry(stage, 3)

// Add a backoff strategy (exponential backoff)
retryStage.WithBackoff(func(attempt int) int {
    // Return delay in milliseconds
    // attempt is 0-based for retries (0 = first retry, etc.)
    return 100 * (1 << attempt) // 100ms, 200ms, 400ms, etc.
})

// Only retry specific errors
retryStage.WithShouldRetry(func(err error) bool {
    // Only retry temporary network errors
    var netErr net.Error
    return errors.As(err, &netErr) && netErr.Temporary()
})

// Custom error handler
retryStage.WithErrorHandler(func(err error) error {
    return fmt.Errorf("all retry attempts failed: %w", err)
})

// Process with retry
result, err := retryStage.Process(ctx, input)
```

### Backoff Strategies

Different backoff strategies for different scenarios:

```go
// Constant backoff - always wait the same amount of time
retryStage.WithBackoff(func(attempt int) int {
    return 100 // Always wait 100ms
})

// Linear backoff - increase wait time linearly
retryStage.WithBackoff(func(attempt int) int {
    return 100 * (attempt + 1) // 100ms, 200ms, 300ms, etc.
})

// Exponential backoff - increase wait time exponentially (good for network issues)
retryStage.WithBackoff(func(attempt int) int {
    return 100 * (1 << attempt) // 100ms, 200ms, 400ms, 800ms, etc.
})

// Exponential backoff with jitter - prevents thundering herd
retryStage.WithBackoff(func(attempt int) int {
    base := 100 * (1 << attempt)
    jitter := rand.Intn(base / 2)
    return base + jitter
})
```

## Timeout

The Timeout pattern sets a maximum duration for a stage to complete.

### Key Features

- Prevents stages from blocking indefinitely
- Integrates with context cancellation
- Custom error handling

### Basic Usage

```go
// Create a timeout stage with a 5-second timeout
timeoutStage := fluxus.NewTimeout(stage, 5*time.Second)

// Add custom error handling
timeoutStage.WithErrorHandler(func(err error) error {
    if errors.Is(err, context.DeadlineExceeded) {
        return fmt.Errorf("operation timed out: %w", err)
    }
    return err
})

// Process with timeout
result, err := timeoutStage.Process(ctx, input)
if errors.Is(err, context.DeadlineExceeded) {
    // Handle timeout
}
```

## Dead Letter Queue (DLQ)

The Dead Letter Queue pattern captures items that consistently fail processing, allowing for later analysis or reprocessing.

### Key Features

- Separates error handling from normal flow
- Allows operation to continue despite some failures
- Configurable error filtering
- Custom error logging

### Basic Usage

```go
// Create a DLQ handler
dlqHandler := fluxus.DLQHandlerFunc[InputType](func(ctx context.Context, item InputType, processingError error) error {
    // Log the error
    log.Printf("[DLQ] Failed to process item: %v, error: %v", item, processingError)
    
    // Save to persistent storage
    return saveToDLQ(item, processingError)
})

// Wrap a stage with DLQ handling
dlqStage := fluxus.NewDeadLetterQueue(
    stage,
    fluxus.WithDLQHandler[InputType, OutputType](dlqHandler),
)

// Custom error filtering - only send certain errors to DLQ
dlqStage := fluxus.NewDeadLetterQueue(
    stage,
    fluxus.WithDLQHandler[InputType, OutputType](dlqHandler),
    fluxus.WithShouldDLQ[InputType, OutputType](func(err error) bool {
        // Only send certain errors to DLQ
        return !errors.Is(err, context.Canceled) &&
               !errors.Is(err, fluxus.ErrItemFiltered)
    }),
)

// Custom error logging for the DLQ handler itself
dlqStage := fluxus.NewDeadLetterQueue(
    stage,
    fluxus.WithDLQHandler[InputType, OutputType](dlqHandler),
    fluxus.WithDLQErrorLogger[InputType, OutputType](func(err error) {
        // Log errors that occur within the DLQ handler
        myCustomLogger.Errorf("DLQ handler error: %v", err)
    }),
)
```

### Implementation Patterns

Common DLQ implementation patterns:

```go
// 1. Log-only DLQ
logDLQ := fluxus.DLQHandlerFunc[InputType](func(ctx context.Context, item InputType, err error) error {
    log.Printf("[DLQ] Item %v failed: %v", item, err)
    return nil
})

// 2. Database DLQ
dbDLQ := fluxus.DLQHandlerFunc[InputType](func(ctx context.Context, item InputType, err error) error {
    return db.Insert(ctx, "failed_items", ItemError{
        Item:      item,
        Error:     err.Error(),
        Timestamp: time.Now(),
    })
})

// 3. Message queue DLQ
mqDLQ := fluxus.DLQHandlerFunc[InputType](func(ctx context.Context, item InputType, err error) error {
    return mqClient.Send(ctx, "dlq-topic", ItemErrorMessage{
        Item:      item,
        Error:     err.Error(),
        Timestamp: time.Now(),
    })
})

// 4. File system DLQ
fsDLQ := fluxus.DLQHandlerFunc[InputType](func(ctx context.Context, item InputType, err error) error {
    itemJSON, err := json.Marshal(ItemError{
        Item:      item,
        Error:     err.Error(),
        Timestamp: time.Now(),
    })
    if err != nil {
        return err
    }
    return os.WriteFile(fmt.Sprintf("dlq/%s.json", uuid.New()), itemJSON, 0644)
})
```

## Rate Limiter

The Rate Limiter pattern controls the rate of requests to a downstream service to prevent overloading it.

### Key Features

- Controls requests per second and burst capacity
- Timeout option to fail fast if rate limited
- Dynamic adjustment of limits

### Basic Usage

```go
// Create a rate limiter with 10 requests per second and burst of 5
limiter := fluxus.NewRateLimiter(
    stage,
    rate.Limit(10),  // 10 requests/second
    5,               // Burst of 5
    fluxus.WithLimiterTimeout[InputType, OutputType](100*time.Millisecond),  // Timeout after 100ms waiting
)

// Dynamically adjust rate limits
limiter.SetLimit(rate.Limit(5))  // Change to 5 requests/second
limiter.SetBurst(10)             // Change burst to 10

// Check if a request would be allowed without blocking
if limiter.Allow() {
    // Request would be allowed
}

// Process with rate limiting
result, err := limiter.Process(ctx, input)
if errors.Is(err, context.DeadlineExceeded) {
    // Rate limit timeout exceeded
}
```

## Combining Resiliency Patterns

Multiple resiliency patterns can be combined for comprehensive protection:

```go
// Start with the base stage
baseStage := fluxus.StageFunc[Input, Output](/* ... */)

// Add retries
withRetry := fluxus.NewRetry(baseStage, 3).
    WithBackoff(func(attempt int) int {
        return 100 * (1 << attempt)
    })

// Add timeout
withTimeout := fluxus.NewTimeout(withRetry, 5*time.Second)

// Add circuit breaker
withCircuitBreaker := fluxus.NewCircuitBreaker(withTimeout, 5, 30*time.Second)

// Add rate limiting
withRateLimiting := fluxus.NewRateLimiter(withCircuitBreaker, rate.Limit(50), 10)

// Add DLQ
dlqHandler := fluxus.DLQHandlerFunc[Input](/* ... */)
withDLQ := fluxus.NewDeadLetterQueue(
    withRateLimiting,
    fluxus.WithDLQHandler[Input, Output](dlqHandler),
)

// Finally create the pipeline
pipeline := fluxus.NewPipeline(withDLQ)

// Process with full resiliency
result, err := pipeline.Process(ctx, input)
```

## Best Practices

### Pattern Selection Guidelines

1. **Start with the innermost protection:**
   - Start with retry for transient failures
   - Add timeout to prevent indefinite blocking
   - Add circuit breaker to prevent overwhelming failing services
   - Add rate limiting to control throughput
   - Add DLQ to capture persistent failures

2. **Configure timeouts appropriately:**
   - Shorter than any client timeout
   - Long enough for normal operation
   - Consider timeouts at different levels

3. **Configure retries carefully:**
   - Don't retry non-idempotent operations
   - Use appropriate backoff strategy
   - Limit total retry attempts
   - Consider retry budgets for the system

4. **Circuit breaker thresholds:**
   - Set based on normal error rates
   - Consider service criticality
   - Monitor circuit state changes

5. **Rate limits:**
   - Based on downstream service capacity
   - Consider burst capacity for spikes
   - Be conservative if unsure

### Observable Resilience

Make your resilience patterns observable:

```go
// Add metrics to circuit breaker
metricated := fluxus.NewMetricatedStage(
    circuitBreaker,
    fluxus.WithMetricsStageName[Input, Output]("service-circuit-breaker"),
)

// Add tracing to retry
traced := fluxus.NewTracedRetry(
    retry,
    "service-retry",
    attribute.Int("max_attempts", 3),
)
```

### Testing Resilience

Test your resilience patterns by introducing failures:

```go
// Create a failure-injecting stage for testing
flakeyStage := fluxus.StageFunc[Input, Output](func(ctx context.Context, input Input) (Output, error) {
    // Inject random failures
    if rand.Float32() < 0.3 {
        return Output{}, errors.New("injected random failure")
    }
    
    // Inject occasional slow responses
    if rand.Float32() < 0.2 {
        time.Sleep(2 * time.Second)
    }
    
    return realStage.Process(ctx, input)
})

// Wrap with resilience patterns and test
resilientStage := createResilientStage(flakeyStage)
```
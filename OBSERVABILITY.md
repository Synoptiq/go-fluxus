# Fluxus Observability Documentation

Proper observability is essential for understanding the behavior and performance of data processing pipelines in production. Fluxus provides built-in support for metrics collection and distributed tracing to help you monitor and troubleshoot your pipelines.

## Metrics Collection

Fluxus includes a metrics collection system that can integrate with various monitoring solutions.

### MetricsCollector Interface

The `MetricsCollector` interface defines methods for recording various pipeline events:

```go
type MetricsCollector interface {
	// --- General Stage Metrics (Apply to both Pipeline and StreamPipeline stages) ---

	// StageStarted is called when a stage's Process method begins execution.
	StageStarted(ctx context.Context, stageName string)
	// StageCompleted is called when a stage's Process method completes successfully.
	StageCompleted(ctx context.Context, stageName string, duration time.Duration)
	// StageError is called when a stage's Process method returns an error.
	StageError(ctx context.Context, stageName string, err error)

	// --- Specific Stage Type Metrics (Apply wherever these stage types are used) ---

	// RetryAttempt is called for each retry attempt within a Retry stage.
	RetryAttempt(ctx context.Context, stageName string, attempt int, err error)
	// BufferBatchProcessed is called when a batch is processed by a Buffer stage.
	BufferBatchProcessed(ctx context.Context, batchSize int, duration time.Duration)
	// FanOutStarted is called when a FanOut stage begins processing.
	FanOutStarted(ctx context.Context, stageName string, numBranches int)
	// FanOutCompleted is called when a FanOut stage completes processing.
	FanOutCompleted(ctx context.Context, stageName string, numBranches int, duration time.Duration)
	// FanInStarted is called when a FanIn stage begins processing.
	FanInStarted(ctx context.Context, stageName string, numInputs int)
	// FanInCompleted is called when a FanIn stage completes processing.
	FanInCompleted(ctx context.Context, stageName string, numInputs int, duration time.Duration)

	// --- Stream Pipeline Lifecycle Metrics ---

	// PipelineStarted is called when a StreamPipeline's Run method begins execution.
	PipelineStarted(ctx context.Context, pipelineName string)
	// PipelineCompleted is called when a StreamPipeline's Run method finishes.
	PipelineCompleted(ctx context.Context, pipelineName string, duration time.Duration, err error)

	// --- Stream Worker Metrics (Specific to how stages run within StreamPipeline) ---

	// StageWorkerConcurrency reports the configured concurrency for a stage within the stream.
	StageWorkerConcurrency(ctx context.Context, stageName string, concurrencyLevel int)
	// StageWorkerItemProcessed reports successful processing of an item by a stream worker.
	StageWorkerItemProcessed(ctx context.Context, stageName string, duration time.Duration)
	// StageWorkerItemSkipped reports an item skipped due to an error with SkipOnError strategy.
	StageWorkerItemSkipped(ctx context.Context, stageName string, err error)
	// StageWorkerErrorSent reports an error sent to an error channel via SendToErrorChannel strategy.
	StageWorkerErrorSent(ctx context.Context, stageName string, err error)

	// --- Windowing Stage Metrics ---

	// WindowEmitted is called when a windowing stage successfully emits a window.
	WindowEmitted(ctx context.Context, stageName string, itemCountInWindow int)

	// --- Timeout Stage Metrics ---

	// TimeoutOccurred is called when a stage times out.
	TimeoutOccurred(ctx context.Context, stageName string, configuredTimeout time.Duration, actualDuration time.Duration)

	// --- Circuit Breaker Stage Metrics ---

	// CircuitStateChanged is called when circuit breaker changes state.
	CircuitStateChanged(ctx context.Context, stageName string, fromState, toState string)
	// CircuitBreakerRejected is called when a request is rejected due to open circuit.
	CircuitBreakerRejected(ctx context.Context, stageName string)
	// CircuitBreakerFailureRecorded is called when a failure is recorded.
	CircuitBreakerFailureRecorded(ctx context.Context, stageName string, failureCount int, threshold int)
	// CircuitBreakerSuccessRecorded is called when success is recorded in half-open state.
	CircuitBreakerSuccessRecorded(ctx context.Context, stageName string, successCount int, threshold int)

	// --- Dead Letter Queue Stage Metrics ---

	// DeadLetterQueueItemSent is called when an item is sent to DLQ.
	DeadLetterQueueItemSent(ctx context.Context, stageName string, originalError error)
	// DeadLetterQueueHandlerError is called when DLQ handler itself fails.
	DeadLetterQueueHandlerError(ctx context.Context, stageName string, dlqError error)

	// --- Router Stage Metrics ---

	// RouterRoutesSelected is called after route selection.
	RouterRoutesSelected(ctx context.Context, stageName string, numRoutesSelected int, totalRoutes int)
	// RouterNoRouteMatched is called when no routes match.
	RouterNoRouteMatched(ctx context.Context, stageName string)
	// RouterRouteProcessed is called for each successfully processed route.
	RouterRouteProcessed(ctx context.Context, stageName string, routeName string, routeIndex int, duration time.Duration)

	// --- Map Stage Metrics ---

	// MapItemProcessed is called for each successfully processed item in Map stage.
	MapItemProcessed(ctx context.Context, stageName string, itemIndex int, duration time.Duration)
	// MapItemError is called for each item that fails processing in Map stage.
	MapItemError(ctx context.Context, stageName string, itemIndex int, err error)
	// MapConcurrencyLevel reports the actual concurrency used for a Map operation.
	MapConcurrencyLevel(ctx context.Context, stageName string, concurrencyLevel int, totalItems int)

	// --- MapReduce Stage Metrics ---

	// MapReduceMapPhaseCompleted is called when map phase completes.
	MapReduceMapPhaseCompleted(ctx context.Context, stageName string, numItems int, numKeys int, duration time.Duration)
	// MapReduceShufflePhaseCompleted is called when shuffle phase completes.
	MapReduceShufflePhaseCompleted(ctx context.Context, stageName string, numKeys int, duration time.Duration)
	// MapReduceReducePhaseCompleted is called when reduce phase completes.
	MapReduceReducePhaseCompleted(ctx context.Context, stageName string, numKeys int, numResults int, duration time.Duration)
	// MapReduceKeyGroupSize reports the size of value groups per key.
	MapReduceKeyGroupSize(ctx context.Context, stageName string, key string, groupSize int)

	// --- Filter Stage Metrics ---

	// FilterItemPassed is called when an item passes the filter.
	FilterItemPassed(ctx context.Context, stageName string)
	// FilterItemDropped is called when an item is filtered out.
	FilterItemDropped(ctx context.Context, stageName string)
	// FilterPredicateError is called when the predicate function returns an error.
	FilterPredicateError(ctx context.Context, stageName string, err error)

	// --- JoinByKey Stage Metrics ---

	// JoinByKeyGroupCreated is called for each unique key found.
	JoinByKeyGroupCreated(ctx context.Context, stageName string, keyStr string, groupSize int)
	// JoinByKeyCompleted is called when join operation completes.
	JoinByKeyCompleted(ctx context.Context, stageName string, numKeys int, totalItems int, duration time.Duration)

	// --- Custom Stage Metrics ---

	// CustomStageMetric is a generic metric for custom stages.
	// Custom stage implementations can call this method directly to report stage-specific metrics.
	// Example: collector.CustomStageMetric(ctx, "my_custom_stage", "items_processed", 42)
	CustomStageMetric(ctx context.Context, stageName string, metricName string, value interface{})
	// CustomStageEvent is called for notable events in custom stages.
	// Custom stage implementations can call this method to report significant events.
	// Example: collector.CustomStageEvent(ctx, "my_custom_stage", "cache_miss", map[string]interface{}{"key": "user:123"})
	CustomStageEvent(ctx context.Context, stageName string, eventName string, metadata map[string]interface{})
}
```

### Adding Metrics to Stages

Use the `MetricatedStage` wrapper to add metrics collection to any stage:

```go
// Create a metricated stage wrapper
metricated := fluxus.NewMetricatedStage(
    stage,
    fluxus.WithMetricsStageName[Input, Output]("my-processing-stage"),
    fluxus.WithMetricsCollector[Input, Output](collector),
)

// Now use the metricated stage in your pipeline
pipeline := fluxus.NewPipeline(metricated)
```

### Specialized Metricated Wrappers

Fluxus provides specialized metricated wrappers for specific stage types that collect additional metrics:

```go
// Metricated Pipeline
metricatedPipeline := fluxus.NewMetricatedPipeline(
    pipeline,
    fluxus.WithPipelineName[Input, Output]("etl-pipeline"),
)

// Metricated FanOut
metricatedFanOut := fluxus.NewMetricatedFanOut(
    fanOut,
    fluxus.WithMetricsStageName[Input, []Output]("parallel-processing"),
)

// Metricated FanIn
metricatedFanIn := fluxus.NewMetricatedFanIn(
    fanIn,
    fluxus.WithMetricsStageName[[]Input, Output]("result-aggregation"),
)

// Metricated Buffer
metricatedBuffer := fluxus.NewMetricatedBuffer(
    buffer,
    fluxus.WithMetricsStageName[[]Input, []Output]("batch-processor"),
)

// Metricated Retry
metricatedRetry := fluxus.NewMetricatedRetry(
    retry,
    fluxus.WithMetricsStageName[Input, Output]("resilient-operation"),
)

// Metricated Timeout
metricatedTimeout := fluxus.NewMetricatedTimeout(
    timeout,
    fluxus.WithMetricsStageName[Input, Output]("timeout-protected"),
)

// Metricated CircuitBreaker
metricatedCircuitBreaker := fluxus.NewMetricatedCircuitBreaker(
    circuitBreaker,
    fluxus.WithMetricsStageName[Input, Output]("resilient-service"),
)

// Metricated DeadLetterQueue
metricatedDLQ := fluxus.NewMetricatedDeadLetterQueue(
    dlq,
    fluxus.WithMetricsStageName[Input, Output]("error-handler"),
)

// Metricated Router
metricatedRouter := fluxus.NewMetricatedRouter(
    router,
    fluxus.WithMetricsStageName[Input, []Output]("conditional-routing"),
)

// Metricated Map
metricatedMap := fluxus.NewMetricatedMap(
    mapStage,
    fluxus.WithMetricsStageName[[]Input, []Output]("parallel-transform"),
)

// Metricated MapReduce
metricatedMapReduce := fluxus.NewMetricatedMapReduce(
    mapReduceStage,
    fluxus.WithMetricsStageName[[]Input, []Output]("distributed-processing"),
)

// Metricated Filter
metricatedFilter := fluxus.NewMetricatedFilter(
    filter,
    fluxus.WithMetricsStageName[Input, Input]("data-filter"),
)

// Metricated JoinByKey
metricatedJoin := fluxus.NewMetricatedJoinByKey(
    joinStage,
    fluxus.WithMetricsStageName[[]Input, map[Key][]Input]("data-join"),
)

// Metricated Window stages
metricatedCountWindow := fluxus.NewMetricatedTumblingCountWindow(
    countWindow,
    fluxus.WithMetricsStreamStageName[Input, []Input]("count-window"),
)

metricatedTimeWindow := fluxus.NewMetricatedTumblingTimeWindow(
    timeWindow,
    fluxus.WithMetricsStreamStageName[Input, []Input]("time-window"),
)
```

### Implementing Custom Collectors

You can implement the `MetricsCollector` interface to integrate with your preferred monitoring system:

```go
// Prometheus metrics collector example
type PrometheusMetrics struct {
    stageLatency    *prometheus.HistogramVec
    stageErrors     *prometheus.CounterVec
    retryAttempts   *prometheus.CounterVec
    batchProcessing *prometheus.HistogramVec
    fanOutLatency   *prometheus.HistogramVec
    fanInLatency    *prometheus.HistogramVec
}

func NewPrometheusMetrics(registry *prometheus.Registry) *PrometheusMetrics {
    m := &PrometheusMetrics{
        stageLatency: prometheus.NewHistogramVec(
            prometheus.HistogramOpts{
                Name:    "fluxus_stage_duration_seconds",
                Help:    "Duration of stage execution in seconds",
                Buckets: prometheus.DefBuckets,
            },
            []string{"stage"},
        ),
        stageErrors: prometheus.NewCounterVec(
            prometheus.CounterOpts{
                Name: "fluxus_stage_errors_total",
                Help: "Total number of stage errors",
            },
            []string{"stage", "error_type"},
        ),
        // ... other metrics definitions
    }
    
    // Register metrics
    registry.MustRegister(
        m.stageLatency,
        m.stageErrors,
        // ... other metrics
    )
    
    return m
}

// Implement StageStarted
func (m *PrometheusMetrics) StageStarted(ctx context.Context, stageName string) {
    // No-op for Prometheus (we'll record duration on completion)
}

// Implement StageCompleted
func (m *PrometheusMetrics) StageCompleted(ctx context.Context, stageName string, duration time.Duration) {
    m.stageLatency.WithLabelValues(stageName).Observe(duration.Seconds())
}

// Implement StageError
func (m *PrometheusMetrics) StageError(ctx context.Context, stageName string, err error) {
    errorType := "unknown"
    
    // Categorize common error types
    if errors.Is(err, context.DeadlineExceeded) {
        errorType = "timeout"
    } else if errors.Is(err, context.Canceled) {
        errorType = "canceled"
    } else if errors.Is(err, fluxus.ErrCircuitOpen) {
        errorType = "circuit_breaker"
    }
    
    m.stageErrors.WithLabelValues(stageName, errorType).Inc()
}

// ... implement other methods
```

### Setting a Default Collector

You can set a default metrics collector for all metricated stages:

```go
// Create a collector
collector := NewPrometheusMetrics(promRegistry)

// Set as the default
fluxus.DefaultMetricsCollector = collector
```

## Distributed Tracing

Fluxus integrates with OpenTelemetry to provide distributed tracing capabilities.

### Adding Tracing to Stages

Use the `TracedStage` wrapper to add tracing to any stage:

```go
// Create a traced stage wrapper
traced := fluxus.NewTracedStage(
    stage,
    fluxus.WithTracerStageName[Input, Output]("my-processing-stage"),
    fluxus.WithTracerAttributes[Input, Output](
        attribute.String("service", "data-processor"),
        attribute.String("environment", "production"),
    ),
)

// Now use the traced stage in your pipeline
pipeline := fluxus.NewPipeline(traced)
```

### Specialized Traced Wrappers

Fluxus provides specialized traced wrappers for specific stage types that collect additional tracing details:

```go
// Traced FanOut
tracedFanOut := fluxus.NewTracedFanOut(
    fanOut,
    fluxus.WithTracerStageName[Input, []Output]("parallel-processing"), // Assuming FanOut output is []Output
    fluxus.WithTracerAttributes[Input, []Output](
        attribute.String("operation", "data-transformation"),
    ),
    // Potentially fluxus.WithTracerProvider(...) if not default
)

// Traced FanIn
tracedFanIn := fluxus.NewTracedFanIn(
    fanIn,
    fluxus.WithTracerStageName[Input, []Output]("result-aggregation")
    fluxus.WithTracerAttributes[Input, []Output](
        attribute.String("operation", "data-aggregation"),
    )
)

// Traced Buffer
tracedBuffer := fluxus.NewTracedBuffer(
    buffer,
    fluxus.WithTracerStageName[Input, []Output]("batch-processor"),
    fluxus.WithTracerAttributes[Input, []Output](
        attribute.String("operation", "batch-insert"),
    )
)

// Traced Retry
tracedRetry := fluxus.NewTracedRetry(
    retry,
    fluxus.WithTracerStageName[Input, []Output]("resilient-operation"),
    fluxus.WithTracerAttributes[Input, []Output](
        attribute.String("operation", "api-call"),
    )
)

// Traced Pipeline
tracedPipeline := fluxus.NewTracedPipeline(
    pipeline,
    fluxus.WithTracerPipelineName[Input, Output]("etl-pipeline"),
    fluxus.WithTracerPipelineAttributes[Input, Output](
        attribute.String("data-source", "sales-db"),
    ),
    // Optionally: fluxus.WithTracerPipelineProvider[Input, Output](customProvider),
)

// Traced Timeout
tracedTimeout := fluxus.NewTracedTimeout(
    timeout,
    fluxus.WithTracerStageName[Input, Output]("timeout-stage"),
    fluxus.WithTracerAttributes[Input, Output](
        attribute.String("operation", "external-api-call"),
    ),
)

// Traced CircuitBreaker
tracedCircuitBreaker := fluxus.NewTracedCircuitBreaker(
    circuitBreaker,
    fluxus.WithTracerStageName[Input, Output]("circuit-breaker"),
    fluxus.WithTracerAttributes[Input, Output](
        attribute.String("service", "payment-gateway"),
    ),
)

// Traced DeadLetterQueue
tracedDLQ := fluxus.NewTracedDeadLetterQueue(
    dlq,
    fluxus.WithTracerStageName[Input, Output]("dead-letter-queue"),
    fluxus.WithTracerAttributes[Input, Output](
        attribute.String("dlq-target", "error-queue"),
    ),
)

// Traced Router
tracedRouter := fluxus.NewTracedRouter(
    router,
    fluxus.WithTracerStageName[Input, []Output]("conditional-router"),
    fluxus.WithTracerAttributes[Input, []Output](
        attribute.String("routing-strategy", "content-based"),
    ),
)

// Traced Filter
tracedFilter := fluxus.NewTracedFilter(
    filter,
    fluxus.WithTracerStageName[Input, Input]("data-filter"),
    fluxus.WithTracerAttributes[Input, Input](
        attribute.String("filter-type", "validation"),
    ),
)

// Traced JoinByKey
tracedJoin := fluxus.NewTracedJoinByKey(
    joinStage,
    fluxus.WithTracerStageName[[]Input, map[Key][]Input]("key-join"),
    fluxus.WithTracerAttributes[[]Input, map[Key][]Input](
        attribute.String("join-type", "inner"),
    ),
)

// Traced Map and MapReduce
tracedMap := fluxus.NewTracedMap(
    mapStage,
    fluxus.WithTracerStageName[[]Input, []Output]("parallel-map"),
    fluxus.WithTracerAttributes[[]Input, []Output](
        attribute.String("operation", "transform"),
    ),
)

tracedMapReduce := fluxus.NewTracedMapReduce(
    mapReduceStage,
    fluxus.WithTracerStageName[[]Input, []Output]("distributed-computation"),
    fluxus.WithTracerAttributes[[]Input, []Output](
        attribute.String("computation", "aggregation"),
    ),
)

// Traced Window stages
tracedCountWindow := fluxus.NewTracedTumblingCountWindow(
    countWindow,
    fluxus.WithTracerStreamStageName[Input, []Input]("count-window"),
    fluxus.WithTracerStreamAttributes[Input, []Input](
        attribute.Int("window.size", 100),
    ),
)

tracedTimeWindow := fluxus.NewTracedTumblingTimeWindow(
    timeWindow,
    fluxus.WithTracerStreamStageName[Input, []Input]("time-window"),
    fluxus.WithTracerStreamAttributes[Input, []Input](
        attribute.String("window.duration", "1m"),
    ),
)
```

### Custom Tracers

You can provide a custom OpenTelemetry tracer:

```go
// Create a custom tracer
provider := tracesdk.NewTracerProvider(
    tracesdk.WithSampler(tracesdk.AlwaysSample()),
    tracesdk.WithBatcher(exporter),
)

// Use with traced stages
traced := fluxus.NewTracedStage(
    stage,
    fluxus.WithTracerStageName[Input, Output]("my-stage"),
    fluxus.WithTracerProvider[Input, Output](provider),
)

// Or with specialized tracers, ensuring options are used consistently:
tracedFanOut := fluxus.NewTracedFanOut(
    fanOut,
    fluxus.WithTracerStageName[Input, []Output]("parallel-processing"),
    fluxus.WithTracerProvider[Input, []Output](provider), // Provider passed as option
    // ... other options
)
```

### Trace Propagation

Tracing context is automatically propagated through the pipeline stages. If your pipeline interacts with external services, you can extract and inject trace context:

```go
// Stage that calls an external service with trace propagation
httpStage := fluxus.StageFunc[Request, Response](func(ctx context.Context, req Request) (Response, error) {
    // Create HTTP request
    httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, req.URL, nil)
    if err != nil {
        return Response{}, err
    }
    
    // Inject trace context into HTTP headers
    otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(httpReq.Header))
    
    // Make the request
    resp, err := httpClient.Do(httpReq)
    if err != nil {
        return Response{}, err
    }
    defer resp.Body.Close()
    
    // Process response
    // ...
    
    return Response{...}, nil
})
```

### Recommended Trace Attributes

For consistent tracing, consider including these attributes in your spans:

```go
// Common attributes for stages
commonAttrs := []attribute.KeyValue{
    attribute.String("service.name", "data-processor"),
    attribute.String("component", "fluxus-pipeline"),
    attribute.String("environment", "production"),
}

// Stage-specific attributes
stageAttrs := []attribute.KeyValue{
    attribute.String("stage.name", "data-transformation"),
    attribute.String("stage.type", "processor"),
    attribute.Int("batch.size", 100),
}

// Combine attributes
allAttrs := append(commonAttrs, stageAttrs...)

// Create traced stage with all attributes
traced := fluxus.NewTracedStage(
    stage,
    fluxus.WithTracerStageName[Input, Output]("transformation-stage"),
    fluxus.WithTracerAttributes[Input, Output](allAttrs...),
)
```

## Combining Metrics and Tracing

For comprehensive observability, you can combine metrics and tracing:

```go
// Start with a basic stage
baseStage := fluxus.StageFunc[Input, Output](/* ... */)

// Add metrics
metricated := fluxus.NewMetricatedStage(
    baseStage,
    fluxus.WithMetricsStageName[Input, Output]("processing-stage"),
    fluxus.WithMetricsCollector[Input, Output](collector),
)

// Add tracing on top of metrics
traced := fluxus.NewTracedStage(
    metricated,
    fluxus.WithTracerStageName[Input, Output]("processing-stage"),
    fluxus.WithTracerAttributes[Input, Output](
        attribute.String("service", "data-processor"),
        attribute.String("operation", "transform"),
    ),
)

// Use it in a pipeline
pipeline := fluxus.NewPipeline(traced)
```

## Pool Statistics

For memory pools, Fluxus provides statistics tracking:

```go
// Create a pool with a name
pool := fluxus.NewObjectPool(
    factory,
    fluxus.WithPoolName[MyObject]("my-object-pool"),
)

// Get statistics
stats := pool.Stats()
fmt.Printf("Pool stats: gets=%d, puts=%d, misses=%d, size=%d\n",
    stats["gets"], stats["puts"], stats["misses"], stats["current_size"])
```

## Custom Logging

Configure custom loggers for various components:

```go
// Create a logger
logger := log.New(os.Stdout, "Pipeline: ", log.LstdFlags)

// Use with stream pipeline
builder := fluxus.NewStreamPipeline[Input](
    fluxus.WithStreamLogger(logger),
)

// Use with adapter
adapter := fluxus.NewStreamAdapter(
    stage,
    fluxus.WithAdapterLogger[Input, Output](logger),
)

// Use with DLQ
dlqStage := fluxus.NewDeadLetterQueue(
    stage,
    fluxus.WithDLQHandler[Input, Output](handler),
    fluxus.WithDLQErrorLogger[Input, Output](func(err error) {
        logger.Printf("DLQ Error: %v", err)
    }),
)
```

## Integrating with External Monitoring Systems

### Prometheus Example

```go
// Initialize Prometheus registry and metrics
registry := prometheus.NewRegistry()
metrics := NewPrometheusMetrics(registry)

// Set as default collector
fluxus.DefaultMetricsCollector = metrics

// Expose Prometheus metrics endpoint
http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
go http.ListenAndServe(":9090", nil)
```

### Jaeger Example

```go
// Initialize Jaeger exporter
jaegerExporter, err := jaeger.New(
    jaeger.WithCollectorEndpoint(jaeger.WithEndpoint("http://jaeger:14268/api/traces")),
)
if err != nil {
    log.Fatalf("Failed to create Jaeger exporter: %v", err)
}

// Create trace provider
provider := tracesdk.NewTracerProvider(
    tracesdk.WithBatcher(jaegerExporter),
    tracesdk.WithSampler(tracesdk.AlwaysSample()),
    tracesdk.WithResource(resource.NewWithAttributes(
        semconv.SchemaURL,
        semconv.ServiceNameKey.String("data-processor"),
        attribute.String("environment", "production"),
    )),
)
defer provider.Shutdown(context.Background())

// Set as global trace provider
otel.SetTracerProvider(provider)
otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
    propagation.TraceContext{},
    propagation.Baggage{},
))

// Use with Fluxus traced stages
traced := fluxus.NewTracedStage(
    stage,
    fluxus.WithTracerStageName[Input, Output]("data-transformation"),
    fluxus.WithTracerProvider[Input, Output](provider),
)
```

## Best Practices

### Naming Conventions

Use consistent names across metrics and tracing:

```go
// Use the same name for both metrics and tracing
const stageName = "order-processing"

metricated := fluxus.NewMetricatedStage(
    stage,
    fluxus.WithMetricsStageName[Input, Output](stageName),
)

traced := fluxus.NewTracedStage(
    metricated,
    fluxus.WithTracerStageName[Input, Output](stageName),
)
```

### Granularity

Choose the right granularity for instrumentation:

- Too fine-grained: Excessive overhead, hard to interpret
- Too coarse-grained: Missing important details
- Just right: Instrument key boundaries, high-risk operations, and important business logic

```go
// Coarse-grained (pipeline level)
tracedPipeline := fluxus.NewTracedPipeline(pipeline, "etl-pipeline")

// Medium-grained (key stages)
extractTraced := fluxus.NewTracedStage(extractStage, "extract")
transformTraced := fluxus.NewTracedStage(transformStage, "transform")
loadTraced := fluxus.NewTracedStage(loadStage, "load")

// Fine-grained (detailed components)
// Generally avoid tracing every small operation
// unless there's a specific performance question
```

### Performance Considerations

Metrics and tracing add overhead. For performance-critical paths:

```go
// Use Noop collectors for dev/test environments
fluxus.DefaultMetricsCollector = &fluxus.NoopMetricsCollector{}

// Configure appropriate sampling rates for tracing
provider := tracesdk.NewTracerProvider(
    tracesdk.WithSampler(tracesdk.TraceIDRatioBased(0.1)), // 10% sampling
)

// Only trace specific stages
if shouldTrace {
    stage = fluxus.NewTracedStage(stage, /* ... */)
}
```

### Debug-Friendly Errors

Enrich errors with context for better observability:

```go
// Custom error handler with context enrichment
stage.WithErrorHandler(func(err error) error {
    return fmt.Errorf("processing failed for batch %d: %w", batchID, err)
})
```

### Health Checks and Readiness

Expose health and readiness endpoints that include component status:

```go
http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
    // Check circuit breakers
    if circuitBreaker.State() == fluxus.CircuitOpen {
        w.WriteHeader(http.StatusServiceUnavailable)
        w.Write([]byte("Circuit breaker open"))
        return
    }
    
    // Check pools
    poolStats := objectPool.Stats()
    if poolStats["misses"] > threshold {
        w.WriteHeader(http.StatusInternalServerError)
        w.Write([]byte("Pool experiencing high miss rate"))
        return
    }
    
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("OK"))
})
```
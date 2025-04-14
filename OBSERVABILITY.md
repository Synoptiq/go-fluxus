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
	FanOutStarted(ctx context.Context, stageName string, numBranches int) // Added stageName
	// FanOutCompleted is called when a FanOut stage completes processing.
	FanOutCompleted(ctx context.Context, stageName string, numBranches int, duration time.Duration) // Added stageName
	// FanInStarted is called when a FanIn stage begins processing.
	FanInStarted(ctx context.Context, stageName string, numInputs int) // Added stageName
	// FanInCompleted is called when a FanIn stage completes processing.
	FanInCompleted(ctx context.Context, stageName string, numInputs int, duration time.Duration) // Added stageName

	// --- Stream Pipeline Lifecycle Metrics ---

	// PipelineStarted is called when a StreamPipeline's Run method begins execution.
	// (Could optionally be called by simple Pipeline.Process too).
	PipelineStarted(ctx context.Context, pipelineName string) // Added
	// PipelineCompleted is called when a StreamPipeline's Run method finishes.
	// (Could optionally be called by simple Pipeline.Process too).
	PipelineCompleted(ctx context.Context, pipelineName string, duration time.Duration, err error) // Added

	// --- Stream Worker Metrics (Specific to how stages run within StreamPipeline) ---

	// StageWorkerConcurrency reports the configured concurrency for a stage within the stream.
	StageWorkerConcurrency(ctx context.Context, stageName string, concurrencyLevel int) // Added (Renamed from Adapter*)
	// StageWorkerItemProcessed reports successful processing of an item by a stream worker.
	StageWorkerItemProcessed(ctx context.Context, stageName string, duration time.Duration) // Added (Renamed from Adapter*)
	// StageWorkerItemSkipped reports an item skipped due to an error with SkipOnError strategy.
	StageWorkerItemSkipped(ctx context.Context, stageName string, err error) // Added (Renamed from Adapter*)
	// StageWorkerErrorSent reports an error sent to an error channel via SendToErrorChannel strategy.
	StageWorkerErrorSent(ctx context.Context, stageName string, err error) // Added (Renamed from Adapter*)
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
    "result-aggregation",
    attribute.String("operation", "data-aggregation"),
)

// Traced Buffer
tracedBuffer := fluxus.NewTracedBuffer(
    buffer,
    "batch-processor",
    attribute.String("operation", "batch-insert"),
)

// Traced Retry
tracedRetry := fluxus.NewTracedRetry(
    retry,
    "resilient-operation",
    attribute.String("operation", "api-call"),
)

// Traced Pipeline
tracedPipeline := fluxus.NewTracedPipeline(
    pipeline,
    "etl-pipeline",
    attribute.String("data-source", "sales-db"),
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

// Or with specialized tracers (New Example Style)
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
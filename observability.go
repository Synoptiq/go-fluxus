package fluxus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/zipkin"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	otelTrace "go.opentelemetry.io/otel/trace"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ObservabilityFactory creates observability components from pipeline configuration.
type ObservabilityFactory struct{}

// NewObservabilityFactory creates a new factory for observability components.
func NewObservabilityFactory() *ObservabilityFactory {
	return &ObservabilityFactory{}
}

// CreateTracerProvider creates a TracerProvider based on the pipeline tracing configuration.
func (f *ObservabilityFactory) CreateTracerProvider(
	config PipelineTracingConfig,
	serviceName string,
) (TracerProvider, error) {
	if !config.Enabled {
		return &NoopTracerProvider{}, nil
	}

	switch config.Type {
	case TracingTypeNoop:
		return &NoopTracerProvider{}, nil
	case TracingTypeZipkin:
		return f.createZipkinTracerProvider(config, serviceName)
	case TracingTypeJaeger:
		return f.createJaegerTracerProvider(config, serviceName)
	case TracingTypeOTLP:
		return f.createOTLPTracerProvider(config, serviceName)
	default:
		return nil, fmt.Errorf("unsupported tracing type: %s", config.Type)
	}
}

// CreateMetricsCollector creates a MetricsCollector based on the pipeline metrics configuration.
func (f *ObservabilityFactory) CreateMetricsCollector(config PipelineMetricsConfig) (MetricsCollector, error) {
	if !config.Enabled {
		return &NoopMetricsCollector{}, nil
	}

	switch config.Type {
	case MetricsTypeNoop:
		return &NoopMetricsCollector{}, nil
	case MetricsTypePrometheus:
		return f.createPrometheusCollector(config)
	case MetricsTypeMongoDB:
		return f.createMongoDBCollector(config)
	case MetricsTypeInfluxDB:
		return f.createInfluxDBCollector(config)
	default:
		return nil, fmt.Errorf("unsupported metrics type: %s", config.Type)
	}
}

func (f *ObservabilityFactory) createPrometheusCollector(config PipelineMetricsConfig) (MetricsCollector, error) {
	if config.Endpoint == "" {
		return nil, errors.New("prometheus endpoint is required")
	}

	// Create a new Prometheus registry
	reg := prometheus.NewRegistry()

	collector := &PrometheusMetricsCollector{
		endpoint: config.Endpoint,
		registry: reg,
		metrics:  make(map[string]prometheus.Collector),
	}

	// Initialize all metrics
	collector.initializePrometheusMetrics()

	return collector, nil
}

func (f *ObservabilityFactory) createInfluxDBCollector(config PipelineMetricsConfig) (MetricsCollector, error) {
	if config.Endpoint == "" {
		return nil, errors.New("influxdb endpoint is required")
	}

	// Create InfluxDB client
	client := influxdb2.NewClient(config.Endpoint, "")
	writeAPI := client.WriteAPI("", "fluxus")

	return &InfluxDBMetricsCollector{
		client:   client,
		writeAPI: writeAPI,
		endpoint: config.Endpoint,
	}, nil
}

func (f *ObservabilityFactory) createOTLPTracerProvider(
	config PipelineTracingConfig,
	serviceName string,
) (TracerProvider, error) {
	if config.Endpoint == "" {
		return nil, errors.New("otlp endpoint is required")
	}

	// Create OTLP exporter
	exporter, err := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithEndpoint(config.Endpoint),
		otlptracegrpc.WithInsecure(), // Use WithTLSCredentials() for secure connections
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Create resource with service information
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String("2.0.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create trace provider
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(res),
	)

	return &OTLPTracerProvider{tp: tp}, nil
}

func (f *ObservabilityFactory) createJaegerTracerProvider(
	config PipelineTracingConfig,
	serviceName string,
) (TracerProvider, error) {
	// Jaeger exporter is deprecated, redirect to OTLP
	// Users should migrate to OTLP for Jaeger ingestion
	return f.createOTLPTracerProvider(config, serviceName)
}

func (f *ObservabilityFactory) createZipkinTracerProvider(
	config PipelineTracingConfig,
	serviceName string,
) (TracerProvider, error) {
	if config.Endpoint == "" {
		return nil, errors.New("zipkin endpoint is required")
	}

	// Create Zipkin exporter
	exporter, err := zipkin.New(config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to create Zipkin exporter: %w", err)
	}

	// Create resource with service information
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String("2.0.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create trace provider
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(res),
	)

	return &ZipkinTracerProvider{tp: tp}, nil
}

func (f *ObservabilityFactory) createMongoDBCollector(config PipelineMetricsConfig) (MetricsCollector, error) {
	if config.Endpoint == "" {
		return nil, errors.New("mongodb endpoint is required")
	}

	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(config.Endpoint)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Test the connection
	if errPing := client.Ping(context.Background(), nil); errPing != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", errPing)
	}

	return &MongoDBMetricsCollector{
		client:     client,
		database:   client.Database("fluxus_metrics"),
		collection: client.Database("fluxus_metrics").Collection("pipeline_metrics"),
		endpoint:   config.Endpoint,
	}, nil
}

// LoggingMetricsCollector is a simple implementation that logs metrics to a logger.
// This serves as a development/testing implementation and format examples for real implementations.
type LoggingMetricsCollector struct {
	logger   *log.Logger
	endpoint string
	format   string // "prometheus", "mongodb", "influxdb", or empty for default
}

// Ensure LoggingMetricsCollector implements MetricsCollector.
var _ MetricsCollector = (*LoggingMetricsCollector)(nil)

// StageStarted logs when a stage starts.
func (l *LoggingMetricsCollector) StageStarted(_ context.Context, stageName string) {
	switch l.format {
	case "prometheus":
		l.logger.Printf(
			"# TYPE fluxus_stage_started counter\nfluxus_stage_started{stage=\"%s\",endpoint=\"%s\"} 1",
			stageName,
			l.endpoint,
		)
	case "influxdb":
		l.logger.Printf("stage_started,stage=%s,endpoint=%s value=1 %d", stageName, l.endpoint, time.Now().UnixNano())
	case "mongodb":
		l.logger.Printf(
			`{"metric":"stage_started","stage":"%s","endpoint":"%s","timestamp":"%s"}`,
			stageName,
			l.endpoint,
			time.Now().Format(time.RFC3339),
		)
	default:
		l.logger.Printf("METRICS [%s]: Stage '%s' started", l.endpoint, stageName)
	}
}

// StageCompleted logs when a stage completes.
func (l *LoggingMetricsCollector) StageCompleted(_ context.Context, stageName string, duration time.Duration) {
	switch l.format {
	case "prometheus":
		l.logger.Printf(
			"# TYPE fluxus_stage_duration histogram\nfluxus_stage_duration{stage=\"%s\",endpoint=\"%s\"} %f",
			stageName,
			l.endpoint,
			duration.Seconds(),
		)
	case "influxdb":
		l.logger.Printf(
			"stage_duration,stage=%s,endpoint=%s value=%f %d",
			stageName,
			l.endpoint,
			duration.Seconds(),
			time.Now().UnixNano(),
		)
	case "mongodb":
		l.logger.Printf(
			`{"metric":"stage_completed","stage":"%s","endpoint":"%s","duration_ms":%d,"timestamp":"%s"}`,
			stageName,
			l.endpoint,
			duration.Milliseconds(),
			time.Now().Format(time.RFC3339),
		)
	default:
		l.logger.Printf("METRICS [%s]: Stage '%s' completed in %v", l.endpoint, stageName, duration)
	}
}

// StageError logs when a stage errors.
func (l *LoggingMetricsCollector) StageError(_ context.Context, stageName string, err error) {
	l.logger.Printf("METRICS [%s]: Stage '%s' error: %v", l.endpoint, stageName, err)
}

// RetryAttempt logs retry attempts.
func (l *LoggingMetricsCollector) RetryAttempt(_ context.Context, stageName string, attempt int, err error) {
	l.logger.Printf("METRICS [%s]: Stage '%s' retry attempt %d: %v", l.endpoint, stageName, attempt, err)
}

// BufferBatchProcessed logs buffer batch processing.
func (l *LoggingMetricsCollector) BufferBatchProcessed(_ context.Context, batchSize int, duration time.Duration) {
	l.logger.Printf("METRICS [%s]: Buffer processed batch of %d items in %v", l.endpoint, batchSize, duration)
}

// FanOutStarted logs fan-out start.
func (l *LoggingMetricsCollector) FanOutStarted(_ context.Context, stageName string, numBranches int) {
	l.logger.Printf("METRICS [%s]: FanOut '%s' started with %d branches", l.endpoint, stageName, numBranches)
}

// FanOutCompleted logs fan-out completion.
func (l *LoggingMetricsCollector) FanOutCompleted(
	_ context.Context,
	stageName string,
	numBranches int,
	duration time.Duration,
) {
	l.logger.Printf(
		"METRICS [%s]: FanOut '%s' completed %d branches in %v",
		l.endpoint,
		stageName,
		numBranches,
		duration,
	)
}

// FanInStarted logs fan-in start.
func (l *LoggingMetricsCollector) FanInStarted(_ context.Context, stageName string, numInputs int) {
	l.logger.Printf("METRICS [%s]: FanIn '%s' started with %d inputs", l.endpoint, stageName, numInputs)
}

// FanInCompleted logs fan-in completion.
func (l *LoggingMetricsCollector) FanInCompleted(
	_ context.Context,
	stageName string,
	numInputs int,
	duration time.Duration,
) {
	l.logger.Printf("METRICS [%s]: FanIn '%s' completed %d inputs in %v", l.endpoint, stageName, numInputs, duration)
}

// PipelineStarted logs pipeline start.
func (l *LoggingMetricsCollector) PipelineStarted(_ context.Context, pipelineName string) {
	l.logger.Printf("METRICS [%s]: Pipeline '%s' started", l.endpoint, pipelineName)
}

// PipelineCompleted logs pipeline completion.
func (l *LoggingMetricsCollector) PipelineCompleted(
	_ context.Context,
	pipelineName string,
	duration time.Duration,
	err error,
) {
	if err != nil {
		l.logger.Printf(
			"METRICS [%s]: Pipeline '%s' completed with error in %v: %v",
			l.endpoint,
			pipelineName,
			duration,
			err,
		)
	} else {
		l.logger.Printf("METRICS [%s]: Pipeline '%s' completed successfully in %v", l.endpoint, pipelineName, duration)
	}
}

// StageWorkerConcurrency logs stage worker concurrency.
func (l *LoggingMetricsCollector) StageWorkerConcurrency(_ context.Context, stageName string, concurrencyLevel int) {
	l.logger.Printf("METRICS [%s]: Stage '%s' using concurrency level %d", l.endpoint, stageName, concurrencyLevel)
}

// StageWorkerItemProcessed logs stage worker item processing.
func (l *LoggingMetricsCollector) StageWorkerItemProcessed(
	_ context.Context,
	stageName string,
	duration time.Duration,
) {
	l.logger.Printf("METRICS [%s]: Stage '%s' processed item in %v", l.endpoint, stageName, duration)
}

// StageWorkerItemSkipped logs stage worker item skipping.
func (l *LoggingMetricsCollector) StageWorkerItemSkipped(_ context.Context, stageName string, err error) {
	l.logger.Printf("METRICS [%s]: Stage '%s' skipped item: %v", l.endpoint, stageName, err)
}

// StageWorkerErrorSent logs stage worker error sending.
func (l *LoggingMetricsCollector) StageWorkerErrorSent(_ context.Context, stageName string, err error) {
	l.logger.Printf("METRICS [%s]: Stage '%s' sent error to channel: %v", l.endpoint, stageName, err)
}

// WindowEmitted logs window emission.
func (l *LoggingMetricsCollector) WindowEmitted(_ context.Context, stageName string, itemCountInWindow int) {
	l.logger.Printf("METRICS [%s]: Window '%s' emitted with %d items", l.endpoint, stageName, itemCountInWindow)
}

// Implement all the stage-specific metrics methods...
// For brevity, I'll implement a few key ones and use a generic approach for others

// TimeoutOccurred logs timeout events.
func (l *LoggingMetricsCollector) TimeoutOccurred(
	_ context.Context,
	stageName string,
	configuredTimeout time.Duration,
	actualDuration time.Duration,
) {
	l.logger.Printf(
		"METRICS [%s]: Stage '%s' timed out after %v (configured: %v)",
		l.endpoint,
		stageName,
		actualDuration,
		configuredTimeout,
	)
}

// CircuitStateChanged logs circuit breaker state changes.
func (l *LoggingMetricsCollector) CircuitStateChanged(
	_ context.Context,
	stageName string,
	fromState, toState string,
) {
	l.logger.Printf(
		"METRICS [%s]: Circuit breaker '%s' state changed from %s to %s",
		l.endpoint,
		stageName,
		fromState,
		toState,
	)
}

// CircuitBreakerRejected logs circuit breaker rejections.
func (l *LoggingMetricsCollector) CircuitBreakerRejected(_ context.Context, stageName string) {
	l.logger.Printf("METRICS [%s]: Circuit breaker '%s' rejected request", l.endpoint, stageName)
}

// CircuitBreakerFailureRecorded logs circuit breaker failure recording.
func (l *LoggingMetricsCollector) CircuitBreakerFailureRecorded(
	_ context.Context,
	stageName string,
	failureCount int,
	threshold int,
) {
	l.logger.Printf(
		"METRICS [%s]: Circuit breaker '%s' recorded failure %d/%d",
		l.endpoint,
		stageName,
		failureCount,
		threshold,
	)
}

// CircuitBreakerSuccessRecorded logs circuit breaker success recording.
func (l *LoggingMetricsCollector) CircuitBreakerSuccessRecorded(
	_ context.Context,
	stageName string,
	successCount int,
	threshold int,
) {
	l.logger.Printf(
		"METRICS [%s]: Circuit breaker '%s' recorded success %d/%d",
		l.endpoint,
		stageName,
		successCount,
		threshold,
	)
}

// DeadLetterQueueItemSent logs DLQ item sending.
func (l *LoggingMetricsCollector) DeadLetterQueueItemSent(_ context.Context, stageName string, originalError error) {
	l.logger.Printf("METRICS [%s]: DLQ '%s' sent item due to error: %v", l.endpoint, stageName, originalError)
}

// DeadLetterQueueHandlerError logs DLQ handler errors.
func (l *LoggingMetricsCollector) DeadLetterQueueHandlerError(_ context.Context, stageName string, dlqError error) {
	l.logger.Printf("METRICS [%s]: DLQ '%s' handler error: %v", l.endpoint, stageName, dlqError)
}

// RouterRoutesSelected logs router route selection.
func (l *LoggingMetricsCollector) RouterRoutesSelected(
	_ context.Context,
	stageName string,
	numRoutesSelected int,
	totalRoutes int,
) {
	l.logger.Printf(
		"METRICS [%s]: Router '%s' selected %d/%d routes",
		l.endpoint,
		stageName,
		numRoutesSelected,
		totalRoutes,
	)
}

// RouterNoRouteMatched logs when no routes match.
func (l *LoggingMetricsCollector) RouterNoRouteMatched(_ context.Context, stageName string) {
	l.logger.Printf("METRICS [%s]: Router '%s' no routes matched", l.endpoint, stageName)
}

// RouterRouteProcessed logs route processing.
func (l *LoggingMetricsCollector) RouterRouteProcessed(
	_ context.Context,
	stageName string,
	routeName string,
	routeIndex int,
	duration time.Duration,
) {
	l.logger.Printf(
		"METRICS [%s]: Router '%s' processed route '%s' (%d) in %v",
		l.endpoint,
		stageName,
		routeName,
		routeIndex,
		duration,
	)
}

// MapItemProcessed logs map item processing.
func (l *LoggingMetricsCollector) MapItemProcessed(
	_ context.Context,
	stageName string,
	itemIndex int,
	duration time.Duration,
) {
	l.logger.Printf("METRICS [%s]: Map '%s' processed item %d in %v", l.endpoint, stageName, itemIndex, duration)
}

// MapItemError logs map item errors.
func (l *LoggingMetricsCollector) MapItemError(_ context.Context, stageName string, itemIndex int, err error) {
	l.logger.Printf("METRICS [%s]: Map '%s' item %d error: %v", l.endpoint, stageName, itemIndex, err)
}

// MapConcurrencyLevel logs map concurrency level.
func (l *LoggingMetricsCollector) MapConcurrencyLevel(
	_ context.Context,
	stageName string,
	concurrencyLevel int,
	totalItems int,
) {
	l.logger.Printf(
		"METRICS [%s]: Map '%s' using concurrency %d for %d items",
		l.endpoint,
		stageName,
		concurrencyLevel,
		totalItems,
	)
}

// MapReduceMapPhaseCompleted logs map-reduce map phase completion.
func (l *LoggingMetricsCollector) MapReduceMapPhaseCompleted(
	_ context.Context,
	stageName string,
	numItems int,
	numKeys int,
	duration time.Duration,
) {
	l.logger.Printf(
		"METRICS [%s]: MapReduce '%s' map phase completed: %d items -> %d keys in %v",
		l.endpoint,
		stageName,
		numItems,
		numKeys,
		duration,
	)
}

// MapReduceShufflePhaseCompleted logs map-reduce shuffle phase completion.
func (l *LoggingMetricsCollector) MapReduceShufflePhaseCompleted(
	_ context.Context,
	stageName string,
	numKeys int,
	duration time.Duration,
) {
	l.logger.Printf(
		"METRICS [%s]: MapReduce '%s' shuffle phase completed: %d keys in %v",
		l.endpoint,
		stageName,
		numKeys,
		duration,
	)
}

// MapReduceReducePhaseCompleted logs map-reduce reduce phase completion.
func (l *LoggingMetricsCollector) MapReduceReducePhaseCompleted(
	_ context.Context,
	stageName string,
	numKeys int,
	numResults int,
	duration time.Duration,
) {
	l.logger.Printf(
		"METRICS [%s]: MapReduce '%s' reduce phase completed: %d keys -> %d results in %v",
		l.endpoint,
		stageName,
		numKeys,
		numResults,
		duration,
	)
}

// MapReduceKeyGroupSize logs map-reduce key group sizes.
func (l *LoggingMetricsCollector) MapReduceKeyGroupSize(
	_ context.Context,
	stageName string,
	key string,
	groupSize int,
) {
	l.logger.Printf("METRICS [%s]: MapReduce '%s' key '%s' has group size %d", l.endpoint, stageName, key, groupSize)
}

// FilterItemPassed logs filter item passing.
func (l *LoggingMetricsCollector) FilterItemPassed(_ context.Context, stageName string) {
	l.logger.Printf("METRICS [%s]: Filter '%s' item passed", l.endpoint, stageName)
}

// FilterItemDropped logs filter item dropping.
func (l *LoggingMetricsCollector) FilterItemDropped(_ context.Context, stageName string) {
	l.logger.Printf("METRICS [%s]: Filter '%s' item dropped", l.endpoint, stageName)
}

// FilterPredicateError logs filter predicate errors.
func (l *LoggingMetricsCollector) FilterPredicateError(_ context.Context, stageName string, err error) {
	l.logger.Printf("METRICS [%s]: Filter '%s' predicate error: %v", l.endpoint, stageName, err)
}

// JoinByKeyGroupCreated logs join by key group creation.
func (l *LoggingMetricsCollector) JoinByKeyGroupCreated(
	_ context.Context,
	stageName string,
	keyStr string,
	groupSize int,
) {
	l.logger.Printf(
		"METRICS [%s]: JoinByKey '%s' created group for key '%s' with %d items",
		l.endpoint,
		stageName,
		keyStr,
		groupSize,
	)
}

// JoinByKeyCompleted logs join by key completion.
func (l *LoggingMetricsCollector) JoinByKeyCompleted(
	_ context.Context,
	stageName string,
	numKeys int,
	totalItems int,
	duration time.Duration,
) {
	l.logger.Printf(
		"METRICS [%s]: JoinByKey '%s' completed: %d items -> %d keys in %v",
		l.endpoint,
		stageName,
		totalItems,
		numKeys,
		duration,
	)
}

// CustomStageMetric logs custom stage metrics.
func (l *LoggingMetricsCollector) CustomStageMetric(
	_ context.Context,
	stageName string,
	metricName string,
	value any,
) {
	l.logger.Printf("METRICS [%s]: Custom stage '%s' metric '%s': %v", l.endpoint, stageName, metricName, value)
}

// CustomStageEvent logs custom stage events.
func (l *LoggingMetricsCollector) CustomStageEvent(
	_ context.Context,
	stageName string,
	eventName string,
	metadata map[string]any,
) {
	l.logger.Printf("METRICS [%s]: Custom stage '%s' event '%s': %+v", l.endpoint, stageName, eventName, metadata)
}

// OTLPTracerProvider wraps the OpenTelemetry SDK TracerProvider for OTLP export.
type OTLPTracerProvider struct {
	tp *trace.TracerProvider
}

// Tracer returns a tracer from the underlying provider.
func (p *OTLPTracerProvider) Tracer(name string, options ...otelTrace.TracerOption) otelTrace.Tracer {
	return p.tp.Tracer(name, options...)
}

// Shutdown gracefully shuts down the tracer provider.
func (p *OTLPTracerProvider) Shutdown(ctx context.Context) error {
	return p.tp.Shutdown(ctx)
}

// Ensure OTLPTracerProvider implements TracerProvider.
var _ TracerProvider = (*OTLPTracerProvider)(nil)

// JaegerTracerProvider wraps the OpenTelemetry SDK TracerProvider for Jaeger export.
type JaegerTracerProvider struct {
	tp *trace.TracerProvider
}

// Tracer returns a tracer from the underlying provider.
func (p *JaegerTracerProvider) Tracer(name string, options ...otelTrace.TracerOption) otelTrace.Tracer {
	return p.tp.Tracer(name, options...)
}

// Shutdown gracefully shuts down the tracer provider.
func (p *JaegerTracerProvider) Shutdown(ctx context.Context) error {
	return p.tp.Shutdown(ctx)
}

// Ensure JaegerTracerProvider implements TracerProvider.
var _ TracerProvider = (*JaegerTracerProvider)(nil)

// ZipkinTracerProvider wraps the OpenTelemetry SDK TracerProvider for Zipkin export.
type ZipkinTracerProvider struct {
	tp *trace.TracerProvider
}

// Tracer returns a tracer from the underlying provider.
func (p *ZipkinTracerProvider) Tracer(name string, options ...otelTrace.TracerOption) otelTrace.Tracer {
	return p.tp.Tracer(name, options...)
}

// Shutdown gracefully shuts down the tracer provider.
func (p *ZipkinTracerProvider) Shutdown(ctx context.Context) error {
	return p.tp.Shutdown(ctx)
}

// Ensure ZipkinTracerProvider implements TracerProvider.
var _ TracerProvider = (*ZipkinTracerProvider)(nil)

// PrometheusMetricsCollector implements MetricsCollector for Prometheus.
type PrometheusMetricsCollector struct {
	endpoint string
	registry *prometheus.Registry
	metrics  map[string]prometheus.Collector

	// Pre-defined metrics
	stageStartedCounter          prometheus.Counter
	stageDurationHistogram       prometheus.Histogram
	stageErrorsCounter           prometheus.Counter
	retryAttemptsCounter         prometheus.Counter
	bufferBatchSizeGauge         prometheus.Gauge
	bufferBatchDurationHistogram prometheus.Histogram
	fanoutBranchesGauge          prometheus.Gauge
	fanoutDurationHistogram      prometheus.Histogram
	faninInputsGauge             prometheus.Gauge
	faninDurationHistogram       prometheus.Histogram
	pipelineStartedCounter       prometheus.Counter
	pipelineDurationHistogram    prometheus.Histogram
	stageConcurrencyGauge        prometheus.Gauge
	stageItemDurationHistogram   prometheus.Histogram
	stageItemsSkippedCounter     prometheus.Counter
	stageErrorsSentCounter       prometheus.Counter
	windowItemsGauge             prometheus.Gauge
}

// Ensure PrometheusMetricsCollector implements MetricsCollector.
var _ MetricsCollector = (*PrometheusMetricsCollector)(nil)

// initializePrometheusMetrics creates all the Prometheus metrics.
func (p *PrometheusMetricsCollector) initializePrometheusMetrics() {
	p.stageStartedCounter = promauto.With(p.registry).NewCounter(prometheus.CounterOpts{
		Name: "fluxus_stage_started_total",
		Help: "Total number of stages started",
	})

	p.stageDurationHistogram = promauto.With(p.registry).NewHistogram(prometheus.HistogramOpts{
		Name: "fluxus_stage_duration_seconds",
		Help: "Duration of stage execution in seconds",
	})

	p.stageErrorsCounter = promauto.With(p.registry).NewCounter(prometheus.CounterOpts{
		Name: "fluxus_stage_errors_total",
		Help: "Total number of stage errors",
	})

	p.retryAttemptsCounter = promauto.With(p.registry).NewCounter(prometheus.CounterOpts{
		Name: "fluxus_retry_attempts_total",
		Help: "Total number of retry attempts",
	})

	p.bufferBatchSizeGauge = promauto.With(p.registry).NewGauge(prometheus.GaugeOpts{
		Name: "fluxus_buffer_batch_size",
		Help: "Current buffer batch size",
	})

	p.bufferBatchDurationHistogram = promauto.With(p.registry).NewHistogram(prometheus.HistogramOpts{
		Name: "fluxus_buffer_batch_duration_seconds",
		Help: "Duration of buffer batch processing in seconds",
	})

	p.fanoutBranchesGauge = promauto.With(p.registry).NewGauge(prometheus.GaugeOpts{
		Name: "fluxus_fanout_branches",
		Help: "Number of fanout branches",
	})

	p.fanoutDurationHistogram = promauto.With(p.registry).NewHistogram(prometheus.HistogramOpts{
		Name: "fluxus_fanout_duration_seconds",
		Help: "Duration of fanout execution in seconds",
	})

	p.faninInputsGauge = promauto.With(p.registry).NewGauge(prometheus.GaugeOpts{
		Name: "fluxus_fanin_inputs",
		Help: "Number of fanin inputs",
	})

	p.faninDurationHistogram = promauto.With(p.registry).NewHistogram(prometheus.HistogramOpts{
		Name: "fluxus_fanin_duration_seconds",
		Help: "Duration of fanin execution in seconds",
	})

	p.pipelineStartedCounter = promauto.With(p.registry).NewCounter(prometheus.CounterOpts{
		Name: "fluxus_pipeline_started_total",
		Help: "Total number of pipelines started",
	})

	p.pipelineDurationHistogram = promauto.With(p.registry).NewHistogram(prometheus.HistogramOpts{
		Name: "fluxus_pipeline_duration_seconds",
		Help: "Duration of pipeline execution in seconds",
	})

	p.stageConcurrencyGauge = promauto.With(p.registry).NewGauge(prometheus.GaugeOpts{
		Name: "fluxus_stage_concurrency",
		Help: "Current stage concurrency level",
	})

	p.stageItemDurationHistogram = promauto.With(p.registry).NewHistogram(prometheus.HistogramOpts{
		Name: "fluxus_stage_item_duration_seconds",
		Help: "Duration of individual item processing in seconds",
	})

	p.stageItemsSkippedCounter = promauto.With(p.registry).NewCounter(prometheus.CounterOpts{
		Name: "fluxus_stage_items_skipped_total",
		Help: "Total number of items skipped by stages",
	})

	p.stageErrorsSentCounter = promauto.With(p.registry).NewCounter(prometheus.CounterOpts{
		Name: "fluxus_stage_errors_sent_total",
		Help: "Total number of errors sent to error channels",
	})

	p.windowItemsGauge = promauto.With(p.registry).NewGauge(prometheus.GaugeOpts{
		Name: "fluxus_window_items",
		Help: "Number of items in current window",
	})
}

// StageStarted increments stage started counter.
func (p *PrometheusMetricsCollector) StageStarted(_ context.Context, _ string) {
	p.stageStartedCounter.Inc()
}

// StageCompleted records stage completion duration.
func (p *PrometheusMetricsCollector) StageCompleted(_ context.Context, _ string, duration time.Duration) {
	p.stageDurationHistogram.Observe(duration.Seconds())
}

// StageError increments stage error counter.
func (p *PrometheusMetricsCollector) StageError(_ context.Context, stageName string, err error) {
	// For better observability, we could use a counter with labels
	errorCounter := promauto.With(p.registry).NewCounterVec(prometheus.CounterOpts{
		Name: "fluxus_stage_errors_by_type_total",
		Help: "Total number of stage errors by type",
	}, []string{"stage", "error_type"})
	errorCounter.WithLabelValues(stageName, fmt.Sprintf("%T", err)).Inc()

	// Also increment the simple counter for backward compatibility
	p.stageErrorsCounter.Inc()
}

// RetryAttempt increments retry attempts counter.
func (p *PrometheusMetricsCollector) RetryAttempt(_ context.Context, stageName string, attempt int, err error) {
	// Track retry attempts with more context
	retryCounter := promauto.With(p.registry).NewCounterVec(prometheus.CounterOpts{
		Name: "fluxus_retry_attempts_by_stage_total",
		Help: "Total number of retry attempts by stage and error type",
	}, []string{"stage", "error_type"})
	retryCounter.WithLabelValues(stageName, fmt.Sprintf("%T", err)).Inc()

	// Also track the attempt number as a gauge
	retryGauge := promauto.With(p.registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "fluxus_retry_current_attempt",
		Help: "Current retry attempt number",
	}, []string{"stage"})
	retryGauge.WithLabelValues(stageName).Set(float64(attempt))

	// Increment simple counter for backward compatibility
	p.retryAttemptsCounter.Inc()
}

// BufferBatchProcessed records buffer batch metrics.
func (p *PrometheusMetricsCollector) BufferBatchProcessed(_ context.Context, batchSize int, duration time.Duration) {
	p.bufferBatchSizeGauge.Set(float64(batchSize))
	p.bufferBatchDurationHistogram.Observe(duration.Seconds())
}

// FanOutStarted sets fanout branches gauge.
func (p *PrometheusMetricsCollector) FanOutStarted(_ context.Context, _ string, numBranches int) {
	p.fanoutBranchesGauge.Set(float64(numBranches))
}

// FanOutCompleted records fanout duration.
func (p *PrometheusMetricsCollector) FanOutCompleted(
	_ context.Context,
	_ string,
	_ int,
	duration time.Duration,
) {
	p.fanoutDurationHistogram.Observe(duration.Seconds())
}

// FanInStarted sets fanin inputs gauge.
func (p *PrometheusMetricsCollector) FanInStarted(_ context.Context, _ string, numInputs int) {
	p.faninInputsGauge.Set(float64(numInputs))
}

// FanInCompleted records fanin duration.
func (p *PrometheusMetricsCollector) FanInCompleted(
	_ context.Context,
	_ string,
	_ int,
	duration time.Duration,
) {
	p.faninDurationHistogram.Observe(duration.Seconds())
}

// PipelineStarted increments pipeline started counter.
func (p *PrometheusMetricsCollector) PipelineStarted(_ context.Context, _ string) {
	p.pipelineStartedCounter.Inc()
}

// PipelineCompleted records pipeline duration.
func (p *PrometheusMetricsCollector) PipelineCompleted(
	_ context.Context,
	_ string,
	duration time.Duration,
	_ error,
) {
	p.pipelineDurationHistogram.Observe(duration.Seconds())
}

// StageWorkerConcurrency sets stage concurrency gauge.
func (p *PrometheusMetricsCollector) StageWorkerConcurrency(
	_ context.Context,
	_ string,
	concurrencyLevel int,
) {
	p.stageConcurrencyGauge.Set(float64(concurrencyLevel))
}

// StageWorkerItemProcessed records item processing duration.
func (p *PrometheusMetricsCollector) StageWorkerItemProcessed(
	_ context.Context,
	_ string,
	duration time.Duration,
) {
	p.stageItemDurationHistogram.Observe(duration.Seconds())
}

// StageWorkerItemSkipped increments items skipped counter.
func (p *PrometheusMetricsCollector) StageWorkerItemSkipped(_ context.Context, _ string, _ error) {
	p.stageItemsSkippedCounter.Inc()
}

// StageWorkerErrorSent increments errors sent counter.
func (p *PrometheusMetricsCollector) StageWorkerErrorSent(_ context.Context, _ string, _ error) {
	p.stageErrorsSentCounter.Inc()
}

// WindowEmitted sets window items gauge.
func (p *PrometheusMetricsCollector) WindowEmitted(_ context.Context, _ string, itemCountInWindow int) {
	p.windowItemsGauge.Set(float64(itemCountInWindow))
}

// GetRegistry returns the Prometheus registry for exposing metrics.
func (p *PrometheusMetricsCollector) GetRegistry() *prometheus.Registry {
	return p.registry
}

// TimeoutOccurred records timeout events for Prometheus.
func (p *PrometheusMetricsCollector) TimeoutOccurred(
	_ context.Context,
	_ string,
	_ time.Duration,
	_ time.Duration,
) {
	// Could add a timeout-specific counter if needed
}

// InfluxDBMetricsCollector implements MetricsCollector for InfluxDB.
type InfluxDBMetricsCollector struct {
	client   influxdb2.Client
	writeAPI api.WriteAPI
	endpoint string
}

// Ensure InfluxDBMetricsCollector implements MetricsCollector.
var _ MetricsCollector = (*InfluxDBMetricsCollector)(nil)

// writePoint writes a metric point to InfluxDB.
func (i *InfluxDBMetricsCollector) writePoint(
	measurement string,
	tags map[string]string,
	fields map[string]any,
) {
	p := influxdb2.NewPoint(measurement, tags, fields, time.Now())
	i.writeAPI.WritePoint(p)
}

// StageStarted writes stage started metric to InfluxDB.
func (i *InfluxDBMetricsCollector) StageStarted(_ context.Context, stageName string) {
	i.writePoint("fluxus_stage_started",
		map[string]string{"stage": stageName},
		map[string]any{"count": 1})
}

// StageCompleted writes stage completion metric to InfluxDB.
func (i *InfluxDBMetricsCollector) StageCompleted(_ context.Context, stageName string, duration time.Duration) {
	i.writePoint("fluxus_stage_duration",
		map[string]string{"stage": stageName},
		map[string]any{"duration_seconds": duration.Seconds()})
}

// StageError writes stage error metric to InfluxDB.
func (i *InfluxDBMetricsCollector) StageError(_ context.Context, stageName string, err error) {
	i.writePoint("fluxus_stage_errors",
		map[string]string{"stage": stageName, "error_type": fmt.Sprintf("%T", err)},
		map[string]any{"count": 1, "error_message": err.Error()})
}

// RetryAttempt writes retry attempt metric to InfluxDB.
func (i *InfluxDBMetricsCollector) RetryAttempt(_ context.Context, stageName string, attempt int, err error) {
	i.writePoint("fluxus_retry_attempts",
		map[string]string{"stage": stageName, "error_type": fmt.Sprintf("%T", err)},
		map[string]any{"attempt": attempt, "count": 1, "error_message": err.Error()})
}

// BufferBatchProcessed writes buffer batch metric to InfluxDB.
func (i *InfluxDBMetricsCollector) BufferBatchProcessed(_ context.Context, batchSize int, duration time.Duration) {
	i.writePoint("fluxus_buffer_batch",
		map[string]string{},
		map[string]any{"batch_size": batchSize, "duration_seconds": duration.Seconds()})
}

// MongoDBMetricsCollector implements MetricsCollector for MongoDB.
type MongoDBMetricsCollector struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	endpoint   string
}

// Ensure MongoDBMetricsCollector implements MetricsCollector.
var _ MetricsCollector = (*MongoDBMetricsCollector)(nil)

// insertMetric inserts a metric document into MongoDB.
func (m *MongoDBMetricsCollector) insertMetric(metricType string, stageName string, data map[string]any) {
	doc := bson.M{
		"timestamp":   time.Now(),
		"metric_type": metricType,
		"stage_name":  stageName,
		"data":        data,
	}

	_, err := m.collection.InsertOne(context.Background(), doc)
	if err != nil {
		log.Printf("Failed to insert metric to MongoDB: %v", err)
	}
}

// StageStarted inserts stage started metric to MongoDB.
func (m *MongoDBMetricsCollector) StageStarted(_ context.Context, stageName string) {
	m.insertMetric("stage_started", stageName, map[string]any{"count": 1})
}

// StageCompleted inserts stage completion metric to MongoDB.
func (m *MongoDBMetricsCollector) StageCompleted(_ context.Context, stageName string, duration time.Duration) {
	m.insertMetric("stage_completed", stageName, map[string]any{
		"duration_seconds": duration.Seconds(),
		"duration_ms":      duration.Milliseconds(),
	})
}

// StageError inserts stage error metric to MongoDB.
func (m *MongoDBMetricsCollector) StageError(_ context.Context, stageName string, err error) {
	m.insertMetric("stage_error", stageName, map[string]any{
		"error_message": err.Error(),
		"count":         1,
	})
}

// CircuitStateChanged records circuit breaker state changes for Prometheus.
func (p *PrometheusMetricsCollector) CircuitStateChanged(
	_ context.Context,
	stageName string,
	fromState, toState string,
) {
	stateChangeCounter := promauto.With(p.registry).NewCounterVec(prometheus.CounterOpts{
		Name: "fluxus_circuit_state_changes_total",
		Help: "Total number of circuit breaker state changes",
	}, []string{"stage", "from_state", "to_state"})
	stateChangeCounter.WithLabelValues(stageName, fromState, toState).Inc()
}

func (p *PrometheusMetricsCollector) CircuitBreakerRejected(_ context.Context, stageName string) {
	rejectionCounter := promauto.With(p.registry).NewCounterVec(prometheus.CounterOpts{
		Name: "fluxus_circuit_rejections_total",
		Help: "Total number of circuit breaker rejections",
	}, []string{"stage"})
	rejectionCounter.WithLabelValues(stageName).Inc()
}

func (p *PrometheusMetricsCollector) CircuitBreakerFailureRecorded(
	_ context.Context,
	stageName string,
	failureCount int,
	threshold int,
) {
	failureGauge := promauto.With(p.registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "fluxus_circuit_failures",
		Help: "Current circuit breaker failure count",
	}, []string{"stage"})
	failureGauge.WithLabelValues(stageName).Set(float64(failureCount))

	thresholdGauge := promauto.With(p.registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "fluxus_circuit_failure_threshold",
		Help: "Circuit breaker failure threshold",
	}, []string{"stage"})
	thresholdGauge.WithLabelValues(stageName).Set(float64(threshold))
}

func (p *PrometheusMetricsCollector) CircuitBreakerSuccessRecorded(
	_ context.Context,
	stageName string,
	successCount int,
	_ int,
) {
	successGauge := promauto.With(p.registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "fluxus_circuit_successes",
		Help: "Current circuit breaker success count",
	}, []string{"stage"})
	successGauge.WithLabelValues(stageName).Set(float64(successCount))
}

func (p *PrometheusMetricsCollector) DeadLetterQueueItemSent(
	_ context.Context,
	stageName string,
	_ error,
) {
	dlqCounter := promauto.With(p.registry).NewCounterVec(prometheus.CounterOpts{
		Name: "fluxus_dlq_items_sent_total",
		Help: "Total number of items sent to dead letter queue",
	}, []string{"stage"})
	dlqCounter.WithLabelValues(stageName).Inc()
}

func (p *PrometheusMetricsCollector) DeadLetterQueueHandlerError(
	_ context.Context,
	stageName string,
	_ error,
) {
	dlqErrorCounter := promauto.With(p.registry).NewCounterVec(prometheus.CounterOpts{
		Name: "fluxus_dlq_handler_errors_total",
		Help: "Total number of dead letter queue handler errors",
	}, []string{"stage"})
	dlqErrorCounter.WithLabelValues(stageName).Inc()
}

func (p *PrometheusMetricsCollector) RouterRoutesSelected(
	_ context.Context,
	stageName string,
	numRoutesSelected int,
	totalRoutes int,
) {
	routesSelectedGauge := promauto.With(p.registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "fluxus_router_routes_selected",
		Help: "Number of routes selected by router",
	}, []string{"stage"})
	routesSelectedGauge.WithLabelValues(stageName).Set(float64(numRoutesSelected))

	totalRoutesGauge := promauto.With(p.registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "fluxus_router_total_routes",
		Help: "Total number of routes available in router",
	}, []string{"stage"})
	totalRoutesGauge.WithLabelValues(stageName).Set(float64(totalRoutes))
}

func (p *PrometheusMetricsCollector) RouterNoRouteMatched(_ context.Context, stageName string) {
	noRouteCounter := promauto.With(p.registry).NewCounterVec(prometheus.CounterOpts{
		Name: "fluxus_router_no_routes_total",
		Help: "Total number of times no routes matched",
	}, []string{"stage"})
	noRouteCounter.WithLabelValues(stageName).Inc()
}

func (p *PrometheusMetricsCollector) RouterRouteProcessed(
	_ context.Context,
	stageName string,
	routeName string,
	_ int,
	duration time.Duration,
) {
	routeDurationHistogram := promauto.With(p.registry).NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxus_router_route_duration_seconds",
		Help: "Duration of route processing in seconds",
	}, []string{"stage", "route"})
	routeDurationHistogram.WithLabelValues(stageName, routeName).Observe(duration.Seconds())
}

func (p *PrometheusMetricsCollector) MapItemProcessed(
	_ context.Context,
	stageName string,
	_ int,
	duration time.Duration,
) {
	mapItemDurationHistogram := promauto.With(p.registry).NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxus_map_item_duration_seconds",
		Help: "Duration of map item processing in seconds",
	}, []string{"stage"})
	mapItemDurationHistogram.WithLabelValues(stageName).Observe(duration.Seconds())
}

func (p *PrometheusMetricsCollector) MapItemError(_ context.Context, stageName string, _ int, _ error) {
	mapErrorCounter := promauto.With(p.registry).NewCounterVec(prometheus.CounterOpts{
		Name: "fluxus_map_item_errors_total",
		Help: "Total number of map item errors",
	}, []string{"stage"})
	mapErrorCounter.WithLabelValues(stageName).Inc()
}

func (p *PrometheusMetricsCollector) MapConcurrencyLevel(
	_ context.Context,
	stageName string,
	concurrencyLevel int,
	totalItems int,
) {
	mapConcurrencyGauge := promauto.With(p.registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "fluxus_map_concurrency",
		Help: "Map stage concurrency level",
	}, []string{"stage"})
	mapConcurrencyGauge.WithLabelValues(stageName).Set(float64(concurrencyLevel))

	mapItemsGauge := promauto.With(p.registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "fluxus_map_total_items",
		Help: "Total items being processed by map stage",
	}, []string{"stage"})
	mapItemsGauge.WithLabelValues(stageName).Set(float64(totalItems))
}

func (p *PrometheusMetricsCollector) MapReduceMapPhaseCompleted(
	_ context.Context,
	stageName string,
	_ int,
	numKeys int,
	duration time.Duration,
) {
	mapPhaseDuration := promauto.With(p.registry).NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxus_mapreduce_map_duration_seconds",
		Help: "Duration of map-reduce map phase in seconds",
	}, []string{"stage"})
	mapPhaseDuration.WithLabelValues(stageName).Observe(duration.Seconds())

	mapKeysGauge := promauto.With(p.registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "fluxus_mapreduce_map_keys",
		Help: "Number of keys produced in map phase",
	}, []string{"stage"})
	mapKeysGauge.WithLabelValues(stageName).Set(float64(numKeys))
}

func (p *PrometheusMetricsCollector) MapReduceShufflePhaseCompleted(
	_ context.Context,
	stageName string,
	_ int,
	duration time.Duration,
) {
	shufflePhaseDuration := promauto.With(p.registry).NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxus_mapreduce_shuffle_duration_seconds",
		Help: "Duration of map-reduce shuffle phase in seconds",
	}, []string{"stage"})
	shufflePhaseDuration.WithLabelValues(stageName).Observe(duration.Seconds())
}

func (p *PrometheusMetricsCollector) MapReduceReducePhaseCompleted(
	_ context.Context,
	stageName string,
	_ int,
	numResults int,
	duration time.Duration,
) {
	reducePhaseDuration := promauto.With(p.registry).NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxus_mapreduce_reduce_duration_seconds",
		Help: "Duration of map-reduce reduce phase in seconds",
	}, []string{"stage"})
	reducePhaseDuration.WithLabelValues(stageName).Observe(duration.Seconds())

	reduceResultsGauge := promauto.With(p.registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "fluxus_mapreduce_reduce_results",
		Help: "Number of results produced in reduce phase",
	}, []string{"stage"})
	reduceResultsGauge.WithLabelValues(stageName).Set(float64(numResults))
}

func (p *PrometheusMetricsCollector) MapReduceKeyGroupSize(
	_ context.Context,
	stageName string,
	_ string,
	groupSize int,
) {
	groupSizeHistogram := promauto.With(p.registry).NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxus_mapreduce_group_size",
		Help: "Size of map-reduce key groups",
	}, []string{"stage"})
	groupSizeHistogram.WithLabelValues(stageName).Observe(float64(groupSize))
}

func (p *PrometheusMetricsCollector) FilterItemPassed(_ context.Context, stageName string) {
	filterPassedCounter := promauto.With(p.registry).NewCounterVec(prometheus.CounterOpts{
		Name: "fluxus_filter_items_passed_total",
		Help: "Total number of items that passed filter",
	}, []string{"stage"})
	filterPassedCounter.WithLabelValues(stageName).Inc()
}

func (p *PrometheusMetricsCollector) FilterItemDropped(_ context.Context, stageName string) {
	filterDroppedCounter := promauto.With(p.registry).NewCounterVec(prometheus.CounterOpts{
		Name: "fluxus_filter_items_dropped_total",
		Help: "Total number of items dropped by filter",
	}, []string{"stage"})
	filterDroppedCounter.WithLabelValues(stageName).Inc()
}

func (p *PrometheusMetricsCollector) FilterPredicateError(_ context.Context, stageName string, _ error) {
	filterErrorCounter := promauto.With(p.registry).NewCounterVec(prometheus.CounterOpts{
		Name: "fluxus_filter_predicate_errors_total",
		Help: "Total number of filter predicate errors",
	}, []string{"stage"})
	filterErrorCounter.WithLabelValues(stageName).Inc()
}

func (p *PrometheusMetricsCollector) JoinByKeyGroupCreated(
	_ context.Context,
	stageName string,
	_ string,
	groupSize int,
) {
	joinGroupSizeHistogram := promauto.With(p.registry).NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxus_joinbykey_group_size",
		Help: "Size of join-by-key groups",
	}, []string{"stage"})
	joinGroupSizeHistogram.WithLabelValues(stageName).Observe(float64(groupSize))
}

func (p *PrometheusMetricsCollector) JoinByKeyCompleted(
	_ context.Context,
	stageName string,
	numKeys int,
	_ int,
	duration time.Duration,
) {
	joinDurationHistogram := promauto.With(p.registry).NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxus_joinbykey_duration_seconds",
		Help: "Duration of join-by-key operation in seconds",
	}, []string{"stage"})
	joinDurationHistogram.WithLabelValues(stageName).Observe(duration.Seconds())

	joinKeysGauge := promauto.With(p.registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "fluxus_joinbykey_keys",
		Help: "Number of keys in join-by-key operation",
	}, []string{"stage"})
	joinKeysGauge.WithLabelValues(stageName).Set(float64(numKeys))
}

func (p *PrometheusMetricsCollector) CustomStageMetric(
	_ context.Context,
	stageName string,
	metricName string,
	value any,
) {
	customGauge := promauto.With(p.registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: fmt.Sprintf("fluxus_custom_%s", metricName),
		Help: fmt.Sprintf("Custom metric: %s", metricName),
	}, []string{"stage"})

	if floatVal, ok := value.(float64); ok {
		customGauge.WithLabelValues(stageName).Set(floatVal)
	} else if intVal, okInt := value.(int); okInt {
		customGauge.WithLabelValues(stageName).Set(float64(intVal))
	}
}

func (p *PrometheusMetricsCollector) CustomStageEvent(
	_ context.Context,
	stageName string,
	eventName string,
	_ map[string]any,
) {
	customEventCounter := promauto.With(p.registry).NewCounterVec(prometheus.CounterOpts{
		Name: "fluxus_custom_events_total",
		Help: "Total number of custom stage events",
	}, []string{"stage", "event"})
	customEventCounter.WithLabelValues(stageName, eventName).Inc()
}

// FanOutStarted records fan-out start events for InfluxDB.
func (i *InfluxDBMetricsCollector) FanOutStarted(_ context.Context, stageName string, numBranches int) {
	i.writePoint(
		"fluxus_fanout_started",
		map[string]string{"stage": stageName},
		map[string]any{"branches": numBranches},
	)
}

func (i *InfluxDBMetricsCollector) FanOutCompleted(
	_ context.Context,
	stageName string,
	numBranches int,
	duration time.Duration,
) {
	i.writePoint(
		"fluxus_fanout_completed",
		map[string]string{"stage": stageName},
		map[string]any{
			"branches":         numBranches,
			"duration_seconds": duration.Seconds(),
			"duration_ms":      duration.Milliseconds(),
		},
	)
}
func (i *InfluxDBMetricsCollector) FanInStarted(_ context.Context, stageName string, numInputs int) {
	i.writePoint(
		"fluxus_fanin_started",
		map[string]string{"stage": stageName},
		map[string]any{"inputs": numInputs},
	)
}

func (i *InfluxDBMetricsCollector) FanInCompleted(
	_ context.Context,
	stageName string,
	numInputs int,
	duration time.Duration,
) {
	i.writePoint(
		"fluxus_fanin_completed",
		map[string]string{"stage": stageName},
		map[string]any{
			"inputs":           numInputs,
			"duration_seconds": duration.Seconds(),
			"duration_ms":      duration.Milliseconds(),
		},
	)
}
func (i *InfluxDBMetricsCollector) PipelineStarted(_ context.Context, pipelineName string) {
	i.writePoint(
		"fluxus_pipeline_started",
		map[string]string{"pipeline": pipelineName},
		map[string]any{"count": 1},
	)
}

func (i *InfluxDBMetricsCollector) PipelineCompleted(
	_ context.Context,
	pipelineName string,
	duration time.Duration,
	err error,
) {
	status := "success"
	fields := map[string]any{"duration_seconds": duration.Seconds(), "duration_ms": duration.Milliseconds()}
	if err != nil {
		status = "error"
		fields["error_message"] = err.Error()
	}
	i.writePoint("fluxus_pipeline_completed", map[string]string{"pipeline": pipelineName, "status": status}, fields)
}
func (i *InfluxDBMetricsCollector) StageWorkerConcurrency(_ context.Context, stageName string, concurrencyLevel int) {
	i.writePoint(
		"fluxus_stage_concurrency",
		map[string]string{"stage": stageName},
		map[string]any{"level": concurrencyLevel},
	)
}

func (i *InfluxDBMetricsCollector) StageWorkerItemProcessed(
	_ context.Context,
	stageName string,
	duration time.Duration,
) {
	i.writePoint(
		"fluxus_stage_item_processed",
		map[string]string{"stage": stageName},
		map[string]any{"duration_seconds": duration.Seconds(), "duration_ms": duration.Milliseconds()},
	)
}
func (i *InfluxDBMetricsCollector) StageWorkerItemSkipped(_ context.Context, stageName string, err error) {
	i.writePoint(
		"fluxus_stage_item_skipped",
		map[string]string{"stage": stageName},
		map[string]any{"count": 1, "error_message": err.Error()},
	)
}
func (i *InfluxDBMetricsCollector) StageWorkerErrorSent(_ context.Context, stageName string, err error) {
	i.writePoint(
		"fluxus_stage_error_sent",
		map[string]string{"stage": stageName},
		map[string]any{"count": 1, "error_message": err.Error()},
	)
}
func (i *InfluxDBMetricsCollector) WindowEmitted(_ context.Context, stageName string, itemCountInWindow int) {
	i.writePoint(
		"fluxus_window_emitted",
		map[string]string{"stage": stageName},
		map[string]any{"item_count": itemCountInWindow},
	)
}

func (i *InfluxDBMetricsCollector) TimeoutOccurred(
	_ context.Context,
	stageName string,
	configuredTimeout time.Duration,
	actualDuration time.Duration,
) {
	i.writePoint("fluxus_timeout_occurred", map[string]string{"stage": stageName}, map[string]any{
		"configured_timeout_ms": configuredTimeout.Milliseconds(),
		"actual_duration_ms":    actualDuration.Milliseconds(),
		"exceeded_by_ms":        actualDuration.Milliseconds() - configuredTimeout.Milliseconds(),
		"count":                 1,
	})
}

func (i *InfluxDBMetricsCollector) CircuitStateChanged(
	_ context.Context,
	stageName string,
	fromState, toState string,
) {
	i.writePoint(
		"fluxus_circuit_state_changed",
		map[string]string{"stage": stageName, "from_state": fromState, "to_state": toState},
		map[string]any{"count": 1},
	)
}

func (i *InfluxDBMetricsCollector) CircuitBreakerRejected(_ context.Context, stageName string) {
	i.writePoint(
		"fluxus_circuit_breaker_rejected",
		map[string]string{"stage": stageName},
		map[string]any{"count": 1},
	)
}

func (i *InfluxDBMetricsCollector) CircuitBreakerFailureRecorded(
	_ context.Context,
	stageName string,
	failureCount int,
	threshold int,
) {
	i.writePoint("fluxus_circuit_breaker_failure", map[string]string{"stage": stageName}, map[string]any{
		"failure_count": failureCount,
		"threshold":     threshold,
		"percentage":    float64(failureCount) / float64(threshold) * 100,
	})
}

func (i *InfluxDBMetricsCollector) CircuitBreakerSuccessRecorded(
	_ context.Context,
	stageName string,
	successCount int,
	threshold int,
) {
	i.writePoint("fluxus_circuit_breaker_success", map[string]string{"stage": stageName}, map[string]any{
		"success_count": successCount,
		"threshold":     threshold,
		"percentage":    float64(successCount) / float64(threshold) * 100,
	})
}

func (i *InfluxDBMetricsCollector) DeadLetterQueueItemSent(_ context.Context, stageName string, originalError error) {
	i.writePoint("fluxus_dlq_item_sent", map[string]string{"stage": stageName}, map[string]any{
		"count":          1,
		"original_error": originalError.Error(),
	})
}

func (i *InfluxDBMetricsCollector) DeadLetterQueueHandlerError(_ context.Context, stageName string, dlqError error) {
	i.writePoint("fluxus_dlq_handler_error", map[string]string{"stage": stageName}, map[string]any{
		"count":     1,
		"dlq_error": dlqError.Error(),
	})
}

func (i *InfluxDBMetricsCollector) RouterRoutesSelected(
	_ context.Context,
	stageName string,
	numRoutesSelected int,
	totalRoutes int,
) {
	i.writePoint("fluxus_router_routes_selected", map[string]string{"stage": stageName}, map[string]any{
		"routes_selected": numRoutesSelected,
		"total_routes":    totalRoutes,
		"selection_ratio": float64(numRoutesSelected) / float64(totalRoutes),
	})
}

func (i *InfluxDBMetricsCollector) RouterNoRouteMatched(_ context.Context, stageName string) {
	i.writePoint(
		"fluxus_router_no_route_matched",
		map[string]string{"stage": stageName},
		map[string]any{"count": 1},
	)
}

func (i *InfluxDBMetricsCollector) RouterRouteProcessed(
	_ context.Context,
	stageName string,
	routeName string,
	routeIndex int,
	duration time.Duration,
) {
	i.writePoint(
		"fluxus_router_route_processed",
		map[string]string{"stage": stageName, "route": routeName},
		map[string]any{
			"route_index":      routeIndex,
			"duration_seconds": duration.Seconds(),
			"duration_ms":      duration.Milliseconds(),
		},
	)
}

func (i *InfluxDBMetricsCollector) MapItemProcessed(
	_ context.Context,
	stageName string,
	itemIndex int,
	duration time.Duration,
) {
	i.writePoint("fluxus_map_item_processed", map[string]string{"stage": stageName}, map[string]any{
		"item_index":       itemIndex,
		"duration_seconds": duration.Seconds(),
		"duration_ms":      duration.Milliseconds(),
	})
}

func (i *InfluxDBMetricsCollector) MapItemError(_ context.Context, stageName string, itemIndex int, err error) {
	i.writePoint("fluxus_map_item_error", map[string]string{"stage": stageName}, map[string]any{
		"item_index":    itemIndex,
		"error_message": err.Error(),
		"count":         1,
	})
}

func (i *InfluxDBMetricsCollector) MapConcurrencyLevel(
	_ context.Context,
	stageName string,
	concurrencyLevel int,
	totalItems int,
) {
	i.writePoint("fluxus_map_concurrency", map[string]string{"stage": stageName}, map[string]any{
		"concurrency_level": concurrencyLevel,
		"total_items":       totalItems,
		"items_per_worker":  float64(totalItems) / float64(concurrencyLevel),
	})
}

func (i *InfluxDBMetricsCollector) MapReduceMapPhaseCompleted(
	_ context.Context,
	stageName string,
	numItems int,
	numKeys int,
	duration time.Duration,
) {
	i.writePoint("fluxus_mapreduce_map_phase", map[string]string{"stage": stageName}, map[string]any{
		"num_items":                numItems,
		"num_keys":                 numKeys,
		"duration_seconds":         duration.Seconds(),
		"duration_ms":              duration.Milliseconds(),
		"items_per_key":            float64(numItems) / float64(numKeys),
		"throughput_items_per_sec": float64(numItems) / duration.Seconds(),
	})
}

func (i *InfluxDBMetricsCollector) MapReduceShufflePhaseCompleted(
	_ context.Context,
	stageName string,
	numKeys int,
	duration time.Duration,
) {
	i.writePoint("fluxus_mapreduce_shuffle_phase", map[string]string{"stage": stageName}, map[string]any{
		"num_keys":                numKeys,
		"duration_seconds":        duration.Seconds(),
		"duration_ms":             duration.Milliseconds(),
		"throughput_keys_per_sec": float64(numKeys) / duration.Seconds(),
	})
}

func (i *InfluxDBMetricsCollector) MapReduceReducePhaseCompleted(
	_ context.Context,
	stageName string,
	numKeys int,
	numResults int,
	duration time.Duration,
) {
	i.writePoint("fluxus_mapreduce_reduce_phase", map[string]string{"stage": stageName}, map[string]any{
		"num_keys":                   numKeys,
		"num_results":                numResults,
		"duration_seconds":           duration.Seconds(),
		"duration_ms":                duration.Milliseconds(),
		"reduction_ratio":            float64(numResults) / float64(numKeys),
		"throughput_results_per_sec": float64(numResults) / duration.Seconds(),
	})
}

func (i *InfluxDBMetricsCollector) MapReduceKeyGroupSize(
	_ context.Context,
	stageName string,
	key string,
	groupSize int,
) {
	i.writePoint(
		"fluxus_mapreduce_key_group",
		map[string]string{"stage": stageName, "key": key},
		map[string]any{
			"group_size": groupSize,
		},
	)
}

func (i *InfluxDBMetricsCollector) FilterItemPassed(_ context.Context, stageName string) {
	i.writePoint("fluxus_filter_item_passed", map[string]string{"stage": stageName}, map[string]any{"count": 1})
}

func (i *InfluxDBMetricsCollector) FilterItemDropped(_ context.Context, stageName string) {
	i.writePoint(
		"fluxus_filter_item_dropped",
		map[string]string{"stage": stageName},
		map[string]any{"count": 1},
	)
}

func (i *InfluxDBMetricsCollector) FilterPredicateError(_ context.Context, stageName string, err error) {
	i.writePoint("fluxus_filter_predicate_error", map[string]string{"stage": stageName}, map[string]any{
		"count":         1,
		"error_message": err.Error(),
	})
}

func (i *InfluxDBMetricsCollector) JoinByKeyGroupCreated(
	_ context.Context,
	stageName string,
	keyStr string,
	groupSize int,
) {
	i.writePoint(
		"fluxus_joinbykey_group_created",
		map[string]string{"stage": stageName, "key": keyStr},
		map[string]any{
			"group_size": groupSize,
		},
	)
}

func (i *InfluxDBMetricsCollector) JoinByKeyCompleted(
	_ context.Context,
	stageName string,
	numKeys int,
	totalItems int,
	duration time.Duration,
) {
	i.writePoint("fluxus_joinbykey_completed", map[string]string{"stage": stageName}, map[string]any{
		"num_keys":                 numKeys,
		"total_items":              totalItems,
		"duration_seconds":         duration.Seconds(),
		"duration_ms":              duration.Milliseconds(),
		"avg_items_per_key":        float64(totalItems) / float64(numKeys),
		"throughput_items_per_sec": float64(totalItems) / duration.Seconds(),
	})
}

func (i *InfluxDBMetricsCollector) CustomStageMetric(
	_ context.Context,
	stageName string,
	metricName string,
	value any,
) {
	i.writePoint(
		"fluxus_custom_metric",
		map[string]string{"stage": stageName, "metric_name": metricName},
		map[string]any{
			"value": value,
		},
	)
}

func (i *InfluxDBMetricsCollector) CustomStageEvent(
	_ context.Context,
	stageName string,
	eventName string,
	metadata map[string]any,
) {
	fields := map[string]any{"count": 1}
	for k, v := range metadata {
		fields[k] = v
	}
	i.writePoint("fluxus_custom_event", map[string]string{"stage": stageName, "event": eventName}, fields)
}

// RetryAttempt records retry attempts for MongoDB.
func (m *MongoDBMetricsCollector) RetryAttempt(_ context.Context, stageName string, attempt int, err error) {
	m.insertMetric("retry_attempt", stageName, map[string]any{"attempt": attempt, "error": err.Error()})
}
func (m *MongoDBMetricsCollector) BufferBatchProcessed(_ context.Context, batchSize int, duration time.Duration) {
	m.insertMetric(
		"buffer_batch_processed",
		"",
		map[string]any{"batch_size": batchSize, "duration_seconds": duration.Seconds()},
	)
}
func (m *MongoDBMetricsCollector) FanOutStarted(_ context.Context, stageName string, numBranches int) {
	m.insertMetric("fanout_started", stageName, map[string]any{"branches": numBranches})
}

func (m *MongoDBMetricsCollector) FanOutCompleted(
	_ context.Context,
	stageName string,
	numBranches int,
	duration time.Duration,
) {
	m.insertMetric(
		"fanout_completed",
		stageName,
		map[string]any{"branches": numBranches, "duration_seconds": duration.Seconds()},
	)
}
func (m *MongoDBMetricsCollector) FanInStarted(_ context.Context, stageName string, numInputs int) {
	m.insertMetric("fanin_started", stageName, map[string]any{"inputs": numInputs})
}

func (m *MongoDBMetricsCollector) FanInCompleted(
	_ context.Context,
	stageName string,
	numInputs int,
	duration time.Duration,
) {
	m.insertMetric(
		"fanin_completed",
		stageName,
		map[string]any{"inputs": numInputs, "duration_seconds": duration.Seconds()},
	)
}
func (m *MongoDBMetricsCollector) PipelineStarted(_ context.Context, pipelineName string) {
	m.insertMetric("pipeline_started", pipelineName, map[string]any{"count": 1})
}

func (m *MongoDBMetricsCollector) PipelineCompleted(
	_ context.Context,
	pipelineName string,
	duration time.Duration,
	err error,
) {
	data := map[string]any{"duration_seconds": duration.Seconds()}
	if err != nil {
		data["error"] = err.Error()
	}
	m.insertMetric("pipeline_completed", pipelineName, data)
}
func (m *MongoDBMetricsCollector) StageWorkerConcurrency(_ context.Context, stageName string, concurrencyLevel int) {
	m.insertMetric("stage_worker_concurrency", stageName, map[string]any{"level": concurrencyLevel})
}

func (m *MongoDBMetricsCollector) StageWorkerItemProcessed(
	_ context.Context,
	stageName string,
	duration time.Duration,
) {
	m.insertMetric(
		"stage_worker_item_processed",
		stageName,
		map[string]any{"duration_seconds": duration.Seconds()},
	)
}
func (m *MongoDBMetricsCollector) StageWorkerItemSkipped(_ context.Context, stageName string, err error) {
	m.insertMetric("stage_worker_item_skipped", stageName, map[string]any{"error": err.Error()})
}
func (m *MongoDBMetricsCollector) StageWorkerErrorSent(_ context.Context, stageName string, err error) {
	m.insertMetric("stage_worker_error_sent", stageName, map[string]any{"error": err.Error()})
}
func (m *MongoDBMetricsCollector) WindowEmitted(_ context.Context, stageName string, itemCountInWindow int) {
	m.insertMetric("window_emitted", stageName, map[string]any{"item_count": itemCountInWindow})
}

// TimeoutOccurred records timeout events for MongoDB.
func (m *MongoDBMetricsCollector) TimeoutOccurred(
	_ context.Context,
	stageName string,
	configuredTimeout time.Duration,
	actualDuration time.Duration,
) {
	m.insertMetric("timeout_occurred", stageName, map[string]any{
		"configured_timeout_ms": configuredTimeout.Milliseconds(),
		"actual_duration_ms":    actualDuration.Milliseconds(),
		"exceeded_by_ms":        actualDuration.Milliseconds() - configuredTimeout.Milliseconds(),
	})
}

func (m *MongoDBMetricsCollector) CircuitStateChanged(
	_ context.Context,
	stageName string,
	fromState, toState string,
) {
	m.insertMetric("circuit_state_changed", stageName, map[string]any{
		"from_state": fromState,
		"to_state":   toState,
	})
}

func (m *MongoDBMetricsCollector) CircuitBreakerRejected(_ context.Context, stageName string) {
	m.insertMetric("circuit_breaker_rejected", stageName, map[string]any{
		"count": 1,
	})
}

func (m *MongoDBMetricsCollector) CircuitBreakerFailureRecorded(
	_ context.Context,
	stageName string,
	failureCount int,
	threshold int,
) {
	m.insertMetric("circuit_breaker_failure_recorded", stageName, map[string]any{
		"failure_count": failureCount,
		"threshold":     threshold,
		"percentage":    float64(failureCount) / float64(threshold) * 100,
	})
}

func (m *MongoDBMetricsCollector) CircuitBreakerSuccessRecorded(
	_ context.Context,
	stageName string,
	successCount int,
	threshold int,
) {
	m.insertMetric("circuit_breaker_success_recorded", stageName, map[string]any{
		"success_count": successCount,
		"threshold":     threshold,
		"percentage":    float64(successCount) / float64(threshold) * 100,
	})
}

func (m *MongoDBMetricsCollector) DeadLetterQueueItemSent(_ context.Context, stageName string, originalError error) {
	m.insertMetric("dead_letter_queue_item_sent", stageName, map[string]any{
		"original_error": originalError.Error(),
		"count":          1,
	})
}

func (m *MongoDBMetricsCollector) DeadLetterQueueHandlerError(_ context.Context, stageName string, dlqError error) {
	m.insertMetric("dead_letter_queue_handler_error", stageName, map[string]any{
		"dlq_error": dlqError.Error(),
		"count":     1,
	})
}

func (m *MongoDBMetricsCollector) RouterRoutesSelected(
	_ context.Context,
	stageName string,
	numRoutesSelected int,
	totalRoutes int,
) {
	m.insertMetric("router_routes_selected", stageName, map[string]any{
		"routes_selected": numRoutesSelected,
		"total_routes":    totalRoutes,
		"selection_ratio": float64(numRoutesSelected) / float64(totalRoutes),
	})
}

func (m *MongoDBMetricsCollector) RouterNoRouteMatched(_ context.Context, stageName string) {
	m.insertMetric("router_no_route_matched", stageName, map[string]any{
		"count": 1,
	})
}

func (m *MongoDBMetricsCollector) RouterRouteProcessed(
	_ context.Context,
	stageName string,
	routeName string,
	routeIndex int,
	duration time.Duration,
) {
	m.insertMetric("router_route_processed", stageName, map[string]any{
		"route_name":       routeName,
		"route_index":      routeIndex,
		"duration_seconds": duration.Seconds(),
		"duration_ms":      duration.Milliseconds(),
	})
}

func (m *MongoDBMetricsCollector) MapItemProcessed(
	_ context.Context,
	stageName string,
	itemIndex int,
	duration time.Duration,
) {
	m.insertMetric("map_item_processed", stageName, map[string]any{
		"item_index":       itemIndex,
		"duration_seconds": duration.Seconds(),
		"duration_ms":      duration.Milliseconds(),
	})
}

func (m *MongoDBMetricsCollector) MapItemError(_ context.Context, stageName string, itemIndex int, err error) {
	m.insertMetric("map_item_error", stageName, map[string]any{
		"item_index":    itemIndex,
		"error_message": err.Error(),
		"count":         1,
	})
}

func (m *MongoDBMetricsCollector) MapConcurrencyLevel(
	_ context.Context,
	stageName string,
	concurrencyLevel int,
	totalItems int,
) {
	m.insertMetric("map_concurrency_level", stageName, map[string]any{
		"concurrency_level": concurrencyLevel,
		"total_items":       totalItems,
		"items_per_worker":  float64(totalItems) / float64(concurrencyLevel),
	})
}

func (m *MongoDBMetricsCollector) MapReduceMapPhaseCompleted(
	_ context.Context,
	stageName string,
	numItems int,
	numKeys int,
	duration time.Duration,
) {
	m.insertMetric("mapreduce_map_phase_completed", stageName, map[string]any{
		"num_items":                numItems,
		"num_keys":                 numKeys,
		"duration_seconds":         duration.Seconds(),
		"duration_ms":              duration.Milliseconds(),
		"items_per_key":            float64(numItems) / float64(numKeys),
		"throughput_items_per_sec": float64(numItems) / duration.Seconds(),
	})
}

func (m *MongoDBMetricsCollector) MapReduceShufflePhaseCompleted(
	_ context.Context,
	stageName string,
	numKeys int,
	duration time.Duration,
) {
	m.insertMetric("mapreduce_shuffle_phase_completed", stageName, map[string]any{
		"num_keys":                numKeys,
		"duration_seconds":        duration.Seconds(),
		"duration_ms":             duration.Milliseconds(),
		"throughput_keys_per_sec": float64(numKeys) / duration.Seconds(),
	})
}

func (m *MongoDBMetricsCollector) MapReduceReducePhaseCompleted(
	_ context.Context,
	stageName string,
	numKeys int,
	numResults int,
	duration time.Duration,
) {
	m.insertMetric("mapreduce_reduce_phase_completed", stageName, map[string]any{
		"num_keys":                   numKeys,
		"num_results":                numResults,
		"duration_seconds":           duration.Seconds(),
		"duration_ms":                duration.Milliseconds(),
		"reduction_ratio":            float64(numResults) / float64(numKeys),
		"throughput_results_per_sec": float64(numResults) / duration.Seconds(),
	})
}

func (m *MongoDBMetricsCollector) MapReduceKeyGroupSize(
	_ context.Context,
	stageName string,
	key string,
	groupSize int,
) {
	m.insertMetric("mapreduce_key_group_size", stageName, map[string]any{
		"key":        key,
		"group_size": groupSize,
	})
}

func (m *MongoDBMetricsCollector) FilterItemPassed(_ context.Context, stageName string) {
	m.insertMetric("filter_item_passed", stageName, map[string]any{
		"count": 1,
	})
}

func (m *MongoDBMetricsCollector) FilterItemDropped(_ context.Context, stageName string) {
	m.insertMetric("filter_item_dropped", stageName, map[string]any{
		"count": 1,
	})
}

func (m *MongoDBMetricsCollector) FilterPredicateError(_ context.Context, stageName string, err error) {
	m.insertMetric("filter_predicate_error", stageName, map[string]any{
		"error_message": err.Error(),
		"count":         1,
	})
}

func (m *MongoDBMetricsCollector) JoinByKeyGroupCreated(
	_ context.Context,
	stageName string,
	keyStr string,
	groupSize int,
) {
	m.insertMetric("joinbykey_group_created", stageName, map[string]any{
		"key":        keyStr,
		"group_size": groupSize,
	})
}

func (m *MongoDBMetricsCollector) JoinByKeyCompleted(
	_ context.Context,
	stageName string,
	numKeys int,
	totalItems int,
	duration time.Duration,
) {
	m.insertMetric("joinbykey_completed", stageName, map[string]any{
		"num_keys":                 numKeys,
		"total_items":              totalItems,
		"duration_seconds":         duration.Seconds(),
		"duration_ms":              duration.Milliseconds(),
		"avg_items_per_key":        float64(totalItems) / float64(numKeys),
		"throughput_items_per_sec": float64(totalItems) / duration.Seconds(),
	})
}

func (m *MongoDBMetricsCollector) CustomStageMetric(
	_ context.Context,
	stageName string,
	metricName string,
	value interface{},
) {
	m.insertMetric("custom_stage_metric", stageName, map[string]any{
		"metric_name":  metricName,
		"metric_value": value,
	})
}

func (m *MongoDBMetricsCollector) CustomStageEvent(
	_ context.Context,
	stageName string,
	eventName string,
	metadata map[string]any,
) {
	data := map[string]any{
		"event_name": eventName,
		"metadata":   metadata,
		"count":      1,
	}
	m.insertMetric("custom_stage_event", stageName, data)
}

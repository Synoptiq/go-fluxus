package fluxus

import (
	"context"
	"time"
)

// MetricsCollector defines an interface for collecting metrics about pipeline operations.
// This allows for integration with various monitoring systems like Prometheus, StatsD, etc.
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
	StageWorkerItemProcessed(
		ctx context.Context,
		stageName string,
		duration time.Duration,
	) // Added (Renamed from Adapter*)
	// StageWorkerItemSkipped reports an item skipped due to an error with SkipOnError strategy.
	StageWorkerItemSkipped(ctx context.Context, stageName string, err error) // Added (Renamed from Adapter*)
	// StageWorkerErrorSent reports an error sent to an error channel via SendToErrorChannel strategy.
	StageWorkerErrorSent(ctx context.Context, stageName string, err error) // Added (Renamed from Adapter*)
}

// NoopMetricsCollector is a metrics collector that does nothing.
// It's useful as a default when no metrics collection is needed.
type NoopMetricsCollector struct{}

// Ensure NoopMetricsCollector implements MetricsCollector
var _ MetricsCollector = (*NoopMetricsCollector)(nil)

// StageStarted implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) StageStarted(_ context.Context, _ string) {}

// StageCompleted implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) StageCompleted(_ context.Context, _ string, _ time.Duration) {}

// StageError implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) StageError(_ context.Context, _ string, _ error) {}

// RetryAttempt implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) RetryAttempt(_ context.Context, _ string, _ int, _ error) {}

// BufferBatchProcessed implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) BufferBatchProcessed(_ context.Context, _ int, _ time.Duration) {}

// FanOutStarted implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) FanOutStarted(_ context.Context, _ string, _ int) {}

// FanOutCompleted implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) FanOutCompleted(_ context.Context, _ string, _ int, _ time.Duration) {}

// FanInStarted implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) FanInStarted(_ context.Context, _ string, _ int) {}

// FanInCompleted implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) FanInCompleted(_ context.Context, _ string, _ int, _ time.Duration) {}

// PipelineStarted implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) PipelineStarted(_ context.Context, _ string) {}

// PipelineCompleted implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) PipelineCompleted(_ context.Context, _ string, _ time.Duration, _ error) {
}

// StageWorkerConcurrency implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) StageWorkerConcurrency(_ context.Context, _ string, _ int) {}

// StageWorkerItemProcessed implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) StageWorkerItemProcessed(_ context.Context, _ string, _ time.Duration) {}

// StageWorkerItemSkipped implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) StageWorkerItemSkipped(_ context.Context, _ string, _ error) {}

// StageWorkerErrorSent implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) StageWorkerErrorSent(_ context.Context, _ string, _ error) {}

// DefaultMetricsCollector is the default metrics collector used when none is provided.
var DefaultMetricsCollector MetricsCollector = &NoopMetricsCollector{}

// MetricatedPipeline wraps a standard Pipeline with metrics collection.
type MetricatedPipeline[I, O any] struct {
	// The underlying pipeline
	pipeline *Pipeline[I, O] // Embed or hold the original pipeline

	// Name for metrics and logging
	name string

	// Metrics collector
	metricsCollector MetricsCollector
}

// MetricatedPipelineOption is a function that configures a MetricatedPipeline.
type MetricatedPipelineOption[I, O any] func(*MetricatedPipeline[I, O])

// WithPipelineMetricsCollector adds a metrics collector to the metricated pipeline.
func WithPipelineMetricsCollector[I, O any](collector MetricsCollector) MetricatedPipelineOption[I, O] {
	return func(mp *MetricatedPipeline[I, O]) {
		if collector != nil {
			mp.metricsCollector = collector
		}
	}
}

// WithPipelineName adds a name to the metricated pipeline for metrics and logging.
func WithPipelineName[I, O any](name string) MetricatedPipelineOption[I, O] {
	return func(mp *MetricatedPipeline[I, O]) {
		if name != "" {
			mp.name = name
		}
	}
}

// NewMetricatedPipeline creates a new metricated pipeline that wraps an existing Pipeline.
func NewMetricatedPipeline[I, O any](
	pipeline *Pipeline[I, O],
	options ...MetricatedPipelineOption[I, O],
) *MetricatedPipeline[I, O] {
	if pipeline == nil {
		panic("fluxus.NewMetricatedPipeline: pipeline cannot be nil")
	}

	mp := &MetricatedPipeline[I, O]{
		pipeline:         pipeline,
		name:             "metricated_pipeline",   // Default name
		metricsCollector: DefaultMetricsCollector, // Default collector
	}

	// Apply options
	for _, option := range options {
		option(mp)
	}

	return mp
}

// Process implements the core processing logic for the metricated pipeline,
// adding metrics calls around the underlying pipeline's Process method.
func (mp *MetricatedPipeline[I, O]) Process(ctx context.Context, input I) (O, error) {
	// Track starting time for metrics
	startTime := time.Now()
	var zero O // Zero value for error returns

	// Signal pipeline start
	mp.metricsCollector.PipelineStarted(ctx, mp.name)

	// Process the request using the underlying pipeline
	output, err := mp.pipeline.Process(ctx, input)

	// Record pipeline completion metrics
	duration := time.Since(startTime)
	mp.metricsCollector.PipelineCompleted(ctx, mp.name, duration, err)

	// Return the result from the underlying pipeline
	// Note: The underlying pipeline already applied its error handler if configured.
	if err != nil {
		return zero, err // Return zero value for O on error
	}
	return output, nil
}

// WithErrorHandler delegates to the underlying pipeline's WithErrorHandler.
// It returns the MetricatedPipeline for chaining.
func (mp *MetricatedPipeline[I, O]) WithErrorHandler(handler func(error) error) *MetricatedPipeline[I, O] {
	mp.pipeline.WithErrorHandler(handler) // Modify the underlying pipeline
	return mp
}

// MetricatedStage wraps any Stage with metrics collection
type MetricatedStage[I, O any] struct {
	// The underlying stage
	stage Stage[I, O]

	// Name for metrics and logging
	name string

	// Metrics collector
	metricsCollector MetricsCollector
}

// MetricatedStageOption is a function that configures a MetricatedStage.
type MetricatedStageOption[I, O any] func(*MetricatedStage[I, O])

// WithMetricsCollector adds a metrics collector to the metricated stage.
func WithMetricsCollector[I, O any](collector MetricsCollector) MetricatedStageOption[I, O] {
	return func(ms *MetricatedStage[I, O]) {
		ms.metricsCollector = collector
	}
}

// WithMetricsStageName adds a name to the metricated stage for metrics and logging.
func WithMetricsStageName[I, O any](name string) MetricatedStageOption[I, O] {
	return func(ms *MetricatedStage[I, O]) {
		ms.name = name
	}
}

// NewMetricatedStage creates a new metricated stage that wraps another stage.
func NewMetricatedStage[I, O any](
	stage Stage[I, O],
	options ...MetricatedStageOption[I, O],
) *MetricatedStage[I, O] {
	if stage == nil {
		panic("fluxus.NewMetricatedStage: stage cannot be nil")
	}

	ms := &MetricatedStage[I, O]{
		stage:            stage,
		name:             "metricated_stage",
		metricsCollector: DefaultMetricsCollector,
	}

	// Apply options
	for _, option := range options {
		option(ms)
	}

	return ms
}

// Process implements the Stage interface for MetricatedStage.
func (ms *MetricatedStage[I, O]) Process(ctx context.Context, input I) (O, error) {
	// Track starting time for metrics
	startTime := time.Now()

	// Signal stage start
	if ms.metricsCollector != nil {
		ms.metricsCollector.StageStarted(ctx, ms.name)
	}

	// Process the request using the underlying stage
	output, err := ms.stage.Process(ctx, input)

	// Record metrics based on the result
	if ms.metricsCollector != nil {
		if err != nil {
			ms.metricsCollector.StageError(ctx, ms.name, err)
		} else {
			ms.metricsCollector.StageCompleted(ctx, ms.name, time.Since(startTime))
		}
	}

	return output, err
}

// MetricatedFanOutStage wraps a FanOut with additional fan-out specific metrics
type MetricatedFanOutStage[I, O any] struct {
	stage            *FanOut[I, O]
	name             string
	metricsCollector MetricsCollector
}

// NewMetricatedFanOut creates a metricated wrapper around a FanOut stage.
func NewMetricatedFanOut[I, O any](
	fanOut *FanOut[I, O],
	options ...MetricatedStageOption[I, []O],
) Stage[I, []O] {
	if fanOut == nil {
		panic("fluxus.NewMetricatedFanOut: fanOut cannot be nil")
	}

	// Create a proxy for handling options
	ms := &MetricatedStage[I, []O]{
		stage:            fanOut,
		name:             "metricated_fan_out",
		metricsCollector: DefaultMetricsCollector,
	}

	// Apply options
	for _, option := range options {
		option(ms)
	}

	// Create the specialized stage
	return &MetricatedFanOutStage[I, O]{
		stage:            fanOut,
		name:             ms.name,
		metricsCollector: ms.metricsCollector,
	}
}

// Process implements the Stage interface for MetricatedFanOutStage
func (ms *MetricatedFanOutStage[I, O]) Process(ctx context.Context, input I) ([]O, error) {
	// Track starting time for metrics
	startTime := time.Now()

	// Signal stage start
	if ms.metricsCollector != nil {
		ms.metricsCollector.StageStarted(ctx, ms.name)
		ms.metricsCollector.FanOutStarted(ctx, ms.name, len(ms.stage.stages))
	}

	// Process the request using the underlying stage
	output, err := ms.stage.Process(ctx, input)

	// Record metrics based on the result
	if ms.metricsCollector != nil {
		if err != nil {
			ms.metricsCollector.StageError(ctx, ms.name, err)
		} else {
			duration := time.Since(startTime)
			ms.metricsCollector.StageCompleted(ctx, ms.name, duration)
			ms.metricsCollector.FanOutCompleted(ctx, ms.name, len(ms.stage.stages), duration)
		}
	}

	return output, err
}

// MetricatedFanInStage wraps a FanIn with additional fan-in specific metrics
type MetricatedFanInStage[I, O any] struct {
	stage            *FanIn[I, O]
	name             string
	metricsCollector MetricsCollector
}

// NewMetricatedFanIn creates a metricated wrapper around a FanIn stage.
func NewMetricatedFanIn[I, O any](
	fanIn *FanIn[I, O],
	options ...MetricatedStageOption[[]I, O],
) Stage[[]I, O] {
	if fanIn == nil {
		panic("fluxus.NewMetricatedFanIn: fanIn cannot be nil")
	}

	// Create a proxy for handling options
	ms := &MetricatedStage[[]I, O]{
		stage:            fanIn,
		name:             "metricated_fan_in",
		metricsCollector: DefaultMetricsCollector,
	}

	// Apply options
	for _, option := range options {
		option(ms)
	}

	// Create the specialized stage
	return &MetricatedFanInStage[I, O]{
		stage:            fanIn,
		name:             ms.name,
		metricsCollector: ms.metricsCollector,
	}
}

// Process implements the Stage interface for MetricatedFanInStage
func (ms *MetricatedFanInStage[I, O]) Process(ctx context.Context, inputs []I) (O, error) {
	// Track starting time for metrics
	startTime := time.Now()

	// Signal stage start
	if ms.metricsCollector != nil {
		ms.metricsCollector.StageStarted(ctx, ms.name)
		ms.metricsCollector.FanInStarted(ctx, ms.name, len(inputs))
	}

	// Process the request using the underlying stage
	output, err := ms.stage.Process(ctx, inputs)

	// Record metrics based on the result
	if ms.metricsCollector != nil {
		if err != nil {
			ms.metricsCollector.StageError(ctx, ms.name, err)
		} else {
			duration := time.Since(startTime)
			ms.metricsCollector.StageCompleted(ctx, ms.name, duration)
			ms.metricsCollector.FanInCompleted(ctx, ms.name, len(inputs), duration)
		}
	}

	return output, err
}

// MetricatedBufferStage wraps a Buffer with buffer-specific metrics
type MetricatedBufferStage[I, O any] struct {
	stage            *Buffer[I, O]
	name             string
	metricsCollector MetricsCollector
}

// NewMetricatedBuffer creates a metricated wrapper around a Buffer stage.
func NewMetricatedBuffer[I, O any](
	buffer *Buffer[I, O],
	options ...MetricatedStageOption[[]I, []O],
) Stage[[]I, []O] {
	if buffer == nil {
		panic("fluxus.NewMetricatedBuffer: buffer cannot be nil")
	}

	// Create a proxy for handling options
	ms := &MetricatedStage[[]I, []O]{
		stage:            buffer,
		name:             "metricated_buffer",
		metricsCollector: DefaultMetricsCollector,
	}

	// Apply options
	for _, option := range options {
		option(ms)
	}

	// Create the specialized stage
	return &MetricatedBufferStage[I, O]{
		stage:            buffer,
		name:             ms.name,
		metricsCollector: ms.metricsCollector,
	}
}

// Process implements the Stage interface for MetricatedBufferStage
func (ms *MetricatedBufferStage[I, O]) Process(ctx context.Context, inputs []I) ([]O, error) {
	// Track starting time for metrics
	startTime := time.Now()

	// Signal stage start
	if ms.metricsCollector != nil {
		ms.metricsCollector.StageStarted(ctx, ms.name)
	}

	// Process the request using the underlying buffer stage
	// Note: The Buffer will call BufferBatchProcessed internally for each batch
	output, err := ms.stage.Process(ctx, inputs)

	// Record metrics based on the result
	if ms.metricsCollector != nil {
		if err != nil {
			ms.metricsCollector.StageError(ctx, ms.name, err)
		} else {
			ms.metricsCollector.StageCompleted(ctx, ms.name, time.Since(startTime))
		}
	}

	return output, err
}

// MetricatedRetryStage wraps a Retry with additional retry-specific metrics
type MetricatedRetryStage[I, O any] struct {
	stage            *Retry[I, O]
	name             string
	metricsCollector MetricsCollector
}

// NewMetricatedRetry creates a metricated wrapper around a Retry stage.
func NewMetricatedRetry[I, O any](
	retry *Retry[I, O],
	options ...MetricatedStageOption[I, O],
) Stage[I, O] {
	if retry == nil {
		panic("fluxus.NewMetricatedRetry: retry cannot be nil")
	}

	// Create a proxy for handling options
	ms := &MetricatedStage[I, O]{
		stage:            retry,
		name:             "metricated_retry",
		metricsCollector: DefaultMetricsCollector,
	}

	// Apply options
	for _, option := range options {
		option(ms)
	}

	// Create the specialized stage
	return &MetricatedRetryStage[I, O]{
		stage:            retry,
		name:             ms.name,
		metricsCollector: ms.metricsCollector,
	}
}

// Process implements the Stage interface for MetricatedRetryStage
func (ms *MetricatedRetryStage[I, O]) Process(ctx context.Context, input I) (O, error) {
	// Track starting time for metrics
	startTime := time.Now()

	// Signal stage start
	if ms.metricsCollector != nil {
		ms.metricsCollector.StageStarted(ctx, ms.name)
	}

	// Create a counter for tracking attempts
	attemptCount := 0

	// Temporarily replace the original stage with our counted version
	originalStage := ms.stage.stage

	// Create a wrapper stage that counts attempts
	countingStage := StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
		// Increment attempt counter before each attempt
		attemptCount++

		// Record the attempt
		if ms.metricsCollector != nil {
			ms.metricsCollector.RetryAttempt(ctx, ms.name, attemptCount, nil)
		}

		// Forward to the original stage
		return originalStage.Process(ctx, input)
	})

	// Temporarily replace the stage with our counting version
	ms.stage.stage = countingStage

	// Process using the modified retry stage
	output, err := ms.stage.Process(ctx, input)

	// Restore the original stage
	ms.stage.stage = originalStage

	// Record metrics based on the result
	if ms.metricsCollector != nil {
		if err != nil {
			ms.metricsCollector.StageError(ctx, ms.name, err)
		} else {
			ms.metricsCollector.StageCompleted(ctx, ms.name, time.Since(startTime))
		}
	}

	return output, err
}

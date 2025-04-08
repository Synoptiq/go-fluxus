package fluxus

import (
	"context"
	"time"
)

// MetricsCollector defines an interface for collecting metrics about pipeline operations.
// This allows for integration with various monitoring systems like Prometheus, StatsD, etc.
type MetricsCollector interface {
	// StageStarted is called when a stage begins processing.
	StageStarted(ctx context.Context, stageName string)

	// StageCompleted is called when a stage successfully completes processing.
	StageCompleted(ctx context.Context, stageName string, duration time.Duration)

	// StageError is called when a stage encounters an error.
	StageError(ctx context.Context, stageName string, err error)

	// RetryAttempt is called for each retry attempt.
	RetryAttempt(ctx context.Context, stageName string, attempt int, err error)

	// BufferBatchProcessed is called when a batch is processed in a buffer.
	BufferBatchProcessed(ctx context.Context, batchSize int, duration time.Duration)

	// FanOutStarted is called when a fan-out operation begins.
	FanOutStarted(ctx context.Context, numStages int)

	// FanOutCompleted is called when a fan-out operation completes.
	FanOutCompleted(ctx context.Context, numStages int, duration time.Duration)

	// FanInStarted is called when a fan-in operation begins.
	FanInStarted(ctx context.Context, numInputs int)

	// FanInCompleted is called when a fan-in operation completes.
	FanInCompleted(ctx context.Context, numInputs int, duration time.Duration)
}

// NoopMetricsCollector is a metrics collector that does nothing.
// It's useful as a default when no metrics collection is needed.
type NoopMetricsCollector struct{}

// Ensure NoopMetricsCollector implements MetricsCollector
var _ MetricsCollector = (*NoopMetricsCollector)(nil)

// StageStarted implements MetricsCollector.
func (*NoopMetricsCollector) StageStarted(_ context.Context, _ string) {}

// StageCompleted implements MetricsCollector.
func (*NoopMetricsCollector) StageCompleted(_ context.Context, _ string, _ time.Duration) {
}

// StageError implements MetricsCollector.
func (*NoopMetricsCollector) StageError(_ context.Context, _ string, _ error) {}

// RetryAttempt implements MetricsCollector.
func (*NoopMetricsCollector) RetryAttempt(_ context.Context, _ string, _ int, _ error) {
}

// BufferBatchProcessed implements MetricsCollector.
func (*NoopMetricsCollector) BufferBatchProcessed(_ context.Context, _ int, _ time.Duration) {
}

// FanOutStarted implements MetricsCollector.
func (*NoopMetricsCollector) FanOutStarted(_ context.Context, _ int) {}

// FanOutCompleted implements MetricsCollector.
func (*NoopMetricsCollector) FanOutCompleted(_ context.Context, _ int, _ time.Duration) {
}

// FanInStarted implements MetricsCollector.
func (*NoopMetricsCollector) FanInStarted(_ context.Context, _ int) {}

// FanInCompleted implements MetricsCollector.
func (*NoopMetricsCollector) FanInCompleted(_ context.Context, _ int, _ time.Duration) {
}

// DefaultMetricsCollector is the default metrics collector used when none is provided.
var DefaultMetricsCollector MetricsCollector = &NoopMetricsCollector{}

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

// WithStageName adds a name to the metricated stage for metrics and logging.
func WithStageName[I, O any](name string) MetricatedStageOption[I, O] {
	return func(ms *MetricatedStage[I, O]) {
		ms.name = name
	}
}

// NewMetricatedStage creates a new metricated stage that wraps another stage.
func NewMetricatedStage[I, O any](
	stage Stage[I, O],
	options ...MetricatedStageOption[I, O],
) *MetricatedStage[I, O] {
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

// Factory functions for creating metricated versions of specific stages

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
		ms.metricsCollector.FanOutStarted(ctx, len(ms.stage.stages))
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
			ms.metricsCollector.FanOutCompleted(ctx, len(ms.stage.stages), duration)
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
		ms.metricsCollector.FanInStarted(ctx, len(inputs))
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
			ms.metricsCollector.FanInCompleted(ctx, len(inputs), duration)
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

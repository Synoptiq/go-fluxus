package fluxus

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

	// --- Windowing Stage Metrics ---

	// WindowEmitted is called when a windowing stage successfully emits a window.
	// stageName should ideally identify the specific window instance and its type.
	WindowEmitted(ctx context.Context, stageName string, itemCountInWindow int)

	// --- Timeout Stage Metrics ---

	// TimeoutOccurred is called when a stage times out.
	TimeoutOccurred(
		ctx context.Context,
		stageName string,
		configuredTimeout time.Duration,
		actualDuration time.Duration,
	)

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
	RouterRouteProcessed(
		ctx context.Context,
		stageName string,
		routeName string,
		routeIndex int,
		duration time.Duration,
	)

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
	MapReduceReducePhaseCompleted(
		ctx context.Context,
		stageName string,
		numKeys int,
		numResults int,
		duration time.Duration,
	)
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

// WindowEmitted implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) WindowEmitted(_ context.Context, _ string, _ int) {}

// TimeoutOccurred implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) TimeoutOccurred(_ context.Context, _ string, _ time.Duration, _ time.Duration) {
}

// CircuitStateChanged implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) CircuitStateChanged(_ context.Context, _ string, _, _ string) {}

// CircuitBreakerRejected implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) CircuitBreakerRejected(_ context.Context, _ string) {}

// CircuitBreakerFailureRecorded implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) CircuitBreakerFailureRecorded(_ context.Context, _ string, _ int, _ int) {
}

// CircuitBreakerSuccessRecorded implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) CircuitBreakerSuccessRecorded(_ context.Context, _ string, _ int, _ int) {
}

// DeadLetterQueueItemSent implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) DeadLetterQueueItemSent(_ context.Context, _ string, _ error) {}

// DeadLetterQueueHandlerError implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) DeadLetterQueueHandlerError(_ context.Context, _ string, _ error) {}

// RouterRoutesSelected implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) RouterRoutesSelected(_ context.Context, _ string, _ int, _ int) {}

// RouterNoRouteMatched implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) RouterNoRouteMatched(_ context.Context, _ string) {}

// RouterRouteProcessed implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) RouterRouteProcessed(_ context.Context, _ string, _ string, _ int, _ time.Duration) {
}

// MapItemProcessed implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) MapItemProcessed(_ context.Context, _ string, _ int, _ time.Duration) {}

// MapItemError implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) MapItemError(_ context.Context, _ string, _ int, _ error) {}

// MapConcurrencyLevel implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) MapConcurrencyLevel(_ context.Context, _ string, _ int, _ int) {}

// MapReduceMapPhaseCompleted implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) MapReduceMapPhaseCompleted(_ context.Context, _ string, _ int, _ int, _ time.Duration) {
}

// MapReduceShufflePhaseCompleted implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) MapReduceShufflePhaseCompleted(_ context.Context, _ string, _ int, _ time.Duration) {
}

// MapReduceReducePhaseCompleted implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) MapReduceReducePhaseCompleted(_ context.Context, _ string, _ int, _ int, _ time.Duration) {
}

// MapReduceKeyGroupSize implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) MapReduceKeyGroupSize(_ context.Context, _ string, _ string, _ int) {}

// FilterItemPassed implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) FilterItemPassed(_ context.Context, _ string) {}

// FilterItemDropped implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) FilterItemDropped(_ context.Context, _ string) {}

// FilterPredicateError implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) FilterPredicateError(_ context.Context, _ string, _ error) {}

// JoinByKeyGroupCreated implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) JoinByKeyGroupCreated(_ context.Context, _ string, _ string, _ int) {}

// JoinByKeyCompleted implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) JoinByKeyCompleted(_ context.Context, _ string, _ int, _ int, _ time.Duration) {
}

// CustomStageMetric implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) CustomStageMetric(_ context.Context, _ string, _ string, _ interface{}) {
}

// CustomStageEvent implements MetricsCollector interface for NoopMetricsCollector.
func (*NoopMetricsCollector) CustomStageEvent(_ context.Context, _ string, _ string, _ map[string]interface{}) {
}

// DefaultMetricsCollector is the default metrics collector used when none is provided.
var DefaultMetricsCollector MetricsCollector = &NoopMetricsCollector{}

// pipelineMetricsConfig holds configuration for the pipeline metrics wrapper.
type pipelineMetricsConfig[I, O any] struct {
	name             string
	metricsCollector MetricsCollector
}

// MetricatedPipelineOption is a function that configures the pipeline metrics wrapper.
type MetricatedPipelineOption[I, O any] func(*pipelineMetricsConfig[I, O])

// WithPipelineMetricsCollector adds a metrics collector to the metricated pipeline.
func WithPipelineMetricsCollector[I, O any](collector MetricsCollector) MetricatedPipelineOption[I, O] {
	return func(mp *pipelineMetricsConfig[I, O]) {
		if collector != nil {
			mp.metricsCollector = collector
		}
	}
}

// WithPipelineName adds a name to the metricated pipeline for metrics and logging.
func WithPipelineName[I, O any](name string) MetricatedPipelineOption[I, O] {
	return func(mp *pipelineMetricsConfig[I, O]) {
		if name != "" {
			mp.name = name
		}
	}
}

// NewMetricatedPipeline creates a new metricated pipeline that wraps an existing Pipeline.
func NewMetricatedPipeline[I, O any](
	pipelineToWrap *Pipeline[I, O], // Renamed for clarity
	options ...MetricatedPipelineOption[I, O],
) Stage[I, O] { // Returns Stage[I,O]
	if pipelineToWrap == nil {
		panic("fluxus.NewMetricatedPipeline: pipelineToWrap cannot be nil")
	}

	cfg := &pipelineMetricsConfig[I, O]{
		name:             "metricated_pipeline",
		metricsCollector: DefaultMetricsCollector,
	}

	for _, option := range options {
		option(cfg)
	}

	// The returned StageFunc will be the metricated pipeline.
	return StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
		startTime := time.Now()
		var zero O

		collector := cfg.metricsCollector // Use configured collector
		pipelineName := cfg.name          // Use configured name

		if collector != nil {
			collector.PipelineStarted(ctx, pipelineName)
		}

		output, err := pipelineToWrap.Process(ctx, input) // Call the original pipeline

		if collector != nil {
			duration := time.Since(startTime)
			collector.PipelineCompleted(ctx, pipelineName, duration, err)
		}

		if err != nil {
			return zero, err
		}
		return output, nil
	})
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

// MetricatedStreamStage wraps any StreamStage with metrics collection.
type MetricatedStreamStage[T, O any] struct {
	stage            StreamStage[T, O]
	name             string
	metricsCollector MetricsCollector
	// Optional callback for emitted items
	onEmit func(ctx context.Context, stageName string, emittedItem O)
}

// MetricatedStreamStageOption configures a MetricatedStreamStage.
type MetricatedStreamStageOption[T, O any] func(*MetricatedStreamStage[T, O])

// WithMetricsStreamStageName sets the name for the metricated stream stage.
func WithMetricsStreamStageName[T, O any](name string) MetricatedStreamStageOption[T, O] {
	return func(mss *MetricatedStreamStage[T, O]) {
		if name != "" {
			mss.name = name
		}
	}
}

// WithMetricsStreamCollector sets the MetricsCollector for the MetricatedStreamStage.
func WithMetricsStreamCollector[T, O any](collector MetricsCollector) MetricatedStreamStageOption[T, O] {
	return func(mss *MetricatedStreamStage[T, O]) {
		if collector != nil {
			mss.metricsCollector = collector
		}
	}
}

// WithStreamEmitCallback sets a callback to be invoked for each item emitted by the stream stage.
func WithStreamEmitCallback[T, O any](
	onEmit func(ctx context.Context, stageName string, emittedItem O),
) MetricatedStreamStageOption[T, O] {
	return func(mss *MetricatedStreamStage[T, O]) {
		mss.onEmit = onEmit
	}
}

// NewMetricatedStreamStage creates a new MetricatedStreamStage that wraps the given stream stage.
func NewMetricatedStreamStage[T, O any](
	stage StreamStage[T, O],
	options ...MetricatedStreamStageOption[T, O],
) StreamStage[T, O] { // Return StreamStage interface
	if stage == nil {
		panic("fluxus.NewMetricatedStreamStage: stage cannot be nil")
	}

	mss := &MetricatedStreamStage[T, O]{
		stage:            stage,
		name:             "metricated_stream_stage", // Default name
		metricsCollector: DefaultMetricsCollector,
	}
	for _, option := range options {
		option(mss)
	}
	return mss
}

// ProcessStream implements the StreamStage interface, adding metrics.
func (mss *MetricatedStreamStage[T, O]) ProcessStream(ctx context.Context, in <-chan T, out chan<- O) error {
	startTime := time.Now()
	collector := mss.metricsCollector
	stageName := mss.name

	if collector != nil {
		collector.StageStarted(ctx, stageName)
	}

	var err error
	if mss.onEmit == nil {
		// Original behavior: directly call the underlying stage
		err = mss.stage.ProcessStream(ctx, in, out)
	} else {
		// Wrap the output channel to call onEmit for each item
		internalOut := make(chan O) // O is the type of item in the output channel (e.g., []T for windows)
		var wg sync.WaitGroup
		wg.Add(1)
		var processingErr error

		go func() {
			defer wg.Done()
			defer close(internalOut)
			processingErr = mss.stage.ProcessStream(ctx, in, internalOut)
		}()

		done := false
		for !done {
			select {
			case <-ctx.Done():
				err = ctx.Err() // Prioritize context error
				done = true
			case item, ok := <-internalOut:
				if !ok {
					err = processingErr // Get error from the underlying stage processing
					done = true
					continue
				}
				mss.onEmit(ctx, stageName, item) // Call the emit callback
				select {
				case out <- item:
				case <-ctx.Done():
					err = ctx.Err()
					done = true
				}
			}
		}
		wg.Wait()                               // Wait for the processing goroutine to finish
		if err == nil && processingErr != nil { // If no context error but processing had an error
			err = processingErr
		}
	}

	if collector != nil {
		if err != nil {
			collector.StageError(ctx, stageName, err)
		} else {
			collector.StageCompleted(ctx, stageName, time.Since(startTime))
		}
	}

	return err
}

// NewMetricatedFanOut creates a metricated wrapper around a FanOut stage.
func NewMetricatedFanOut[I, O any](
	fanOut *FanOut[I, O],
	options ...MetricatedStageOption[I, []O],
) Stage[I, []O] {
	if fanOut == nil {
		panic("fluxus.NewMetricatedFanOut: fanOut cannot be nil")
	}

	configHolder := &MetricatedStage[I, []O]{
		name:             "metricated_fan_out",
		metricsCollector: DefaultMetricsCollector,
	}

	for _, option := range options {
		option(configHolder)
	}

	collector := configHolder.metricsCollector
	metricName := configHolder.name

	intermediateStage := StageFunc[I, []O](func(ctx context.Context, input I) ([]O, error) {
		fanOutStartTime := time.Now()
		if collector != nil {
			collector.FanOutStarted(ctx, metricName, len(fanOut.stages))
		}

		outputs, err := fanOut.Process(ctx, input)

		if collector != nil && err == nil {
			collector.FanOutCompleted(ctx, metricName, len(fanOut.stages), time.Since(fanOutStartTime))
		}
		return outputs, err
	})

	return NewMetricatedStage(intermediateStage, options...)
}

// NewMetricatedFanIn creates a metricated wrapper around a FanIn stage.
func NewMetricatedFanIn[I, O any](
	fanIn *FanIn[I, O],
	options ...MetricatedStageOption[[]I, O],
) Stage[[]I, O] {
	if fanIn == nil {
		panic("fluxus.NewMetricatedFanIn: fanIn cannot be nil")
	}

	configHolder := &MetricatedStage[[]I, O]{
		name:             "metricated_fan_in",
		metricsCollector: DefaultMetricsCollector,
	}

	for _, option := range options {
		option(configHolder)
	}

	collector := configHolder.metricsCollector
	metricName := configHolder.name

	intermediateStage := StageFunc[[]I, O](func(ctx context.Context, inputs []I) (O, error) {
		fanInStartTime := time.Now()
		if collector != nil {
			collector.FanInStarted(ctx, metricName, len(inputs))
		}

		output, err := fanIn.Process(ctx, inputs)

		if collector != nil && err == nil {
			collector.FanInCompleted(ctx, metricName, len(inputs), time.Since(fanInStartTime))
		}
		return output, err
	})

	return NewMetricatedStage(intermediateStage, options...)
}

// NewMetricatedBuffer creates a metricated wrapper around a Buffer stage.
func NewMetricatedBuffer[I, O any](
	buffer *Buffer[I, O],
	options ...MetricatedStageOption[[]I, []O],
) Stage[[]I, []O] {
	if buffer == nil {
		panic("fluxus.NewMetricatedBuffer: buffer cannot be nil")
	}

	// Buffer stage calls BufferBatchProcessed internally.
	// So, NewMetricatedBuffer can directly use NewMetricatedStage to wrap the buffer.
	// The options will configure the name and collector for the generic MetricatedStage.
	// The default name can be set here if not provided by options.

	// Apply options to a temporary config holder to get the final name and collector.
	configHolder := &MetricatedStage[[]I, []O]{
		name:             "metricated_buffer",
		metricsCollector: DefaultMetricsCollector,
	}

	for _, option := range options {
		option(configHolder)
	}

	// Construct the final options to pass to NewMetricatedStage
	finalOptions := []MetricatedStageOption[[]I, []O]{
		WithMetricsStageName[[]I, []O](configHolder.name),
		WithMetricsCollector[[]I, []O](configHolder.metricsCollector),
	}
	// Add any other options the user might have passed that are generic.

	return NewMetricatedStage(buffer, finalOptions...)
}

// NewMetricatedRetry creates a metricated wrapper around a Retry stage.
func NewMetricatedRetry[I, O any](
	retry *Retry[I, O],
	options ...MetricatedStageOption[I, O],
) Stage[I, O] {
	if retry == nil {
		panic("fluxus.NewMetricatedRetry: retry cannot be nil")
	}

	configHolder := &MetricatedStage[I, O]{
		name:             "metricated_retry",
		metricsCollector: DefaultMetricsCollector,
	}

	for _, option := range options {
		option(configHolder)
	}
	collector := configHolder.metricsCollector
	metricName := configHolder.name

	// Return a StageFunc that implements the retry metrics logic
	return StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
		startTime := time.Now()
		if collector != nil {
			collector.StageStarted(ctx, metricName)
		}

		attemptCount := 0
		originalInternalStage := retry.stage // The stage *inside* the Retry struct

		// Wrapper for the internal stage to count and report attempts
		metricatedInternalAttemptStage := StageFunc[I, O](func(attemptCtx context.Context, attemptInput I) (O, error) {
			attemptCount++
			// Call Process on originalInternalStage first to get the error for RetryAttempt
			output, err := originalInternalStage.Process(attemptCtx, attemptInput)
			if collector != nil {
				collector.RetryAttempt(ctx, metricName, attemptCount, err) // Pass the error from the attempt
			}
			return output, err
		})

		// Temporarily replace the Retry stage's internal stage
		retry.stage = metricatedInternalAttemptStage

		output, err := retry.Process(ctx, input) // Call the original Retry stage's Process method

		// Restore the original internal stage
		retry.stage = originalInternalStage

		if collector != nil {
			if err != nil {
				collector.StageError(ctx, metricName, err)
			} else {
				collector.StageCompleted(ctx, metricName, time.Since(startTime))
			}
		}
		return output, err
	})
}

// NewMetricatedMap creates a metricated wrapper around a Map stage.
func NewMetricatedMap[I, O any](
	mapStage *Map[I, O],
	options ...MetricatedStageOption[[]I, []O], // Map's Process takes []I and returns []O
) Stage[[]I, []O] {
	if mapStage == nil {
		panic("fluxus.NewMetricatedMap: mapStage cannot be nil")
	}

	configHolder := &MetricatedStage[[]I, []O]{
		name:             "metricated_map",
		metricsCollector: DefaultMetricsCollector,
	}

	for _, option := range options {
		option(configHolder)
	}

	collector := configHolder.metricsCollector
	metricName := configHolder.name

	// Return a StageFunc that implements the map metrics logic
	return StageFunc[[]I, []O](func(ctx context.Context, inputs []I) ([]O, error) {
		startTime := time.Now()
		if collector != nil {
			collector.StageStarted(ctx, metricName)
		}

		// Report concurrency level before processing
		totalItems := len(inputs)
		actualConcurrency := mapStage.concurrency
		if actualConcurrency > totalItems {
			actualConcurrency = totalItems
		}
		if collector != nil {
			collector.MapConcurrencyLevel(ctx, metricName, actualConcurrency, totalItems)
		}

		// We need to wrap the map's internal stage to capture per-item metrics
		originalStage := mapStage.stage
		mapStage.stage = StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
			itemStartTime := time.Now()
			output, err := originalStage.Process(ctx, input)
			itemDuration := time.Since(itemStartTime)

			// We need a way to get the current item index, but Map doesn't expose it
			// For now, we'll use -1 as a placeholder and track per-item in the aggregate
			if collector != nil {
				if err != nil {
					collector.MapItemError(ctx, metricName, -1, err)
				} else {
					collector.MapItemProcessed(ctx, metricName, -1, itemDuration)
				}
			}
			return output, err
		})

		outputs, err := mapStage.Process(ctx, inputs)
		duration := time.Since(startTime)

		// Restore original stage
		mapStage.stage = originalStage

		if collector != nil {
			if err != nil {
				collector.StageError(ctx, metricName, err)
			} else {
				collector.StageCompleted(ctx, metricName, duration)
			}
		}
		return outputs, err
	})
}

// NewMetricatedMapReduce creates a metricated wrapper around a MapReduce stage.
//
//nolint:gocognit // Complex wrapper function justified for comprehensive metrics
func NewMetricatedMapReduce[I any, K comparable, V any, R any](
	mapReduceStage *MapReduce[I, K, V, R],
	options ...MetricatedStageOption[[]I, []R], // MapReduce's Process takes []I and returns []R
) Stage[[]I, []R] {
	if mapReduceStage == nil {
		panic("fluxus.NewMetricatedMapReduce: mapReduceStage cannot be nil")
	}

	configHolder := &MetricatedStage[[]I, []R]{
		name:             "metricated_map_reduce",
		metricsCollector: DefaultMetricsCollector,
	}

	for _, option := range options {
		option(configHolder)
	}

	collector := configHolder.metricsCollector
	metricName := configHolder.name

	// Return a StageFunc that implements the MapReduce metrics logic
	return StageFunc[[]I, []R](func(ctx context.Context, inputs []I) ([]R, error) {
		startTime := time.Now()
		if collector != nil {
			collector.StageStarted(ctx, metricName)
		}

		// We need to wrap the MapReduce methods to capture phase metrics
		// Since we can't modify the private methods directly, we'll create a new
		// MapReduce with wrapped mapper and reducer functions
		originalMapper := mapReduceStage.mapper
		originalReducer := mapReduceStage.reducer

		numItems := len(inputs)
		var mapPhaseStartTime time.Time
		var mapPhaseKeys int
		var reducePhaseStartTime time.Time

		// Wrap mapper to track map phase
		wrappedMapper := func(ctx context.Context, input I) (KeyValue[K, V], error) {
			if mapPhaseStartTime.IsZero() {
				mapPhaseStartTime = time.Now()
			}
			kv, err := originalMapper(ctx, input)
			return kv, err
		}

		// Wrap reducer to track reduce phase and key group sizes
		wrappedReducer := func(ctx context.Context, input ReduceInput[K, V]) (R, error) {
			if reducePhaseStartTime.IsZero() {
				reducePhaseStartTime = time.Now()
			}

			if collector != nil {
				keyStr := fmt.Sprintf("%v", input.Key)
				collector.MapReduceKeyGroupSize(ctx, metricName, keyStr, len(input.Values))
			}

			return originalReducer(ctx, input)
		}

		// Create a new MapReduce with wrapped functions
		wrappedMapReduce := &MapReduce[I, K, V, R]{
			mapper:      wrappedMapper,
			reducer:     wrappedReducer,
			parallelism: mapReduceStage.parallelism,
		}

		// Track phases by intercepting the process
		results, err := wrappedMapReduce.Process(ctx, inputs)
		duration := time.Since(startTime)

		// Report phase completions (approximate timing)
		//nolint:nestif // Complex timing calculations justified for comprehensive metrics
		if collector != nil {
			if !mapPhaseStartTime.IsZero() {
				// Estimate map phase completion (before reduce starts or at 1/3 of total duration)
				mapDuration := duration / 3
				if !reducePhaseStartTime.IsZero() {
					mapDuration = reducePhaseStartTime.Sub(mapPhaseStartTime)
				}
				collector.MapReduceMapPhaseCompleted(ctx, metricName, numItems, mapPhaseKeys, mapDuration)

				// Estimate shuffle phase (middle 1/3 of duration)
				shuffleDuration := duration / 3
				collector.MapReduceShufflePhaseCompleted(ctx, metricName, mapPhaseKeys, shuffleDuration)

				// Reduce phase (final 1/3 or actual time)
				reduceDuration := duration / 3
				if !reducePhaseStartTime.IsZero() {
					reduceDuration = time.Since(reducePhaseStartTime)
				}
				numResults := len(results)
				collector.MapReduceReducePhaseCompleted(ctx, metricName, mapPhaseKeys, numResults, reduceDuration)
			}
		}

		if collector != nil {
			if err != nil {
				collector.StageError(ctx, metricName, err)
			} else {
				collector.StageCompleted(ctx, metricName, duration)
			}
		}
		return results, err
	})
}

// NewMetricatedTumblingCountWindow creates a metricated wrapper for TumblingCountWindow.
func NewMetricatedTumblingCountWindow[T any](
	windowStage *TumblingCountWindow[T],
	options ...MetricatedStreamStageOption[T, []T],
) StreamStage[T, []T] {
	if windowStage == nil {
		panic("fluxus.NewMetricatedTumblingCountWindow: windowStage cannot be nil")
	}
	// Determine the collector that will be used by MetricatedStreamStage
	tempCfg := &MetricatedStreamStage[T, []T]{metricsCollector: DefaultMetricsCollector}

	for _, opt := range options { // Apply user-provided options first to see if collector is overridden
		opt(tempCfg)
	}

	finalCollector := tempCfg.metricsCollector

	emitCallback := func(ctx context.Context, stageName string, emittedWindow []T) {
		if finalCollector != nil {
			finalCollector.WindowEmitted(ctx, stageName, len(emittedWindow))
		}
	}

	//nolint:gocritic // this is for clarity, not a linting issue
	allOptions := append(
		options, // Pass original options through
		WithMetricsStreamStageName[T, []T](
			"metricated_tumbling_count_window",
		), // Default name, can be overridden by user options
		WithStreamEmitCallback[T](emitCallback),
	)

	return NewMetricatedStreamStage(windowStage, allOptions...)
}

// NewMetricatedTumblingTimeWindow creates a metricated wrapper for TumblingTimeWindow.
func NewMetricatedTumblingTimeWindow[T any](
	windowStage *TumblingTimeWindow[T],
	options ...MetricatedStreamStageOption[T, []T],
) StreamStage[T, []T] {
	// Similar logic to NewMetricatedTumblingCountWindow for emitCallback
	tempCfg := &MetricatedStreamStage[T, []T]{metricsCollector: DefaultMetricsCollector}

	for _, opt := range options {
		opt(tempCfg)
	}

	finalCollector := tempCfg.metricsCollector

	emitCallback := func(ctx context.Context, stageName string, emittedWindow []T) {
		if finalCollector != nil {
			finalCollector.WindowEmitted(ctx, stageName, len(emittedWindow))
		}
	}

	//nolint:gocritic // this is for clarity, not a linting issue
	allOptions := append(
		options,
		WithMetricsStreamStageName[T, []T]("metricated_tumbling_time_window"),
		WithStreamEmitCallback[T](emitCallback),
	)

	return NewMetricatedStreamStage(windowStage, allOptions...)
}

// NewMetricatedSlidingCountWindow creates a metricated wrapper for SlidingCountWindow.
func NewMetricatedSlidingCountWindow[T any](
	windowStage *SlidingCountWindow[T],
	options ...MetricatedStreamStageOption[T, []T],
) StreamStage[T, []T] {
	tempCfg := &MetricatedStreamStage[T, []T]{metricsCollector: DefaultMetricsCollector}

	for _, opt := range options {
		opt(tempCfg)
	}

	finalCollector := tempCfg.metricsCollector

	emitCallback := func(ctx context.Context, stageName string, emittedWindow []T) {
		if finalCollector != nil {
			finalCollector.WindowEmitted(ctx, stageName, len(emittedWindow))
		}
	}

	//nolint:gocritic // this is for clarity, not a linting issue
	allOptions := append(
		options,
		WithMetricsStreamStageName[T, []T]("metricated_sliding_count_window"),
		WithStreamEmitCallback[T](emitCallback),
	)

	return NewMetricatedStreamStage(windowStage, allOptions...)
}

// NewMetricatedSlidingTimeWindow creates a metricated wrapper for SlidingTimeWindow.
func NewMetricatedSlidingTimeWindow[T any](
	windowStage *SlidingTimeWindow[T],
	options ...MetricatedStreamStageOption[T, []T],
) StreamStage[T, []T] {
	tempCfg := &MetricatedStreamStage[T, []T]{metricsCollector: DefaultMetricsCollector}

	for _, opt := range options {
		opt(tempCfg)
	}

	finalCollector := tempCfg.metricsCollector

	emitCallback := func(ctx context.Context, stageName string, emittedWindow []T) {
		if finalCollector != nil {
			finalCollector.WindowEmitted(ctx, stageName, len(emittedWindow))
		}
	}

	//nolint:gocritic // this is for clarity, not a linting issue
	allOptions := append(
		options,
		WithMetricsStreamStageName[T, []T]("metricated_sliding_time_window"),
		WithStreamEmitCallback[T](emitCallback),
	)

	return NewMetricatedStreamStage(windowStage, allOptions...)
}

// NewMetricatedFilter creates a metricated wrapper around a Filter stage.
func NewMetricatedFilter[T any](
	filterStage *Filter[T],
	options ...MetricatedStageOption[T, T],
) Stage[T, T] {
	if filterStage == nil {
		panic("fluxus.NewMetricatedFilter: filterStage cannot be nil")
	}

	configHolder := &MetricatedStage[T, T]{
		name:             "metricated_filter",
		metricsCollector: DefaultMetricsCollector,
	}

	for _, option := range options {
		option(configHolder)
	}

	collector := configHolder.metricsCollector
	metricName := configHolder.name

	// Return a StageFunc that implements the filter metrics logic
	return StageFunc[T, T](func(ctx context.Context, input T) (T, error) {
		startTime := time.Now()
		if collector != nil {
			collector.StageStarted(ctx, metricName)
		}

		// We need to wrap the filter's predicate to capture filtering metrics
		originalPredicate := filterStage.predicate
		filterStage.predicate = func(ctx context.Context, input T) (bool, error) {
			keep, err := originalPredicate(ctx, input)
			if collector != nil {
				switch {
				case err != nil:
					collector.FilterPredicateError(ctx, metricName, err)
				case keep:
					collector.FilterItemPassed(ctx, metricName)
				default:
					collector.FilterItemDropped(ctx, metricName)
				}
			}
			return keep, err
		}

		output, err := filterStage.Process(ctx, input)
		duration := time.Since(startTime)

		// Restore original predicate
		filterStage.predicate = originalPredicate

		if collector != nil {
			if err != nil && !errors.Is(err, ErrItemFiltered) {
				// Only report as error if it's not the expected filtering error
				collector.StageError(ctx, metricName, err)
			} else {
				collector.StageCompleted(ctx, metricName, duration)
			}
		}
		return output, err
	})
}

// NewMetricatedRouter creates a metricated wrapper around a Router stage.
//
//nolint:gocognit // Complex wrapper function justified for comprehensive metrics
func NewMetricatedRouter[I, O any](
	routerStage *Router[I, O],
	options ...MetricatedStageOption[I, []O],
) Stage[I, []O] {
	if routerStage == nil {
		panic("fluxus.NewMetricatedRouter: routerStage cannot be nil")
	}

	configHolder := &MetricatedStage[I, []O]{
		name:             "metricated_router",
		metricsCollector: DefaultMetricsCollector,
	}

	for _, option := range options {
		option(configHolder)
	}

	collector := configHolder.metricsCollector
	metricName := configHolder.name

	// Return a StageFunc that implements the router metrics logic
	return StageFunc[I, []O](func(ctx context.Context, input I) ([]O, error) {
		startTime := time.Now()
		if collector != nil {
			collector.StageStarted(ctx, metricName)
		}

		// We need to wrap the router's selectorFunc to capture routing metrics
		originalSelectorFunc := routerStage.selectorFunc
		routerStage.selectorFunc = func(ctx context.Context, input I) ([]int, error) {
			indices, err := originalSelectorFunc(ctx, input)
			if collector != nil {
				if err == nil {
					numSelected := len(indices)
					totalRoutes := len(routerStage.routes)

					if numSelected == 0 {
						collector.RouterNoRouteMatched(ctx, metricName)
					} else {
						collector.RouterRoutesSelected(ctx, metricName, numSelected, totalRoutes)
					}
				}
			}
			return indices, err
		}

		// We also need to wrap individual route stages to track per-route metrics
		originalRoutes := make([]Route[I, O], len(routerStage.routes))
		copy(originalRoutes, routerStage.routes)

		for i, route := range routerStage.routes {
			originalStage := route.Stage
			routeIndex := i
			routeName := route.Name
			if routeName == "" {
				routeName = fmt.Sprintf("route_%d", i)
			}

			wrappedStage := StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
				routeStartTime := time.Now()
				output, err := originalStage.Process(ctx, input)
				routeDuration := time.Since(routeStartTime)

				if collector != nil && err == nil {
					collector.RouterRouteProcessed(ctx, metricName, routeName, routeIndex, routeDuration)
				}
				return output, err
			})

			routerStage.routes[i] = Route[I, O]{
				Name:  route.Name,
				Stage: wrappedStage,
			}
		}

		output, err := routerStage.Process(ctx, input)
		duration := time.Since(startTime)

		// Restore original functions and routes
		routerStage.selectorFunc = originalSelectorFunc
		copy(routerStage.routes, originalRoutes)

		if collector != nil {
			if err != nil {
				collector.StageError(ctx, metricName, err)
			} else {
				collector.StageCompleted(ctx, metricName, duration)
			}
		}
		return output, err
	})
}

// NewMetricatedJoinByKey creates a metricated wrapper around a JoinByKey stage.
// Note: The signature assumes JoinByKey processes a slice of inputs, similar to MapReduce.
func NewMetricatedJoinByKey[I any, K comparable](
	joinStage *JoinByKey[I, K],
	options ...MetricatedStageOption[[]I, map[K][]I],
) Stage[[]I, map[K][]I] {
	if joinStage == nil {
		panic("fluxus.NewMetricatedJoinByKey: joinStage cannot be nil")
	}

	configHolder := &MetricatedStage[[]I, map[K][]I]{
		name:             "metricated_join_by_key",
		metricsCollector: DefaultMetricsCollector,
	}

	for _, option := range options {
		option(configHolder)
	}

	collector := configHolder.metricsCollector
	metricName := configHolder.name

	// Return a StageFunc that implements the JoinByKey metrics logic
	return StageFunc[[]I, map[K][]I](func(ctx context.Context, inputs []I) (map[K][]I, error) {
		startTime := time.Now()
		if collector != nil {
			collector.StageStarted(ctx, metricName)
		}

		// We need to wrap the join's keyFunc to capture grouping metrics
		originalKeyFunc := joinStage.keyFunc
		keyGroups := make(map[K]int) // Track group sizes as we build them

		joinStage.keyFunc = func(ctx context.Context, input I) (K, error) {
			key, err := originalKeyFunc(ctx, input)
			if err == nil {
				// Track the group size for this key
				keyGroups[key]++
			}
			return key, err
		}

		results, err := joinStage.Process(ctx, inputs)
		duration := time.Since(startTime)

		// Restore original keyFunc
		joinStage.keyFunc = originalKeyFunc

		if collector != nil {
			if err == nil {
				// Report group creation metrics
				for key, groupSize := range keyGroups {
					keyStr := fmt.Sprintf("%v", key)
					collector.JoinByKeyGroupCreated(ctx, metricName, keyStr, groupSize)
				}

				// Report overall join completion
				numKeys := len(results)
				totalItems := len(inputs)
				collector.JoinByKeyCompleted(ctx, metricName, numKeys, totalItems, duration)
				collector.StageCompleted(ctx, metricName, duration)
			} else {
				collector.StageError(ctx, metricName, err)
			}
		}
		return results, err
	})
}

// stateToString converts CircuitBreakerState to string for metrics reporting.
func stateToString(state CircuitBreakerState) string {
	switch state {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// NewMetricatedCircuitBreaker creates a metricated wrapper around a CircuitBreaker stage.
func NewMetricatedCircuitBreaker[I, O any](
	cbStage *CircuitBreaker[I, O],
	options ...MetricatedStageOption[I, O],
) Stage[I, O] {
	if cbStage == nil {
		panic("fluxus.NewMetricatedCircuitBreaker: cbStage cannot be nil")
	}

	configHolder := &MetricatedStage[I, O]{
		name:             "metricated_circuit_breaker",
		metricsCollector: DefaultMetricsCollector,
	}

	for _, option := range options {
		option(configHolder)
	}

	collector := configHolder.metricsCollector
	metricName := configHolder.name

	// Return a StageFunc that implements the circuit breaker metrics logic
	return StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
		startTime := time.Now()
		if collector != nil {
			collector.StageStarted(ctx, metricName)
		}

		// Check current circuit state before processing
		var prevState CircuitBreakerState
		cbStage.mu.RLock()
		prevState = cbStage.state
		failures := cbStage.failures
		successes := cbStage.consecutiveSuccesses
		cbStage.mu.RUnlock()

		output, err := cbStage.Process(ctx, input)
		duration := time.Since(startTime)

		//nolint:nestif // Complex metrics logic justified for circuit breaker state tracking
		if collector != nil {
			// Check if circuit was rejected
			if errors.Is(err, ErrCircuitOpen) {
				collector.CircuitBreakerRejected(ctx, metricName)
			}

			// Check state after processing
			cbStage.mu.RLock()
			currentState := cbStage.state
			newFailures := cbStage.failures
			newSuccesses := cbStage.consecutiveSuccesses
			cbStage.mu.RUnlock()

			// Report state change if it occurred
			if prevState != currentState {
				collector.CircuitStateChanged(ctx, metricName, stateToString(prevState), stateToString(currentState))
			}

			// Report failure/success recording
			if err != nil && !errors.Is(err, ErrCircuitOpen) {
				if newFailures > failures {
					collector.CircuitBreakerFailureRecorded(ctx, metricName, newFailures, cbStage.failureThreshold)
				}
				collector.StageError(ctx, metricName, err)
			} else if err == nil {
				if currentState == CircuitHalfOpen && newSuccesses > successes {
					collector.CircuitBreakerSuccessRecorded(ctx, metricName, newSuccesses, cbStage.successThreshold)
				}
				collector.StageCompleted(ctx, metricName, duration)
			}
		}
		return output, err
	})
}

// NewMetricatedTimeout creates a metricated wrapper around a Timeout stage.
func NewMetricatedTimeout[I, O any](
	timeoutStage *Timeout[I, O],
	options ...MetricatedStageOption[I, O],
) Stage[I, O] {
	if timeoutStage == nil {
		panic("fluxus.NewMetricatedTimeout: timeoutStage cannot be nil")
	}

	configHolder := &MetricatedStage[I, O]{
		name:             "metricated_timeout",
		metricsCollector: DefaultMetricsCollector,
	}

	for _, option := range options {
		option(configHolder)
	}

	collector := configHolder.metricsCollector
	metricName := configHolder.name

	// Return a StageFunc that implements the timeout metrics logic
	return StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
		startTime := time.Now()
		if collector != nil {
			collector.StageStarted(ctx, metricName)
		}

		output, err := timeoutStage.Process(ctx, input)
		duration := time.Since(startTime)

		if collector != nil {
			if err != nil {
				// Check if it was a timeout error
				if errors.Is(err, context.DeadlineExceeded) {
					collector.TimeoutOccurred(ctx, metricName, timeoutStage.timeout, duration)
				}
				collector.StageError(ctx, metricName, err)
			} else {
				collector.StageCompleted(ctx, metricName, duration)
			}
		}
		return output, err
	})
}

// NewMetricatedDeadLetterQueue creates a metricated wrapper around a DeadLetterQueue stage.
func NewMetricatedDeadLetterQueue[I, O any](
	dlqStage *DeadLetterQueue[I, O],
	options ...MetricatedStageOption[I, O],
) Stage[I, O] {
	if dlqStage == nil {
		panic("fluxus.NewMetricatedDeadLetterQueue: dlqStage cannot be nil")
	}

	configHolder := &MetricatedStage[I, O]{
		name:             "metricated_dead_letter_queue",
		metricsCollector: DefaultMetricsCollector,
	}

	for _, option := range options {
		option(configHolder)
	}

	collector := configHolder.metricsCollector
	metricName := configHolder.name

	// Return a StageFunc that implements the DLQ metrics logic
	return StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
		startTime := time.Now()
		if collector != nil {
			collector.StageStarted(ctx, metricName)
		}

		// Create a wrapped handler for metrics (thread-safe approach)
		wrappedHandler := DLQHandlerFunc[I](func(ctx context.Context, item I, processingError error) error {
			if collector != nil {
				collector.DeadLetterQueueItemSent(ctx, metricName, processingError)
			}
			dlqErr := dlqStage.dlqHandler.Handle(ctx, item, processingError)
			if dlqErr != nil && collector != nil {
				collector.DeadLetterQueueHandlerError(ctx, metricName, dlqErr)
			}
			return dlqErr
		})

		// Create a new DLQ stage with the wrapped handler (thread-safe)
		metricatedDLQStage := &DeadLetterQueue[I, O]{
			stage:       dlqStage.stage,
			shouldDLQ:   dlqStage.shouldDLQ,
			logDLQError: dlqStage.logDLQError,
			dlqHandler:  wrappedHandler,
		}

		output, err := metricatedDLQStage.Process(ctx, input)
		duration := time.Since(startTime)

		if collector != nil {
			if err != nil {
				collector.StageError(ctx, metricName, err)
			} else {
				collector.StageCompleted(ctx, metricName, duration)
			}
		}
		return output, err
	})
}

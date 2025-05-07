package fluxus

import (
	"context"
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
func WithStreamEmitCallback[T, O any](onEmit func(ctx context.Context, stageName string, emittedItem O)) MetricatedStreamStageOption[T, O] {
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
	// Map stage doesn't have specific metrics in MetricsCollector beyond generic StageStarted/Completed/Error.
	// So, we directly wrap it with NewMetricatedStage.
	// Options provided to NewMetricatedMap will be passed to NewMetricatedStage.
	// A default name can be set if not overridden by options.
	defaultOptions := []MetricatedStageOption[[]I, []O]{
		WithMetricsStageName[[]I, []O]("metricated_map"),
	}
	allOptions := append(defaultOptions, options...)
	return NewMetricatedStage(mapStage, allOptions...)
}

// NewMetricatedMapReduce creates a metricated wrapper around a MapReduce stage.
func NewMetricatedMapReduce[I any, K comparable, V any, R any](
	mapReduceStage *MapReduce[I, K, V, R],
	options ...MetricatedStageOption[[]I, []R], // MapReduce's Process takes []I and returns []R
) Stage[[]I, []R] {
	if mapReduceStage == nil {
		panic("fluxus.NewMetricatedMapReduce: mapReduceStage cannot be nil")
	}
	// Similar to Map, MapReduce doesn't have specific metrics in MetricsCollector.
	defaultOptions := []MetricatedStageOption[[]I, []R]{
		WithMetricsStageName[[]I, []R]("metricated_map_reduce"),
	}
	allOptions := append(defaultOptions, options...)
	return NewMetricatedStage(mapReduceStage, allOptions...)
}

// NewMetricatedTumblingCountWindow creates a metricated wrapper for TumblingCountWindow.
func NewMetricatedTumblingCountWindow[T any](windowStage *TumblingCountWindow[T], options ...MetricatedStreamStageOption[T, []T]) StreamStage[T, []T] {
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

	allOptions := append(
		options, // Pass original options through
		WithMetricsStreamStageName[T, []T]("metricated_tumbling_count_window"), // Default name, can be overridden by user options
		WithStreamEmitCallback[T](emitCallback),
	)

	return NewMetricatedStreamStage(windowStage, allOptions...)
}

// NewMetricatedTumblingTimeWindow creates a metricated wrapper for TumblingTimeWindow.
func NewMetricatedTumblingTimeWindow[T any](windowStage *TumblingTimeWindow[T], options ...MetricatedStreamStageOption[T, []T]) StreamStage[T, []T] {
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

	allOptions := append(options, WithMetricsStreamStageName[T, []T]("metricated_tumbling_time_window"), WithStreamEmitCallback[T](emitCallback))

	return NewMetricatedStreamStage(windowStage, allOptions...)
}

// NewMetricatedSlidingCountWindow creates a metricated wrapper for SlidingCountWindow.
func NewMetricatedSlidingCountWindow[T any](windowStage *SlidingCountWindow[T], options ...MetricatedStreamStageOption[T, []T]) StreamStage[T, []T] {
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

	allOptions := append(options, WithMetricsStreamStageName[T, []T]("metricated_sliding_count_window"), WithStreamEmitCallback[T](emitCallback))

	return NewMetricatedStreamStage(windowStage, allOptions...)
}

// NewMetricatedSlidingTimeWindow creates a metricated wrapper for SlidingTimeWindow.
func NewMetricatedSlidingTimeWindow[T any](windowStage *SlidingTimeWindow[T], options ...MetricatedStreamStageOption[T, []T]) StreamStage[T, []T] {
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

	allOptions := append(options, WithMetricsStreamStageName[T, []T]("metricated_sliding_time_window"), WithStreamEmitCallback[T](emitCallback))

	return NewMetricatedStreamStage(windowStage, allOptions...)
}

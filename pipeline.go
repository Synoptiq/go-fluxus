package fluxus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

// Pipeline represents a simple, single-stage pipeline for processing individual items.
// It wraps a single Stage and provides basic lifecycle management (Start/Stop)
// and error handling. This is suitable for scenarios where you process items
// one at a time on demand, rather than processing a continuous stream.
// For stream processing, use StreamPipeline and its builder.
type Pipeline[I, O any] struct {
	stage      Stage[I, O]
	errHandler func(error) error
	// --- New Lifecycle Fields ---
	startMu sync.Mutex
	started bool
	// --- End New Lifecycle Fields ---
}

// NewPipeline creates a new single-stage Pipeline.
// It requires the Stage[I, O] that will perform the processing.
func NewPipeline[I, O any](stage Stage[I, O]) *Pipeline[I, O] {
	return &Pipeline[I, O]{
		stage:      stage,
		errHandler: func(err error) error { return err },
		started:    false, // Initialize started state
	}
}

// WithErrorHandler adds an optional custom error handler to the pipeline.
// This handler function receives any error returned by the underlying stage's
// Process method. It can be used to wrap, log, modify, or suppress the error
// before it's returned by the pipeline's Process method.
// If nil is provided, a default handler that returns the original error is used.
func (p *Pipeline[I, O]) WithErrorHandler(handler func(error) error) *Pipeline[I, O] {
	if handler != nil {
		p.errHandler = handler
	} else {
		p.errHandler = func(err error) error { return err } // Default to no-op handler
	}
	return p
}

// Process executes the pipeline's single stage on the given input item.
// It first checks if the pipeline has been started via the Start method.
// If started, it calls the underlying stage's Process method.
// Any error returned by the stage is passed through the configured error handler.
// Returns ErrPipelineNotStarted if called before Start().
// Returns context.Canceled or context.DeadlineExceeded if the context is done
func (p *Pipeline[I, O]) Process(ctx context.Context, input I) (O, error) {
	p.startMu.Lock() // Quick check on started state
	started := p.started
	p.startMu.Unlock()

	if !started {
		var zero O
		// Decide behavior: error or allow processing on non-started? Error is safer.
		return zero, ErrPipelineNotStarted // Use predefined
	}

	var zero O

	// Check for context cancellation
	if ctx.Err() != nil {
		return zero, p.errHandler(ctx.Err())
	}

	// Process through the stage
	result, err := p.stage.Process(ctx, input)
	if err != nil {
		return zero, p.errHandler(err)
	}

	return result, nil
}

// Start initializes the pipeline by calling the Start method of the underlying
// stage, but only if the stage implements the optional Starter interface.
// It marks the pipeline as started, allowing Process calls.
// Returns ErrPipelineAlreadyStarted if called on an already started pipeline.
// Returns an error if the stage's Start method fails.
// It is safe to call Start multiple times; subsequent calls after the first
// successful one will return ErrPipelineAlreadyStarted.
func (p *Pipeline[I, O]) Start(ctx context.Context) error {
	p.startMu.Lock()
	defer p.startMu.Unlock()

	if p.started {
		return ErrPipelineAlreadyStarted // Use predefined
	}

	// Call Initializer.Setup if implemented
	if initializer, ok := p.stage.(Initializer); ok {
		if err := initializer.Setup(ctx); err != nil {
			return NewPipelineLifecycleError("Start", "stage setup failed", err)
		}
	}

	p.started = true
	return nil
}

// Stop cleans up the pipeline by calling the Stop method of the underlying
// stage, but only if the stage implements the optional Stopper interface.
// It marks the pipeline as stopped, preventing further Process calls (they
// will return ErrPipelineNotStarted).
// Returns an error if the stage's Stop method fails.
// It is safe to call Stop multiple times; subsequent calls on a stopped
// pipeline will do nothing and return nil.
func (p *Pipeline[I, O]) Stop(ctx context.Context) error {
	p.startMu.Lock()
	defer p.startMu.Unlock()

	if !p.started {
		return nil // Not an error to stop an already stopped pipeline
	}

	var collectedErrs []error

	// Call Closer.Close if implemented
	if closer, ok := p.stage.(Closer); ok {
		if err := closer.Close(ctx); err != nil {
			collectedErrs = append(collectedErrs, fmt.Errorf("stage close failed: %w", err))
		}
	}

	p.started = false // Mark as stopped even if stopper errors

	if len(collectedErrs) > 0 {
		return NewPipelineLifecycleError("Stop", "stage close failed", NewMultiError(collectedErrs))
	}
	return nil
}

// Reset attempts to reset the pipeline and its underlying stage to an initial state.
// If the underlying stage implements the Resettable interface, its Reset method is called.
// The pipeline's own 'started' state is also reset, allowing it to be started again.
// It is recommended to only call Reset on a pipeline that is not currently started.
// If the pipeline is started, an error will be returned.
func (p *Pipeline[I, O]) Reset(ctx context.Context) error {
	p.startMu.Lock()
	defer p.startMu.Unlock()

	if p.started {
		return NewPipelineLifecycleError("Reset", "cannot reset an already started pipeline", nil)
	}

	if resettable, ok := p.stage.(Resettable); ok {
		if err := resettable.Reset(ctx); err != nil {
			return NewPipelineLifecycleError("Reset", "failed to reset underlying stage", err)
		}
	}

	// p.started is already false if we passed the check above.
	// If we allowed resetting a started pipeline after stopping it, we'd set it here.
	// For now, strict check: must not be started.
	return nil
}

// HealthStatus returns the health status of the pipeline.
// If the underlying stage implements the HealthCheckable interface, its HealthStatus is returned.
// Otherwise, nil is returned, indicating the pipeline structure itself is sound.
func (p *Pipeline[I, O]) HealthStatus(ctx context.Context) error {
	if healthCheckable, ok := p.stage.(HealthCheckable); ok {
		return healthCheckable.HealthStatus(ctx)
	}
	return nil // Default to healthy if stage is not HealthCheckable
}

// --- Stream Pipeline Configuration ---

// streamPipelineConfig holds the configuration options applied to a StreamPipeline
// via the builder pattern using StreamPipelineOption functions.
type streamPipelineConfig struct {
	bufferSize       int
	logger           *log.Logger
	concurrency      int              // Number of concurrent goroutines for each stage
	metricsCollector MetricsCollector // Optional metrics collector for stages
	pipelineName     string           // Optional name for the pipeline
	tracerProvider   TracerProvider
	tracer           trace.Tracer
}

// StreamPipelineOption defines a function type used to modify the streamPipelineConfig.
// These are passed to NewStreamPipeline or specific builder methods.
type StreamPipelineOption func(*streamPipelineConfig)

// WithStreamTracerProvider provides an option to set the OpenTelemetry TracerProvider
// for the entire StreamPipeline. This provider will be used to create traces for the
// overall pipeline run (Pipeline.Run span) and will be passed down to StreamAdapters
// (unless overridden by WithAdapterTracerProvider) to trace individual item processing.
// If nil is provided, the DefaultTracerProvider (otel.GetGlobalTracerProvider) is used.
// Default: DefaultTracerProvider.
func WithStreamTracerProvider(provider TracerProvider) StreamPipelineOption {
	return func(cfg *streamPipelineConfig) {
		if provider != nil {
			cfg.tracerProvider = provider
		} else {
			cfg.tracerProvider = DefaultTracerProvider // Ensure it's never nil
		}
	}
}

// WithStreamMetricsCollector provides an option to set the MetricsCollector instance
// for the StreamPipeline. This collector will receive pipeline-level metrics
// (PipelineStarted, PipelineCompleted) emitted by the Run/Start/Wait/Stop methods.
// It will also be passed down as the default collector for StreamAdapters created
// via AddStage (unless overridden by WithAdapterMetrics or disabled by WithoutAdapterMetrics).
// If nil is provided, the DefaultMetricsCollector (a no-op collector) is used.
// Default: nil (results in DefaultMetricsCollector).
func WithStreamMetricsCollector(collector MetricsCollector) StreamPipelineOption {
	return func(cfg *streamPipelineConfig) {
		cfg.metricsCollector = collector // Allow setting nil
	}
}

// WithStreamPipelineName provides an option to set a descriptive name for the StreamPipeline.
// This name is used as a dimension/attribute in pipeline-level metrics and traces,
// helping to identify the specific pipeline instance.
// Default: "fluxus_stream_pipeline".
func WithStreamPipelineName(name string) StreamPipelineOption {
	return func(cfg *streamPipelineConfig) {
		if name != "" {
			cfg.pipelineName = name
		}
	}
}

// WithStreamBufferSize provides an option to set the default buffer size for the channels
// created internally to connect the stages within the StreamPipeline.
//   - n > 0: Creates buffered channels of size 'n'.
//   - n <= 0: Creates unbuffered channels.
//
// Buffering can improve throughput by decoupling stages but increases memory usage
// and latency for individual items. Unbuffered channels enforce tighter synchronization.
// Default: 0 (unbuffered).
func WithStreamBufferSize(n int) StreamPipelineOption {
	return func(cfg *streamPipelineConfig) {
		if n < 0 {
			cfg.bufferSize = 0 // Treat negative as unbuffered
		} else {
			cfg.bufferSize = n
		}
	}
}

// WithStreamLogger provides an option to set a custom *log.Logger for the StreamPipeline.
// This logger is used for messages related to the pipeline's lifecycle (Start, Stop, Wait, Run)
// and stage execution progress/errors. It is also passed down as the default logger
// to StreamAdapters created via AddStage (unless overridden by WithAdapterLogger).
// If nil is provided, logging defaults to a logger that discards all output (io.Discard).
// Default: io.Discard logger.
func WithStreamLogger(logger *log.Logger) StreamPipelineOption {
	return func(cfg *streamPipelineConfig) {
		if logger != nil {
			cfg.logger = logger // logger can be nil, allowing for no logging
		}
	}
}

// WithStreamConcurrency provides an option to set the default concurrency level for
// StreamAdapters created automatically when using the AddStage builder method.
// This value will be used unless explicitly overridden for a specific stage using
// the WithAdapterConcurrency option when calling AddStage.
// See WithAdapterConcurrency for details on how the concurrency value is interpreted.
// Concurrency must be at least 1.
// Default: 1 (sequential).
func WithStreamConcurrency(n int) StreamPipelineOption {
	return func(cfg *streamPipelineConfig) {
		if n < 1 {
			cfg.concurrency = 1 // Default to 1 if invalid value is provided
		} else {
			cfg.concurrency = n
		}
	}
}

// --- Internal Stage Representation ---

// runnableStage is an internal struct holding the necessary information to execute
// a single stage within the StreamPipeline's Run loop. It abstracts away whether
// the original stage was a Stage (wrapped in an adapter) or a StreamStage.
type runnableStage struct {
	// run is a function closure that encapsulates the logic to execute the stage.
	// It handles the necessary type assertions for the input and output channels
	// and calls either the StreamAdapter's or the StreamStage's ProcessStream method
	run func(ctx context.Context, inChan, outChan interface{}) error
	// Input type of the stage, used for channel creation.
	inType reflect.Type
	// Output type of the stage, used for channel creation.
	outType reflect.Type
	// Name or description for logging/debugging (optional).
	name string
	// originalStage holds a reference to the user-provided stage instance (Stage or StreamStage).
	// This is primarily used for checking if the stage implements Starter or Stopper interfaces.
	originalStage interface{}
}

// --- Internal Lifecycle Helper Structs ---

// namedInitializer associates an Initializer with its stage name.
type namedInitializer struct {
	Initializer
	name string
}

// namedCloser associates a Closer with its stage name.
type namedCloser struct {
	Closer
	name string
}

// --- Stream Pipeline Builder ---

// StreamPipelineBuilder facilitates the type-safe construction of a multi-stage StreamPipeline.
// It uses a fluent API (AddStage, AddStreamStage) and generics to ensure that the output type
// of one stage matches the input type of the next stage at compile time.
// The generic parameter `LastOutput` tracks the expected input type for the *next* stage to be added.
type StreamPipelineBuilder[LastOutput any] struct {
	stages []runnableStage       // Sequentially ordered list of stages to be run.
	cfg    *streamPipelineConfig // Configuration applied to the pipeline and default for adapters.
	// --- New Lifecycle Interface Collections ---
	initializers []namedInitializer
	closers      []namedCloser
}

// NewStreamPipeline creates a new StreamPipelineBuilder to start building a pipeline.
// The generic type parameter `I` specifies the input type required for the *first* stage
// that will be added to the pipeline.
// Optional StreamPipelineOption functions can be provided to configure the pipeline's
// behavior (e.g., logging, metrics, default buffer size, default concurrency).
func NewStreamPipeline[I any](options ...StreamPipelineOption) *StreamPipelineBuilder[I] {
	cfg := &streamPipelineConfig{
		bufferSize:       0,                          // Default unbuffered
		logger:           log.New(io.Discard, "", 0), // Default discard logger
		concurrency:      1,                          // Default concurrency
		metricsCollector: nil,                        // Default nil collector
		pipelineName:     "fluxus_stream_pipeline",   // Default name
		tracer:           nil,                        // Default nil tracer
		tracerProvider:   DefaultTracerProvider,
	}

	for _, option := range options {
		option(cfg)
	}

	// Although no stages are added yet, the builder is typed to expect
	// a stage consuming type 'I' next.
	return &StreamPipelineBuilder[I]{
		stages:       make([]runnableStage, 0),
		cfg:          cfg,
		initializers: make([]namedInitializer, 0),
		closers:      make([]namedCloser, 0),
	}
}

// AddStage adds a standard Stage[CurrentOutput, NextOutput] to the pipeline definition.
// The `CurrentOutput` type parameter *must* match the `LastOutput` type parameter of the
// builder it's called on, ensuring type safety between stages.
//
// The provided Stage is automatically wrapped in a StreamAdapter before being added.
// This adapter uses the pipeline's default configuration (logger, concurrency, metrics, tracer)
// unless specific StreamAdapterOption functions are provided via the `adapterOptions` parameter
// to override these defaults for this specific stage.
//
// It returns a *new* builder instance whose `LastOutput` type parameter is updated to
// `NextOutput`, ready for the next stage in the chain.
// Panics if the provided stage is nil.
func AddStage[CurrentOutput, NextOutput any](
	builder *StreamPipelineBuilder[CurrentOutput],
	name string,
	stage Stage[CurrentOutput, NextOutput],
	adapterOptions ...StreamAdapterOption[CurrentOutput, NextOutput], // Optional adapter options
) *StreamPipelineBuilder[NextOutput] {
	if stage == nil {
		panic("fluxus.AddStage: stage cannot be nil")
	}

	if name == "" {
		// Provide a default name if empty, though user should provide one
		inTypeName := reflect.TypeOf((*CurrentOutput)(nil)).Elem().Name()
		outTypeName := reflect.TypeOf((*NextOutput)(nil)).Elem().Name()
		name = fmt.Sprintf("adapter_stage[%s->%s]", inTypeName, outTypeName)
	}

	inType := reflect.TypeOf((*CurrentOutput)(nil)).Elem()
	outType := reflect.TypeOf((*NextOutput)(nil)).Elem()

	// --- Interface Checks ---
	var stageAsInitializer Initializer
	var stageAsCloser Closer

	if s, ok := stage.(Initializer); ok {
		stageAsInitializer = s
	}
	if s, ok := stage.(Closer); ok {
		stageAsCloser = s
	}
	// --- End Interface Checks ---

	// Create the runner function closure. This captures the typed stage
	// and pipeline config, handling type assertions internally.
	runner := func(ctx context.Context, inChan, outChan interface{}) error {
		// Assert channels to their concrete types.
		typedInChan, okIn := castChannelToReadable[CurrentOutput](inChan)
		if !okIn {
			return fmt.Errorf(
				"internal error: stage '%s' received incompatible input channel type %T, expected readable %s",
				name,
				inChan,
				inType.Name(),
			)
		}

		// Use helper for output channel assertion
		typedOutChan, okOut := castChannelToWritable[NextOutput](outChan)
		if !okOut {
			return fmt.Errorf(
				"internal error: stage '%s' received incompatible output channel type %T, expected writable %s",
				name,
				outChan,
				outType.Name(),
			)
		}

		// Prepare the list of options to pass to NewStreamAdapter.
		defaultAdapterOptions := []StreamAdapterOption[CurrentOutput, NextOutput]{
			// Apply an adapter name as a default adapter name
			WithAdapterName[CurrentOutput, NextOutput](name),
			// Apply an adapter logger as a default adapter logger
			WithAdapterLogger[CurrentOutput, NextOutput](builder.cfg.logger),
			// Apply adapter concurrency as a default adapter concurrency
			WithAdapterConcurrency[CurrentOutput, NextOutput](builder.cfg.concurrency),
			// Apply metrics collector as a default adapter metrics collector
			WithAdapterMetrics[CurrentOutput, NextOutput](builder.cfg.metricsCollector),
			// Apply tracer as a default adapter tracer
			WithAdapterTracerProvider[CurrentOutput, NextOutput](builder.cfg.tracerProvider),
		}

		// Append the user-provided options. Options provided later might override earlier ones
		// depending on how NewStreamAdapter applies them.
		//nolint:gocritic // It's fine to append to a different slice than defaultAdapterOptions
		finalAdapterOptions := append(defaultAdapterOptions, adapterOptions...)

		// Wrap the Stage in a StreamAdapter.
		// Explicitly provide the generic types to NewStreamAdapter.
		// Pass the logger from the pipeline config using the adapter option function.
		adapter := NewStreamAdapter(stage, finalAdapterOptions...)

		// Execute the adapted stage
		return adapter.ProcessStream(ctx, typedInChan, typedOutChan)
	}

	newRunnable := runnableStage{
		run:           runner,
		inType:        inType,
		outType:       outType,
		name:          name,
		originalStage: stage, // <-- Store original stage
	}

	// --- Add to builder lists ---
	newInitializers := builder.initializers
	if stageAsInitializer != nil {
		newInitializers = append(newInitializers, namedInitializer{Initializer: stageAsInitializer, name: name})
	}
	newClosers := builder.closers
	if stageAsCloser != nil {
		newClosers = append(newClosers, namedCloser{Closer: stageAsCloser, name: name})
	}
	// --- End Add to builder lists ---

	// Return a new builder instance with the updated stage list and output type
	return &StreamPipelineBuilder[NextOutput]{
		stages:       append(builder.stages, newRunnable),
		cfg:          builder.cfg,
		initializers: newInitializers,
		closers:      newClosers,
	}
}

// AddStreamStage adds a custom StreamStage[CurrentOutput, NextOutput] directly to the pipeline.
// The `CurrentOutput` type parameter *must* match the `LastOutput` type parameter of the
// builder it's called on, ensuring type safety.
//
// Unlike AddStage, this method does *not* wrap the stage in a StreamAdapter. The provided
// StreamStage is used as-is. This means the stage implementation itself is responsible for:
//   - Handling concurrency (if desired).
//   - Implementing its own item-level error handling logic.
//   - Emitting its own metrics or creating its own traces if needed (it will not automatically
//     get the StageWorker* metrics or item-level traces provided by StreamAdapter).
//   - Closing its output channel correctly before returning from ProcessStream.
//
// This is useful for complex stages that need fine-grained control over stream processing logic.
//
// It returns a *new* builder instance whose `LastOutput` type parameter is updated to
// `NextOutput`, ready for the next stage in the chain.
// Panics if the provided stage is nil.
func AddStreamStage[CurrentOutput, NextOutput any](
	builder *StreamPipelineBuilder[CurrentOutput],
	name string,
	stage StreamStage[CurrentOutput, NextOutput],
) *StreamPipelineBuilder[NextOutput] {
	if stage == nil {
		panic("fluxus.AddStreamStage: stage cannot be nil")
	}

	if name == "" {
		// Provide a default name if empty
		inTypeName := reflect.TypeOf((*CurrentOutput)(nil)).Elem().Name()
		outTypeName := reflect.TypeOf((*NextOutput)(nil)).Elem().Name()
		name = fmt.Sprintf("stream_stage[%s->%s]", inTypeName, outTypeName)
	}

	inType := reflect.TypeOf((*CurrentOutput)(nil)).Elem()
	outType := reflect.TypeOf((*NextOutput)(nil)).Elem()

	// --- Interface Checks ---
	var stageAsInitializer Initializer
	var stageAsCloser Closer
	if s, ok := stage.(Initializer); ok {
		stageAsInitializer = s
	}
	if s, ok := stage.(Closer); ok {
		stageAsCloser = s
	}
	// --- End Interface Checks ---

	// Create the runner function closure.
	runner := func(ctx context.Context, inChan, outChan interface{}) error {
		// Assert channels to their concrete types.
		typedInChan, okIn := castChannelToReadable[CurrentOutput](inChan)
		if !okIn {
			return fmt.Errorf(
				"internal error: stage '%s' received incompatible input channel type %T, expected readable %s",
				name,
				inChan,
				inType.Name(),
			)
		}

		// Use helper for output channel assertion
		typedOutChan, okOut := castChannelToWritable[NextOutput](outChan)
		if !okOut {
			return fmt.Errorf(
				"internal error: stage '%s' received incompatible output channel type %T, expected writable %s",
				name,
				outChan,
				outType.Name(),
			)
		}

		// --- Metrics Note ---
		// Since this uses StreamStage directly, it bypasses StreamAdapter.
		// Therefore, the StageWorker* metrics will NOT be emitted automatically
		// for this stage. The implementation of the custom StreamStage would
		// need to incorporate its own metrics reporting if desired.
		// The pipeline-level metrics (Started/Completed) still apply.
		// --- End Metrics Note ---

		// Execute the StreamStage directly
		return stage.ProcessStream(ctx, typedInChan, typedOutChan)
	}

	newRunnable := runnableStage{
		run:           runner,
		inType:        inType,
		outType:       outType,
		name:          name,
		originalStage: stage, // <-- Store original stage
	}

	// --- Add to builder lists ---
	newInitializers := builder.initializers
	if stageAsInitializer != nil {
		newInitializers = append(newInitializers, namedInitializer{Initializer: stageAsInitializer, name: name})
	}

	newClosers := builder.closers
	if stageAsCloser != nil {
		newClosers = append(newClosers, namedCloser{Closer: stageAsCloser, name: name})
	}
	// --- End Add to builder lists ---

	// Return a new builder instance
	return &StreamPipelineBuilder[NextOutput]{
		stages:       append(builder.stages, newRunnable),
		cfg:          builder.cfg,
		initializers: newInitializers,
		closers:      newClosers,
	}
}

// --- Finalized Pipeline ---

// StreamPipeline represents a fully constructed, runnable stream processing pipeline.
// It is created by calling Finalize on a StreamPipelineBuilder.
//
// It manages the lifecycle of the pipeline stages, including:
//   - Starting stages that implement the Starter interface.
//   - Launching goroutines for each stage and connecting them with channels.
//   - Waiting for all stages to complete or for an error to occur.
//   - Stopping stages that implement the Stopper interface during shutdown.
//   - Handling context cancellation and propagating errors.
//   - Emitting pipeline-level metrics and traces.
//
// Use the Start, Wait, and Stop methods (or the convenience Run function) to control execution.
type StreamPipeline[I, O any] struct {
	stages []runnableStage
	cfg    *streamPipelineConfig
	// Store the overall input and output types for final validation in Run
	// firstInputType reflect.Type
	// lastOutputType reflect.Type

	// --- New Lifecycle Fields ---
	startMu  sync.Mutex         // Protects access to started flag and during start/stop transitions
	stopOnce sync.Once          // Ensures Stop logic runs only once
	stopCh   chan struct{}      // Closed to signal goroutines to stop
	runGroup *errgroup.Group    // The errgroup managing the running pipeline goroutines
	runCtx   context.Context    // The context for the running errgroup
	cancelFn context.CancelFunc // Function to cancel the runCtx
	started  atomic.Bool        // Tracks if the pipeline is currently running (atomic for potential read checks)

	// Store stages that need lifecycle management
	initializers []namedInitializer
	closers      []namedCloser
	// --- End New Lifecycle Fields ---

	pipelineSpan trace.Span // Span for the overall pipeline run
	startTime    time.Time  // Start time for metrics duration

}

// Finalize constructs the runnable StreamPipeline from the builder.
// It performs final validation checks. The `LastOutput` type parameter
// must match the output type of the very last stage added.
func Finalize[I, O any](builder *StreamPipelineBuilder[O]) (*StreamPipeline[I, O], error) {
	if len(builder.stages) == 0 {
		return nil, errors.New("fluxus.Finalize: cannot build an empty pipeline")
	}

	// --- Optional Sanity Check (Recommended) ---
	// Verify the builder's actual first stage input matches the expected 'I' type.
	// Generics should enforce this, but an explicit check catches potential misuse.
	firstStageActualInputType := builder.stages[0].inType
	expectedFirstInputType := reflect.TypeOf((*I)(nil)).Elem()
	if firstStageActualInputType != expectedFirstInputType {
		// This indicates a mismatch, likely a programming error in how Finalize was called
		// relative to how the builder was created (e.g., NewStreamPipeline[string] but Finalize[int, ...])
		return nil, NewPipelineConfigurationError(
			fmt.Sprintf(
				"fluxus.Finalize: builder's first stage input type (%s) does not match expected pipeline input type (%s)",
				firstStageActualInputType.Name(),
				expectedFirstInputType.Name(),
			),
		)
	}
	// --- End Optional Sanity Check ---

	return &StreamPipeline[I, O]{
		stages:       builder.stages,
		cfg:          builder.cfg,
		initializers: append([]namedInitializer(nil), builder.initializers...), // Copy slices
		closers:      append([]namedCloser(nil), builder.closers...),           // Copy slices
		stopCh:       make(chan struct{}),                                      // Initialize stopCh here or in Start
	}, nil
}

// --- Pipeline Execution ---

// initializeObservability sets up the pipeline-level trace span and records the start time
// for metrics. It should be called at the beginning of Start/Run.
// Returns the potentially updated context (with the trace span added), the span itself,
// the start time, and a boolean indicating if a real metrics collector is configured.
//
//nolint:nonamedreturns // Clear enough for internal helper
func (p *StreamPipeline[I, O]) initializeObservability(ctx context.Context) (
	pipelineCtx context.Context,
	pipelineSpan trace.Span,
	startTime time.Time,
	isRealCollector bool,
) {
	// --- Tracing Setup ---
	if p.cfg.tracerProvider == nil {
		p.cfg.tracerProvider = DefaultTracerProvider
	}
	pipelineName := p.cfg.pipelineName
	// Ensure tracer is initialized if not already (might be redundant if always done in config)
	if p.cfg.tracer == nil {
		p.cfg.tracer = p.cfg.tracerProvider.Tracer(fmt.Sprintf("fluxus/pipeline/%s", pipelineName))
	}
	tracer := p.cfg.tracer

	pipelineCtx = ctx // Start with the original context

	// Start overall pipeline span if tracer exists
	pipelineCtx, pipelineSpan = tracer.Start(pipelineCtx, fmt.Sprintf("%s.Run", pipelineName),
		trace.WithAttributes(
			attribute.String("fluxus.pipeline.name", pipelineName),
			attribute.Int("fluxus.pipeline.stages", len(p.stages)),
		),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	// Store span on pipeline struct immediately
	p.pipelineSpan = pipelineSpan

	// --- Metrics Setup ---
	startTime = time.Now()
	// Store start time on pipeline struct immediately
	p.startTime = startTime

	collector := p.cfg.metricsCollector
	isRealCollector = collector != nil && collector != DefaultMetricsCollector
	if isRealCollector {
		// It's generally safe to call PipelineStarted here.
		// If subsequent steps fail, PipelineCompleted will be called with an error.
		collector.PipelineStarted(pipelineCtx, pipelineName)
	}

	return pipelineCtx, pipelineSpan, startTime, isRealCollector
}

// initializeRunState sets up the internal context, errgroup, and cancellation function
// required for managing the pipeline's execution lifecycle. It derives the internal
// context (runCtx) from the potentially trace-enhanced pipeline context.
// Returns the errgroup's context (gctx) which should be passed to stage goroutines.
func (p *StreamPipeline[I, O]) initializeRunState(pipelineCtx context.Context) context.Context {
	p.stopCh = make(chan struct{})
	// Create the cancellable context for the errgroup, derived from the (potentially traced) pipelineCtx
	p.runCtx, p.cancelFn = context.WithCancel(pipelineCtx)
	g, gctx := errgroup.WithContext(p.runCtx)
	p.runGroup = g // Store the group
	return gctx    // Return the group's context for stages
}

// setupInitializers iterates through Initializer stages.
// sequentially. It uses the errgroup's context (gctx) so that setup respects
// pipeline cancellation. If any Initializer fails, it attempts to gracefully close any
// previously setup Initializers (using the original context for timeout) and returns an error,
// after finalizing observability fields to indicate the failed start.
func (p *StreamPipeline[I, O]) setupInitializers(
	gctx context.Context,
	pipelineSpan trace.Span,
	isRealCollector bool,
	startTime time.Time,
) error {
	p.cfg.logger.Printf("DEBUG: Setting up Initializer stages for pipeline '%s'...", p.cfg.pipelineName)
	for i, ni := range p.initializers { // Use p.initializers
		err := ni.Initializer.Setup(gctx) // Call Setup()
		if err != nil {
			// If an initializer fails, the Start operation fails.
			// Cleanup of other resources (via Closer) is typically handled by a subsequent Stop call on the pipeline.
			p.cfg.logger.Printf(
				"ERROR: Failed to setup initializer stage '%s' (index %d in initializers list): %v.",
				ni.name,
				i,
				err,
			)
			// Ensure cancelFn is available before calling
			if p.cancelFn != nil {
				p.cancelFn()
			}

			// Removed direct call to stop/close other stages here.
			// The pipeline's Stop method will handle closing all registered Closers.

			// Finalize observability for failed start
			if pipelineSpan != nil {
				pipelineSpan.RecordError(err)
				pipelineSpan.SetStatus(codes.Error, "stage setup failed")
				pipelineSpan.End()
			}
			if isRealCollector && p.cfg.metricsCollector != nil {
				p.cfg.metricsCollector.PipelineCompleted(
					context.Background(),
					p.cfg.pipelineName,
					time.Since(startTime),
					err, // The original error from Setup
				)
			}

			// Wrap error with stage info if possible (p.initializers might not directly map to p.stages if not all stages are initializers)
			wrappedErr := fmt.Errorf("failed to setup initializer stage '%s' (index %d): %w", ni.name, i, err)
			return NewPipelineLifecycleError("Start", "stage setup failed", wrappedErr)
		}
	}

	p.cfg.logger.Printf("DEBUG: Initializer stages set up successfully for pipeline '%s'.", p.cfg.pipelineName)
	return nil
}

// launchStageGoroutines creates the intermediate channels connecting the pipeline stages
// and launches a dedicated goroutine for each stage using the pipeline's errgroup.
// Each goroutine executes the stage's `run` function, passing it the appropriate
// input and output channels and the errgroup's context (gctx).
// It also wraps each stage's execution in a trace span if tracing is enabled.
func (p *StreamPipeline[I, O]) launchStageGoroutines(gctx context.Context, source, sink interface{}) {
	p.cfg.logger.Printf("DEBUG: Launching stage goroutines for pipeline '%s'...", p.cfg.pipelineName)
	var currentInChan = source
	tracer := p.cfg.tracer // Get tracer from config

	for i, stage := range p.stages {
		var currentOutChan interface{}
		isLastStage := (i == len(p.stages)-1)

		if isLastStage {
			currentOutChan = sink
		} else {
			chanType := reflect.ChanOf(reflect.BothDir, stage.outType)
			currentOutChan = reflect.MakeChan(chanType, p.cfg.bufferSize).Interface()
		}

		// Capture loop variables
		inChan := currentInChan
		outChan := currentOutChan
		currentStage := stage
		stageIndex := i

		p.runGroup.Go(func() error {
			// Stage execution with tracing
			stageCtx := gctx
			var stageSpan trace.Span
			if tracer != nil {
				stageStartTime := time.Now()
				stageCtx, stageSpan = tracer.Start(gctx,
					fmt.Sprintf("Stage[%d]:%s", stageIndex, currentStage.name),
					trace.WithAttributes(
						attribute.String("fluxus.pipeline.stage.name", currentStage.name),
						attribute.Int("fluxus.pipeline.stage.index", stageIndex),
					),
					trace.WithSpanKind(trace.SpanKindInternal),
				)
				defer func() {
					stageDuration := time.Since(stageStartTime)
					stageSpan.SetAttributes(
						attribute.Int64(
							"fluxus.pipeline.stage.duration_ms",
							stageDuration.Milliseconds(),
						),
					)
					stageSpan.End()
				}()
			}

			p.cfg.logger.Printf("DEBUG: Starting stage %d (%s)", stageIndex, currentStage.name)
			err := currentStage.run(stageCtx, inChan, outChan)
			if err != nil {
				p.cfg.logger.Printf("ERROR: Stage %d (%s) failed: %v", stageIndex, currentStage.name, err)
				if stageSpan != nil {
					stageSpan.RecordError(err)
					stageSpan.SetStatus(codes.Error, err.Error())
				}
				return NewStageError(currentStage.name, stageIndex, err) // Wrap error
			}

			p.cfg.logger.Printf("DEBUG: Stage %d (%s) finished successfully.", stageIndex, currentStage.name)
			if stageSpan != nil {
				stageSpan.SetStatus(codes.Ok, "")
			}
			return nil
		})

		currentInChan = currentOutChan // Prepare input for the next stage
	}
}

// Start initializes and begins the execution of the StreamPipeline.
// It performs the following steps:
//  1. Checks if the pipeline is already running or empty.
//  2. Validates the types of the provided source and sink channels.
//  3. Initializes observability (tracing span, start time, metrics).
//  4. Sets up the internal execution context and error group.
//  5. Calls the Start method on all stages implementing the Starter interface.
//  6. Launches goroutines for each pipeline stage, connecting them with channels.
//
// Start is non-blocking. After a successful call to Start, the pipeline runs in the
// background. Use the Wait() method to block until completion or error, and Stop()
// to initiate a graceful shutdown.
//
// The source must be a readable channel (e.g., <-chan I, chan I) whose element type
// matches the input type of the first stage.
// The sink must be a writable channel (e.g., chan<- O, chan O) whose element type
// matches the output type of the last stage.
//
// Returns ErrPipelineAlreadyStarted, ErrEmptyPipeline, PipelineConfigurationError,
// or an error from a Starter stage's Start method. Returns nil on success.
func (p *StreamPipeline[I, O]) Start(ctx context.Context, source <-chan I, sink chan<- O) error {
	p.startMu.Lock()
	defer p.startMu.Unlock()

	// 1. Initial Checks
	if p.started.Load() {
		return ErrPipelineAlreadyStarted
	}
	if len(p.stages) == 0 {
		return ErrEmptyPipeline
	}

	// 2. Observability Setup
	pipelineCtx, pipelineSpan, startTime, isRealCollector := p.initializeObservability(ctx)

	// 3. Initialize Lifecycle State
	gctx := p.initializeRunState(pipelineCtx) // Get the group's context

	// 4. Start Starter Stages (handles its own cleanup on failure)
	if err := p.setupInitializers(gctx, pipelineSpan, isRealCollector, startTime); err != nil {
		// Error already wrapped, observability handled within the helper on failure
		return err
	}

	// 5. Launch Stage Goroutines
	p.launchStageGoroutines(gctx, source, sink)

	// 6. Finalization
	p.started.Store(true)
	p.cfg.logger.Printf("INFO: Pipeline '%s' started successfully.", p.cfg.pipelineName)

	return nil
}

// Wait blocks until the pipeline, previously started with Start(), finishes execution.
// Completion occurs when:
//   - The source channel is closed, all data has flowed through the stages, and all
//     stage goroutines have exited cleanly.
//   - An error occurs in any stage goroutine (including cancellation errors).
//   - The pipeline's internal context (derived from the context passed to Start) is cancelled.
//
// It returns the first error encountered by any stage goroutine, or nil if the pipeline
// completed successfully.
//
// Wait also handles the finalization of pipeline-level metrics and tracing spans,
// recording the duration and success/error status. It ensures this finalization
// happens exactly once, coordinating with the Stop method via sync.Once.
// If Wait finishes naturally (no error, source closed), it will attempt to call Stop
// on any Stopper stages as part of cleanup.
//
// Returns ErrPipelineNotStarted if called before Start() or after Stop()/Wait() has completed.
//
//nolint:nonamedreturns // runErr is idiomatic here
func (p *StreamPipeline[I, O]) Wait() (runErr error) {
	p.startMu.Lock()
	if !p.started.Load() {
		p.startMu.Unlock()
		return ErrPipelineNotStarted
	}
	// Keep lock until runGroup is checked to prevent race with Stop
	runGroup := p.runGroup
	pipelineSpan := p.pipelineSpan // Get span stored during Start
	startTime := p.startTime       // Get start time stored during Start
	p.startMu.Unlock()

	if runGroup == nil {
		// Should not happen if started is true, but defensive check
		return NewPipelineLifecycleError("Wait", "internal error: runGroup is nil", nil)
	}

	p.cfg.logger.Printf("INFO: Pipeline '%s' waiting for completion...", p.cfg.pipelineName)
	runErr = runGroup.Wait() // Block here
	p.cfg.logger.Printf("INFO: Pipeline '%s' finished waiting. Result error: %v", p.cfg.pipelineName, runErr)

	// --- Final Metrics and Tracing ---
	// Ensure these run even if Stop was called concurrently, but only once.
	// Stop() will also try to finalize these if it initiated the shutdown.
	p.stopOnce.Do(func() {
		p.startMu.Lock() // Lock to safely modify started state
		p.started.Store(false)
		p.startMu.Unlock()

		p.cfg.logger.Printf("DEBUG: Finalizing metrics and trace span in Wait for pipeline '%s'.", p.cfg.pipelineName)
		duration := time.Since(startTime)
		collector := p.cfg.metricsCollector
		isRealCollector := collector != nil && collector != DefaultMetricsCollector
		if isRealCollector {
			// Use runCtx here? Or background? runCtx might be cancelled. Use background.
			collector.PipelineCompleted(context.Background(), p.cfg.pipelineName, duration, runErr)
		}

		if pipelineSpan != nil {
			if runErr != nil {
				pipelineSpan.RecordError(runErr)
				pipelineSpan.SetStatus(codes.Error, runErr.Error())
			} else {
				pipelineSpan.SetStatus(codes.Ok, "")
			}
			pipelineSpan.End()
		}

		// Attempt to stop stoppers here as well, in case Wait finished naturally
		// before Stop was called. Stop will handle its own context/timeout.
		// Use a background context for this cleanup initiated by Wait finishing.
		closeErr := p.closeClosers(context.Background(), len(p.closers)-1) // Use p.closers
		if closeErr != nil {
			p.cfg.logger.Printf("ERROR: Error closing stages during Wait cleanup: %v", closeErr)
			if runErr == nil {
				runErr = NewPipelineLifecycleError("Wait.cleanup", "failed to close stages", closeErr)
			} else {
				// If runErr already exists, we might wrap it or log this additional error.
				// For now, prioritize the original runErr but log the closeErr.
				p.cfg.logger.Printf("DEBUG: Pipeline already had error '%v', additionally failed to close stages: %v", runErr, closeErr)
			}
		}

		// Clean up context if we own it (i.e., if Stop wasn't called first)
		if p.cancelFn != nil {
			p.cancelFn()
		}
	})
	// --- End Final Metrics and Tracing ---

	// Check parent context error *after* group wait (if provided to Start)
	// This seems less relevant now as runCtx cancellation is the primary signal.
	// if ctxErr := ctx.Err(); ctxErr != nil {
	//     runErr = ctxErr // Prioritize context error? Or errgroup error? Usually errgroup.
	// }

	return runErr
}

// Stop initiates a graceful shutdown of a running pipeline.
// It performs the following steps, coordinated with Wait via sync.Once:
//  1. Marks the pipeline as stopped.
//  2. Cancels the pipeline's internal context (runCtx), signaling all stage goroutines to stop.
//  3. Waits for all stage goroutines in the errgroup to exit, respecting the deadline
//     or cancellation of the context provided to Stop.
//  4. Calls the Stop method on all stages implementing the Stopper interface (in reverse
//     order of Start), using the context provided to Stop for timeout/cancellation.
//  5. Finalizes pipeline-level metrics and tracing if Wait hasn't already done so.
//
// The provided context `ctx` governs the maximum time allowed for the *entire shutdown process*
// (waiting for goroutines + stopping stoppers).
//
// Stop is safe to call multiple times; subsequent calls after the first will have no effect.
// It's also safe to call Stop concurrently with Wait finishing.
//
// Returns nil if the shutdown completes successfully within the context's deadline.
// Returns context.DeadlineExceeded or context.Canceled if the shutdown times out or is cancelled.
// Returns an error if stopping any Stopper stage fails (potentially a MultiError).
// Note: Errors returned by the running stages themselves are returned by Wait(), not Stop().
//
//nolint:nonamedreturns // stopErr is idiomatic here
func (p *StreamPipeline[I, O]) Stop(ctx context.Context) (stopErr error) {
	p.startMu.Lock()
	if !p.started.Load() {
		p.startMu.Unlock()
		p.cfg.logger.Printf("DEBUG: Stop called on already stopped or never started pipeline '%s'.", p.cfg.pipelineName)
		return nil // Not an error to stop a stopped pipeline
	}
	// Keep lock while accessing shared state like cancelFn, runGroup
	cancelFn := p.cancelFn
	runGroup := p.runGroup
	pipelineSpan := p.pipelineSpan // Get span stored during Start
	startTime := p.startTime       // Get start time stored during Start
	p.startMu.Unlock()

	p.cfg.logger.Printf("INFO: Stopping pipeline '%s'...", p.cfg.pipelineName)

	// Use stopOnce to ensure shutdown logic runs only once,
	// coordinating between explicit Stop calls and natural completion via Wait.
	p.stopOnce.Do(func() {
		p.startMu.Lock() // Lock again to modify started state
		p.started.Store(false)
		p.startMu.Unlock()

		p.cfg.logger.Printf(
			"DEBUG: Initiating shutdown sequence within stopOnce for pipeline '%s'.",
			p.cfg.pipelineName,
		)

		// 1. Cancel the context for running goroutines
		if cancelFn != nil {
			p.cfg.logger.Printf("DEBUG: Cancelling run context for pipeline '%s'.", p.cfg.pipelineName)
			cancelFn()
		} else {
			p.cfg.logger.Printf("WARN: cancelFn is nil during Stop for pipeline '%s'.", p.cfg.pipelineName)
		}

		// 2. Wait for the errgroup to finish, respecting the Stop context timeout
		waitDone := make(chan error, 1)
		go func() {
			if runGroup != nil {
				waitDone <- runGroup.Wait()
			} else {
				// Should not happen if started was true, but handle defensively
				waitDone <- errors.New("internal error: runGroup is nil during Stop")
			}
			close(waitDone)
		}()

		var groupWaitErr error
		select {
		case err := <-waitDone:
			groupWaitErr = err // Capture the error from the pipeline run
			p.cfg.logger.Printf(
				"DEBUG: Pipeline goroutines finished gracefully for '%s'. Wait error: %v",
				p.cfg.pipelineName,
				err,
			)
		case <-ctx.Done():
			stopErr = NewPipelineLifecycleError("Stop", "shutdown timed out or cancelled", ctx.Err())
			p.cfg.logger.Printf("WARN: Pipeline stop timed out/cancelled for '%s': %v", p.cfg.pipelineName, ctx.Err())
			// Don't return yet, proceed to stop stoppers
		}

		// 3. Close the Closer stages (in reverse order of registration)
		// Use the Stop context for closing these stages too.
		p.cfg.logger.Printf("DEBUG: Closing Closer stages for pipeline '%s'.", p.cfg.pipelineName)

		closeErr := p.closeClosers(ctx, len(p.closers)-1) // Use p.closers

		if closeErr != nil {
			p.cfg.logger.Printf(
				"ERROR: Error closing closer stages for pipeline '%s': %v",
				p.cfg.pipelineName,
				closeErr,
			)
			if stopErr == nil { // Keep the first error (timeout or stopper error)
				stopErr = NewPipelineLifecycleError("Stop", "failed to close stages", closeErr) // Wrap the MultiError
			}
		} else if stopErr == nil { // If no timeout error, but groupWaitErr occurred, it should be the stopErr
			stopErr = groupWaitErr // Propagate pipeline execution error if stop itself was clean
		}

		// 4. Finalize metrics and tracing (if not already done by Wait)
		p.cfg.logger.Printf("DEBUG: Finalizing metrics and trace span in Stop for pipeline '%s'.", p.cfg.pipelineName)
		duration := time.Since(startTime)
		finalRunErr := groupWaitErr // Use the actual error from the pipeline run for reporting
		collector := p.cfg.metricsCollector
		isRealCollector := collector != nil && collector != DefaultMetricsCollector
		if isRealCollector {
			// Use background context as stop context might be done, and runCtx is cancelled
			collector.PipelineCompleted(context.Background(), p.cfg.pipelineName, duration, finalRunErr)
		}

		if pipelineSpan != nil {
			// Determine final span status based on errors
			switch {
			case finalRunErr != nil:
				// Pipeline execution failed, prioritize this error
				pipelineSpan.RecordError(finalRunErr)
				pipelineSpan.SetStatus(codes.Error, finalRunErr.Error())
			case stopErr != nil:
				// Pipeline execution succeeded, but stopping failed/timed out
				pipelineSpan.RecordError(stopErr)
				pipelineSpan.SetStatus(codes.Error, fmt.Sprintf("pipeline stopped with error: %s", stopErr.Error()))
			default:
				// Both pipeline execution and stop were successful
				pipelineSpan.SetStatus(codes.Ok, "")
			}
			pipelineSpan.End() // End the span after setting status
		}
		p.cfg.logger.Printf("INFO: Pipeline '%s' stop sequence complete. Final error: %v", p.cfg.pipelineName, stopErr)
	})

	return stopErr
}

// closeClosers calls Close on Closer stages in reverse order of their registration.
// It collects errors into a MultiError if multiple closers fail.
// Uses the provided context for cancellation/timeout of the Close calls.
func (p *StreamPipeline[I, O]) closeClosers(ctx context.Context, lastIndex int) error {
	var multiErr *MultiError // Assuming you have or will create a MultiError type
	for i := lastIndex; i >= 0; i-- {
		nc := p.closers[i]                                                         // nc is namedCloser
		p.cfg.logger.Printf("DEBUG: Closing stage '%s' (index %d)...", nc.name, i) // Log message update
		// Apply close context to individual stage close
		if err := nc.Closer.Close(ctx); err != nil { // Call Close()
			p.cfg.logger.Printf(
				"ERROR: Failed to close stage '%s' (index %d): %v",
				nc.name,
				i,
				err,
			) // Log message update
			if multiErr == nil {
				multiErr = &MultiError{Errors: make([]error, 0)} // Initialize with an empty slice
			}
			multiErr.Add(
				fmt.Errorf("failed to close stage '%s' (index %d): %w", nc.name, i, err),
			) // Error message update
			// Continue closing others even if one fails
		} else {
			p.cfg.logger.Printf("DEBUG: Closed stage '%s' (index %d) successfully.", nc.name, i) // Log message update
		}
	}

	if multiErr != nil && multiErr.HasErrors() {
		return multiErr
	}

	return nil
}

// Run provides a convenient way to execute a finalized StreamPipeline synchronously.
// It orchestrates the Start, Wait, and Stop lifecycle methods.
// ... (existing documentation) ...
// CHANGE signature: add receiver (pipeline *StreamPipeline[I, O]) and typed channels
func (p *StreamPipeline[I, O]) Run(ctx context.Context, source <-chan I, sink chan<- O) error {
	// Start the pipeline (non-blocking)
	// CHANGE: Call the method pipeline.Start
	err := p.Start(ctx, source, sink)
	if err != nil {
		// If start fails, attempt a cleanup Stop just in case.
		// Use a background context for this cleanup stop.
		_ = p.Stop(context.Background()) // Use the method receiver
		return fmt.Errorf("failed to start pipeline: %w", err)
	}

	// Wait for the pipeline to complete or error out (blocking)
	runErr := p.Wait() // Use the method receiver

	// Always attempt to Stop the pipeline cleanly after Wait finishes
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 15*time.Second) // Example timeout
	defer cancelStop()
	// CHANGE: Call the method pipeline.Stop
	if stopErr := p.Stop(stopCtx); stopErr != nil {
		p.cfg.logger.Printf("ERROR: Error during pipeline stop after run: %v", stopErr)
		// Log stop errors but return the primary execution error (runErr)
	}

	return runErr // Return the error from the pipeline's execution (Wait)
}

// Reset attempts to reset the StreamPipeline and all its constituent stages
// that implement the Resettable interface.
// This method should only be called when the pipeline is NOT running (i.e., after
// Stop() has completed or before Start() has been called).
// It resets internal state to allow the pipeline to be started again cleanly.
// Returns a MultiError if any of the resettable stages fail to reset.
func (p *StreamPipeline[I, O]) Reset(ctx context.Context) error {
	p.startMu.Lock()
	defer p.startMu.Unlock()

	if p.started.Load() {
		return NewPipelineLifecycleError("Reset", "cannot reset a running pipeline", nil)
	}

	p.cfg.logger.Printf("DEBUG: Resetting pipeline '%s'...", p.cfg.pipelineName)

	var multiErr *MultiError
	for i, rStage := range p.stages {
		if original, ok := rStage.originalStage.(Resettable); ok {
			p.cfg.logger.Printf("DEBUG: Resetting stage '%s' (index %d)...", rStage.name, i)
			if err := original.Reset(ctx); err != nil {
				p.cfg.logger.Printf("ERROR: Failed to reset stage '%s' (index %d): %v", rStage.name, i, err)
				if multiErr == nil {
					multiErr = NewMultiError(nil) // Initialize if first error
				}
				multiErr.Add(NewStageError(rStage.name, i, fmt.Errorf("reset failed: %w", err)))
			}
		}
	}

	// Reset pipeline's own lifecycle state for a fresh Start
	p.stopOnce = sync.Once{} // Critical for allowing Stop/Wait to work again
	p.runGroup = nil
	p.runCtx = nil
	if p.cancelFn != nil { // Call previous cancelFn if it exists, to be safe
		p.cancelFn()
	}
	p.cancelFn = nil
	p.pipelineSpan = nil
	p.startTime = time.Time{}
	// p.started is already false

	p.cfg.logger.Printf("INFO: Pipeline '%s' reset complete.", p.cfg.pipelineName)
	if multiErr.HasErrors() {
		return NewPipelineLifecycleError("Reset", "one or more stages failed to reset", multiErr)
	}
	return nil
}

// HealthStatus checks the health of the StreamPipeline and its constituent stages.
// It iterates through all stages. If a stage implements HealthCheckable, its
// HealthStatus is checked.
// Returns a MultiError containing all reported health issues. If all stages are
// healthy (or not HealthCheckable), it returns nil.
// The pipeline's own running state is not directly part of this health check,
// as 'health' here pertains to the components' operational readiness/status.
func (p *StreamPipeline[I, O]) HealthStatus(ctx context.Context) error {
	p.cfg.logger.Printf("DEBUG: Checking health for pipeline '%s'...", p.cfg.pipelineName)
	var multiErr *MultiError

	// Check health of individual stages
	for i, rStage := range p.stages {
		if original, ok := rStage.originalStage.(HealthCheckable); ok {
			if err := original.HealthStatus(ctx); err != nil {
				p.cfg.logger.Printf("WARN: Stage '%s' (index %d) reported unhealthy: %v", rStage.name, i, err)
				if multiErr == nil {
					multiErr = NewMultiError(nil) // Initialize if first error
				}
				multiErr.Add(NewStageError(rStage.name, i, fmt.Errorf("health check failed: %w", err)))
			}
		}
	}

	if multiErr.HasErrors() {
		return NewPipelineLifecycleError("HealthStatus", "one or more stages unhealthy", multiErr)
	}

	p.cfg.logger.Printf("DEBUG: Pipeline '%s' reported as healthy.", p.cfg.pipelineName)
	return nil
}

// castChannelToReadable attempts to cast an interface{} to a readable channel of type T.
// It handles both <-chan T and chan T.
func castChannelToReadable[T any](ch interface{}) (<-chan T, bool) {
	// Check for exact read-only type first
	if typedChan, ok := ch.(<-chan T); ok {
		return typedChan, true
	}
	// Check for bidirectional type (assignable to read-only)
	if typedChan, ok := ch.(chan T); ok {
		return typedChan, true // chan T can be used as <-chan T
	}
	// Type mismatch
	return nil, false
}

// castChannelToWritable attempts to cast an interface{} to a writable channel of type T.
// It handles both chan<- T and chan T.
func castChannelToWritable[T any](ch interface{}) (chan<- T, bool) {
	// Check for exact write-only type first
	if typedChan, ok := ch.(chan<- T); ok {
		return typedChan, true
	}
	// Check for bidirectional type (assignable to write-only)
	if typedChan, ok := ch.(chan T); ok {
		return typedChan, true // chan T can be used as chan<- T
	}
	// Type mismatch
	return nil, false
}

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

// Pipeline represents a sequence of processing stages.
type Pipeline[I, O any] struct {
	stage      Stage[I, O]
	errHandler func(error) error
	// --- New Lifecycle Fields ---
	startMu sync.Mutex
	started bool
	// --- End New Lifecycle Fields ---
}

// NewPipeline creates a new pipeline with a single stage.
func NewPipeline[I, O any](stage Stage[I, O]) *Pipeline[I, O] {
	return &Pipeline[I, O]{
		stage:      stage,
		errHandler: func(err error) error { return err },
		started:    false, // Initialize started state
	}
}

// WithErrorHandler adds a custom error handler to the pipeline.
func (p *Pipeline[I, O]) WithErrorHandler(handler func(error) error) *Pipeline[I, O] {
	if handler != nil {
		p.errHandler = handler
	} else {
		p.errHandler = func(err error) error { return err } // Default to no-op handler
	}
	return p
}

// Process runs the pipeline on the given input.
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

// Start initializes the pipeline's stage if it implements the Starter interface.
func (p *Pipeline[I, O]) Start(ctx context.Context) error {
	p.startMu.Lock()
	defer p.startMu.Unlock()

	if p.started {
		return ErrPipelineAlreadyStarted // Use predefined
	}

	if starter, ok := p.stage.(Starter); ok {
		if err := starter.Start(ctx); err != nil {
			return NewPipelineLifecycleError("Start", "stage start failed", err)
		}
	}

	p.started = true
	return nil
}

// Stop cleans up the pipeline's stage if it implements the Stopper interface.
func (p *Pipeline[I, O]) Stop(ctx context.Context) error {
	p.startMu.Lock()
	defer p.startMu.Unlock()

	if !p.started {
		return nil // Not an error to stop an already stopped pipeline
	}

	var stopErr error
	if stopper, ok := p.stage.(Stopper); ok {
		stopErr = stopper.Stop(ctx)
	}

	p.started = false // Mark as stopped even if stopper errors

	if stopErr != nil {
		return NewPipelineLifecycleError("Stop", "stage stop failed", stopErr)
	}
	return nil
}

// --- Stream Pipeline Configuration ---

// streamPipelineConfig holds configuration applied to the pipeline.
type streamPipelineConfig struct {
	bufferSize       int
	logger           *log.Logger
	concurrency      int              // Number of concurrent goroutines for each stage
	metricsCollector MetricsCollector // Optional metrics collector for stages
	pipelineName     string           // Optional name for the pipeline
	tracerProvider   TracerProvider
	tracer           trace.Tracer
}

// StreamPipelineOption defines a function that modifies the pipeline configuration.
type StreamPipelineOption func(*streamPipelineConfig)

// WithStreamTracerProvider sets the OpenTelemetry Tracer for the pipeline.
// If set to nil or not called, no pipeline/stage spans will be created by Run.
func WithStreamTracerProvider(provider TracerProvider) StreamPipelineOption {
	return func(cfg *streamPipelineConfig) {
		if provider != nil {
			cfg.tracerProvider = provider
		} else {
			cfg.tracerProvider = DefaultTracerProvider // Ensure it's never nil
		}
	}
}

// WithStreamMetricsCollector sets the metrics collector for the pipeline.
// This collector will receive PipelineStarted/Completed and default StageWorker* metrics.
func WithStreamMetricsCollector(collector MetricsCollector) StreamPipelineOption {
	return func(cfg *streamPipelineConfig) {
		cfg.metricsCollector = collector // Allow setting nil
	}
}

// WithStreamPipelineName sets a name for the pipeline, used in metrics reporting.
func WithStreamPipelineName(name string) StreamPipelineOption {
	return func(cfg *streamPipelineConfig) {
		if name != "" {
			cfg.pipelineName = name
		}
	}
}

// WithStreamBufferSize sets the buffer size for the channels connecting stages.
// n > 0: buffered channel of size n.
// n <= 0: unbuffered channel.
func WithStreamBufferSize(n int) StreamPipelineOption {
	return func(cfg *streamPipelineConfig) {
		if n < 0 {
			cfg.bufferSize = 0 // Treat negative as unbuffered
		} else {
			cfg.bufferSize = n
		}
	}
}

// WithStreamLogger sets a logger for the pipeline and its adapters.
// If nil, logging defaults to a logger that discards output.
func WithStreamLogger(logger *log.Logger) StreamPipelineOption {
	return func(cfg *streamPipelineConfig) {
		if logger != nil {
			cfg.logger = logger // logger can be nil, allowing for no logging
		}
	}
}

// WithStreamConcurrency sets a default concurrency level for StreamAdapters
// created within the pipeline. This applies unless overridden by
// WithAdapterConcurrency when calling AddStage.
// Concurrency must be at least 1.
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

// runnableStage holds the necessary information to run a single stage
// within the pipeline, including type information and the execution logic.
type runnableStage struct {
	// A function that executes the stage's logic, handling type assertions.
	run func(ctx context.Context, inChan, outChan interface{}) error
	// Input type of the stage, used for channel creation.
	inType reflect.Type
	// Output type of the stage, used for channel creation.
	outType reflect.Type
	// Name or description for logging/debugging (optional).
	name string
	// Optional field to store the original stage instance
	originalStage interface{}
}

// --- Stream Pipeline Builder ---

// StreamPipelineBuilder facilitates the type-safe construction of a StreamPipeline.
// The generic parameter `LastOutput` tracks the output type of the last stage added,
// ensuring the next stage's input type matches.
type StreamPipelineBuilder[LastOutput any] struct {
	stages   []runnableStage
	cfg      *streamPipelineConfig
	starters []Starter
	stoppers []Stopper
}

// NewStreamPipeline starts building a new stream pipeline.
// The pipeline expects an initial input type `I`.
// It returns a builder ready to accept the first stage, which must take `I` as input.
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
		stages:   make([]runnableStage, 0),
		cfg:      cfg,
		starters: make([]Starter, 0),
		stoppers: make([]Stopper, 0),
	}
}

// AddStage adds a standard Stage[CurrentOutput, NextOutput] to the pipeline.
// The `CurrentOutput` type parameter *must* match the `LastOutput` type of the builder.
// The stage will be automatically wrapped in a StreamAdapter using the pipeline's configuration.
// It returns a new builder instance typed with the stage's output type `NextOutput`.
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
	var stageAsStarter Starter
	var stageAsStopper Stopper
	if s, ok := stage.(Starter); ok {
		stageAsStarter = s
	}
	if s, ok := stage.(Stopper); ok {
		stageAsStopper = s
	}
	// --- End Interface Checks ---

	// Create the runner function closure. This captures the typed stage
	// and pipeline config, handling type assertions internally.
	runner := func(ctx context.Context, inChan, outChan interface{}) error {
		// Assert channels to their concrete types.
		var typedInChan <-chan CurrentOutput
		if ch, okInChan := inChan.(<-chan CurrentOutput); okInChan { // Check if it's exactly the source type
			typedInChan = ch
		} else if ch, okInChanBi := inChan.(chan CurrentOutput); okInChanBi { // Check if it's an intermediate bidirectional chan
			typedInChan = ch // Assignable to <-chan CurrentOutput
		} else {
			// This should ideally never happen if Run is correct
			return fmt.Errorf("internal error: stage '%s' received incompatible input channel type %T, expected <-chan %s or chan %s", name, inChan, inType.Name(), inType.Name())
		}

		var typedOutChan chan<- NextOutput
		if ch, okOutChan := outChan.(chan<- NextOutput); okOutChan { // Check if it's exactly the sink type
			typedOutChan = ch
		} else if ch, okOutChanBi := outChan.(chan NextOutput); okOutChanBi { // Check if it's an intermediate bidirectional chan
			typedOutChan = ch // Assignable to chan<- NextOutput
		} else {
			// This should ideally never happen if Run is correct
			return fmt.Errorf("internal error: stage '%s' received incompatible output channel type %T, expected chan<- %s or chan %s", name, outChan, outType.Name(), outType.Name())
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
	newStarters := builder.starters
	if stageAsStarter != nil {
		newStarters = append(newStarters, stageAsStarter)
	}
	newStoppers := builder.stoppers
	if stageAsStopper != nil {
		newStoppers = append(newStoppers, stageAsStopper)
	}
	// --- End Add to builder lists ---

	// Return a new builder instance with the updated stage list and output type
	return &StreamPipelineBuilder[NextOutput]{
		stages:   append(builder.stages, newRunnable),
		cfg:      builder.cfg,
		starters: newStarters,
		stoppers: newStoppers,
	}
}

// AddStreamStage adds a StreamStage[CurrentOutput, NextOutput] to the pipeline.
// The `CurrentOutput` type parameter *must* match the `LastOutput` type of the builder.
// This stage is used directly without needing a StreamAdapter.
// It returns a new builder instance typed with the stage's output type `NextOutput`.
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
	var stageAsStarter Starter
	var stageAsStopper Stopper
	if s, ok := stage.(Starter); ok {
		stageAsStarter = s
	}
	if s, ok := stage.(Stopper); ok {
		stageAsStopper = s
	}
	// --- End Interface Checks ---

	// Create the runner function closure.
	runner := func(ctx context.Context, inChan, outChan interface{}) error {
		// Assert channels to their concrete types.
		var typedInChan <-chan CurrentOutput
		if ch, okInChan := inChan.(<-chan CurrentOutput); okInChan { // Check if it's exactly the source type
			typedInChan = ch
		} else if ch, okInChanBi := inChan.(chan CurrentOutput); okInChanBi { // Check if it's an intermediate bidirectional chan
			typedInChan = ch // Assignable to <-chan CurrentOutput
		} else {
			// This should ideally never happen if Run is correct
			return fmt.Errorf("internal error: stage '%s' received incompatible input channel type %T, expected <-chan %s or chan %s", name, inChan, inType.Name(), inType.Name())
		}

		var typedOutChan chan<- NextOutput
		if ch, okOutChan := outChan.(chan<- NextOutput); okOutChan { // Check if it's exactly the sink type
			typedOutChan = ch
		} else if ch, okOutChanBi := outChan.(chan NextOutput); okOutChanBi { // Check if it's an intermediate bidirectional chan
			typedOutChan = ch // Assignable to chan<- NextOutput
		} else {
			// This should ideally never happen if Run is correct
			return fmt.Errorf("internal error: stage '%s' received incompatible output channel type %T, expected chan<- %s or chan %s", name, outChan, outType.Name(), outType.Name())
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
	newStarters := builder.starters
	if stageAsStarter != nil {
		newStarters = append(newStarters, stageAsStarter)
	}
	newStoppers := builder.stoppers
	if stageAsStopper != nil {
		newStoppers = append(newStoppers, stageAsStopper)
	}
	// --- End Add to builder lists ---

	// Return a new builder instance
	return &StreamPipelineBuilder[NextOutput]{
		stages:   append(builder.stages, newRunnable),
		cfg:      builder.cfg,
		starters: newStarters,
		stoppers: newStoppers,
	}
}

// --- Finalized Pipeline ---

// StreamPipeline represents the configured, runnable pipeline.
// It is created by calling Finalize on a StreamPipelineBuilder.
type StreamPipeline struct {
	stages []runnableStage
	cfg    *streamPipelineConfig
	// Store the overall input and output types for final validation in Run
	firstInputType reflect.Type
	lastOutputType reflect.Type

	// --- New Lifecycle Fields ---
	startMu  sync.Mutex         // Protects access to started flag and during start/stop transitions
	stopOnce sync.Once          // Ensures Stop logic runs only once
	stopCh   chan struct{}      // Closed to signal goroutines to stop
	runGroup *errgroup.Group    // The errgroup managing the running pipeline goroutines
	runCtx   context.Context    // The context for the running errgroup
	cancelFn context.CancelFunc // Function to cancel the runCtx
	started  atomic.Bool        // Tracks if the pipeline is currently running (atomic for potential read checks)

	// Store stages that need lifecycle management
	starters []Starter
	stoppers []Stopper
	// --- End New Lifecycle Fields ---

	pipelineSpan trace.Span // Span for the overall pipeline run
	startTime    time.Time  // Start time for metrics duration

}

// Finalize constructs the runnable StreamPipeline from the builder.
// It performs final validation checks. The `LastOutput` type parameter
// must match the output type of the very last stage added.
func Finalize[LastOutput any](builder *StreamPipelineBuilder[LastOutput]) (*StreamPipeline, error) {
	if len(builder.stages) == 0 {
		return nil, errors.New("fluxus.Finalize: cannot build an empty pipeline")
	}

	// Final check: Ensure the builder's final output type matches the last stage's output type.
	// This is mostly a sanity check, as the generic Add methods should guarantee this.
	lastStageActualOutputType := builder.stages[len(builder.stages)-1].outType
	expectedLastOutputType := reflect.TypeOf((*LastOutput)(nil)).Elem()

	if lastStageActualOutputType != expectedLastOutputType {
		// This indicates a potential misuse of the builder or an internal error.
		return nil, fmt.Errorf(
			"fluxus.Finalize: internal inconsistency - builder's final type %s does not match last stage's output type %s",
			expectedLastOutputType.Name(),
			lastStageActualOutputType.Name(),
		)
	}

	return &StreamPipeline{
		stages:         builder.stages,
		cfg:            builder.cfg,
		firstInputType: builder.stages[0].inType,                    // Input type of the first stage
		lastOutputType: lastStageActualOutputType,                   // Output type of the last stage
		starters:       append([]Starter(nil), builder.starters...), // Copy slices
		stoppers:       append([]Stopper(nil), builder.stoppers...), // Copy slices
		stopCh:         make(chan struct{}),                         // Initialize stopCh here or in Start
	}, nil
}

// --- Pipeline Execution ---

// validateStartChannels checks if the provided source and sink channels match the pipeline's expected types.
func (p *StreamPipeline) validateStartChannels(source, sink interface{}) error {
	// Validate source channel type
	sourceVal := reflect.ValueOf(source)
	if sourceVal.Kind() != reflect.Chan || sourceVal.Type().ChanDir()&reflect.RecvDir == 0 {
		return NewPipelineConfigurationError(fmt.Sprintf("source must be a readable channel, got %T", source))
	}
	sourceType := sourceVal.Type().Elem() // Use Elem() for channel element type
	if sourceType != p.firstInputType {
		return NewPipelineConfigurationError(
			fmt.Sprintf(
				"incompatible source channel type. Expected %s, got %s",
				p.firstInputType.Name(),
				sourceType.Name(),
			),
		)
	}

	// Validate sink channel type
	sinkVal := reflect.ValueOf(sink)
	if sinkVal.Kind() != reflect.Chan || sinkVal.Type().ChanDir()&reflect.SendDir == 0 {
		return NewPipelineConfigurationError(fmt.Sprintf("sink must be a writable channel, got %T", sink))
	}
	sinkType := sinkVal.Type().Elem() // Use Elem() for channel element type
	if sinkType != p.lastOutputType {
		return NewPipelineConfigurationError(
			fmt.Sprintf(
				"incompatible sink channel type. Expected %s, got %s",
				p.lastOutputType.Name(),
				sinkType.Name(),
			),
		)
	}
	return nil
}

// initializeObservability sets up tracing and metrics for the pipeline run.
//
//nolint:nonamedreturns // This is a common pattern in Go for multiple return values
func (p *StreamPipeline) initializeObservability(ctx context.Context) (
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

// initializeRunState sets up the internal context, errgroup, and stop channel for the pipeline run.
func (p *StreamPipeline) initializeRunState(pipelineCtx context.Context) context.Context {
	p.stopCh = make(chan struct{})
	// Create the cancellable context for the errgroup, derived from the (potentially traced) pipelineCtx
	p.runCtx, p.cancelFn = context.WithCancel(pipelineCtx)
	g, gctx := errgroup.WithContext(p.runCtx)
	p.runGroup = g // Store the group
	return gctx    // Return the group's context for stages
}

// startStarterStages attempts to start all stages implementing the Starter interface.
// It handles cleanup by stopping already started stages if one fails.
func (p *StreamPipeline) startStarterStages(
	ctx context.Context,
	gctx context.Context,
	pipelineSpan trace.Span,
	isRealCollector bool,
	startTime time.Time,
) error {
	p.cfg.logger.Printf("DEBUG: Starting Starter stages for pipeline '%s'...", p.cfg.pipelineName)
	for i, starter := range p.starters {
		err := starter.Start(gctx) // Use group context
		if err != nil {
			// If a starter fails, perform cleanup
			p.cfg.logger.Printf("ERROR: Failed to start stage %d: %v. Attempting cleanup...", i, err)
			if p.cancelFn != nil { // Ensure cancelFn is available before calling
				p.cancelFn()
			}
			_ = p.stopStoppers(
				ctx,
				i-1,
			) // Stop stages started before the failed one (use original ctx for stop timeout)

			// Finalize observability for failed start
			if pipelineSpan != nil {
				pipelineSpan.RecordError(err)
				pipelineSpan.SetStatus(codes.Error, "failed to start starter stage")
				pipelineSpan.End()
			}
			if isRealCollector && p.cfg.metricsCollector != nil {
				p.cfg.metricsCollector.PipelineCompleted(
					context.Background(),
					p.cfg.pipelineName,
					time.Since(startTime),
					err,
				)
			}

			wrappedErr := fmt.Errorf("failed to start stage %d: %w", i, err)
			return NewPipelineLifecycleError("Start", "stage initialization failed", wrappedErr)
		}
	}
	p.cfg.logger.Printf("DEBUG: Starter stages started successfully for pipeline '%s'.", p.cfg.pipelineName)
	return nil
}

// launchStageGoroutines creates intermediate channels and launches a goroutine for each stage.
func (p *StreamPipeline) launchStageGoroutines(gctx context.Context, source, sink interface{}) {
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

// Start initializes the pipeline, starts any Starter stages, and launches the
// processing goroutines for each stage. It connects the pipeline stages using
// the provided source and sink channels.
//
// Start is non-blocking. Use the Wait() method to block until the pipeline
// completes or encounters an error. Call Stop() to initiate a graceful shutdown.
//
// The `I` and `O` type parameters *must* match the input type of the first stage
// and the output type of the last stage, respectively.
func (p *StreamPipeline) Start(ctx context.Context, source interface{}, sink interface{}) error {
	p.startMu.Lock()
	defer p.startMu.Unlock()

	// 1. Initial Checks
	if p.started.Load() {
		return ErrPipelineAlreadyStarted
	}
	if len(p.stages) == 0 {
		return ErrEmptyPipeline
	}

	// 2. Type Validation
	if err := p.validateStartChannels(source, sink); err != nil {
		return err
	}

	// 3. Observability Setup
	pipelineCtx, pipelineSpan, startTime, isRealCollector := p.initializeObservability(ctx)

	// 4. Initialize Lifecycle State
	gctx := p.initializeRunState(pipelineCtx) // Get the group's context

	// 5. Start Starter Stages (handles its own cleanup on failure)
	if err := p.startStarterStages(ctx, gctx, pipelineSpan, isRealCollector, startTime); err != nil {
		// Error already wrapped, observability handled within the helper on failure
		return err
	}

	// 6. Launch Stage Goroutines
	p.launchStageGoroutines(gctx, source, sink)

	// 7. Finalization
	p.started.Store(true)
	p.cfg.logger.Printf("INFO: Pipeline '%s' started successfully.", p.cfg.pipelineName)

	return nil
}

// Wait blocks until the started pipeline finishes processing or encounters an error.
// It returns the first error encountered by any stage, or nil if the pipeline
// completes successfully after the source channel is closed and all data is processed.
// Wait should only be called after a successful call to Start().
// It also handles final metrics emission and trace span closure for successful/error completion.
//
//nolint:nonamedreturns // runErr is a named return value for clarity
func (p *StreamPipeline) Wait() (runErr error) {
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
		_ = p.stopStoppers(context.Background(), len(p.stoppers)-1)

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

// Stop initiates a graceful shutdown of the pipeline.
// It cancels the pipeline's internal context, signals stages to stop,
// waits for running goroutines to complete (up to the context deadline),
// and calls Stop() on any Stopper stages.
// The provided context governs the timeout for the *shutdown process itself*.
// Stop is safe to call multiple times.
//
//nolint:nonamedreturns // stopErr is a named return value for clarity
func (p *StreamPipeline) Stop(ctx context.Context) (stopErr error) {
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

		// 3. Stop the Stopper stages (in reverse order of starting)
		// Use the Stop context for stopping these stages too.
		p.cfg.logger.Printf("DEBUG: Stopping Stopper stages for pipeline '%s'.", p.cfg.pipelineName)
		stopperErr := p.stopStoppers(ctx, len(p.stoppers)-1)
		if stopperErr != nil {
			p.cfg.logger.Printf(
				"ERROR: Error stopping stopper stages for pipeline '%s': %v",
				p.cfg.pipelineName,
				stopperErr,
			)
			if stopErr == nil { // Keep the first error (timeout or stopper error)
				stopErr = NewPipelineLifecycleError("Stop", "failed to stop stages", stopperErr) // Wrap the MultiError
			}
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

// Helper function to stop stoppers
func (p *StreamPipeline) stopStoppers(ctx context.Context, lastIndex int) error {
	var multiErr *MultiError // Assuming you have or will create a MultiError type
	for i := lastIndex; i >= 0; i-- {
		stopper := p.stoppers[i]
		p.cfg.logger.Printf("DEBUG: Stopping stage %d...", i)
		// Apply stop context to individual stage stop
		if err := stopper.Stop(ctx); err != nil {
			p.cfg.logger.Printf("ERROR: Failed to stop stage %d: %v", i, err)
			if multiErr == nil {
				multiErr = &MultiError{Errors: make([]error, 0)} // Initialize with an empty slice
			}
			multiErr.Add(fmt.Errorf("failed to stop stage %d: %w", i, err))
			// Continue stopping others even if one fails
		} else {
			p.cfg.logger.Printf("DEBUG: Stopped stage %d successfully.", i)
		}
	}
	if multiErr != nil && multiErr.HasErrors() {
		return multiErr
	}
	return nil
}

// Run executes the finalized stream pipeline by calling Start, Wait, and Stop.
// It connects the stages with channels based on the pipeline configuration
// and runs them concurrently.
//
// The `I` and `O` type parameters *must* match the input type of the first stage
// and the output type of the last stage, respectively.
//
// The function blocks until the pipeline completes naturally (source closed, data processed),
// an error occurs in any stage, or the provided context is cancelled.
//
// It returns the first error encountered during pipeline execution (from Wait),
// or an error if starting the pipeline fails. Stop errors are logged but not returned directly by Run.
func Run(ctx context.Context, pipeline *StreamPipeline, source interface{}, sink interface{}) error {
	// Start the pipeline (non-blocking)
	err := pipeline.Start(ctx, source, sink) // Pass interface{} directly
	if err != nil {
		// If start fails, attempt a cleanup Stop just in case some resources were partially acquired.
		// Use a background context for this cleanup stop.
		_ = pipeline.Stop(context.Background())
		return fmt.Errorf("failed to start pipeline: %w", err)
	}

	// Wait for the pipeline to complete or error out (blocking)
	runErr := pipeline.Wait()

	// Always attempt to Stop the pipeline cleanly after Wait finishes,
	// even if Wait returned an error. Stop handles idempotency.
	// Use a separate context for Stop, perhaps with a short timeout,
	// as the main ctx might already be done.
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 15*time.Second) // Example timeout
	defer cancelStop()
	if stopErr := pipeline.Stop(stopCtx); stopErr != nil {
		pipeline.cfg.logger.Printf("ERROR: Error during pipeline stop after run: %v", stopErr)
		// We typically return the runErr, as that's the primary outcome.
		// Stop errors are logged.
	}

	return runErr // Return the error from the pipeline's execution (Wait)
}

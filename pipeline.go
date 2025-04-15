package fluxus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
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
}

// NewPipeline creates a new pipeline with a single stage.
func NewPipeline[I, O any](stage Stage[I, O]) *Pipeline[I, O] {
	return &Pipeline[I, O]{
		stage:      stage,
		errHandler: func(err error) error { return err },
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
}

// --- Stream Pipeline Builder ---

// StreamPipelineBuilder facilitates the type-safe construction of a StreamPipeline.
// The generic parameter `LastOutput` tracks the output type of the last stage added,
// ensuring the next stage's input type matches.
type StreamPipelineBuilder[LastOutput any] struct {
	stages []runnableStage
	cfg    *streamPipelineConfig
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
		stages: make([]runnableStage, 0),
		cfg:    cfg,
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
		run:     runner,
		inType:  inType,
		outType: outType,
		name:    name,
	}

	// Return a new builder instance with the updated stage list and output type
	return &StreamPipelineBuilder[NextOutput]{
		stages: append(builder.stages, newRunnable),
		cfg:    builder.cfg,
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
		run:     runner,
		inType:  inType,
		outType: outType,
		name:    name,
	}

	// Return a new builder instance
	return &StreamPipelineBuilder[NextOutput]{
		stages: append(builder.stages, newRunnable),
		cfg:    builder.cfg,
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
		firstInputType: builder.stages[0].inType,  // Input type of the first stage
		lastOutputType: lastStageActualOutputType, // Output type of the last stage
	}, nil
}

// --- Pipeline Execution ---

// Run executes the finalized stream pipeline.
// It connects the stages with channels based on the pipeline configuration
// and runs them concurrently using an errgroup.
//
// The `I` and `O` type parameters *must* match the input type of the first stage
// and the output type of the last stage, respectively. This is checked at runtime
// to ensure the provided source and sink channels are compatible with the built pipeline.
//
// The function blocks until:
// 1. The source channel is closed and all data has been processed by all stages.
// 2. An error occurs in any stage.
// 3. The provided context is cancelled.
//
// It returns the first error encountered by any stage, or nil if execution completes successfully.
//
//nolint:nonamedreturns // runErr is a named return variable for clarity
func Run[I, O any](
	ctx context.Context,
	pipeline *StreamPipeline,
	source <-chan I,
	sink chan<- O,
) (runErr error) {
	if len(pipeline.stages) == 0 {
		return errors.New("fluxus.Run: cannot run an empty pipeline")
	}
	// --- Tracing Setup ---
	// Ensure tracer provider is not nil
	if pipeline.cfg.tracerProvider == nil { // <-- Add check
		pipeline.cfg.tracerProvider = DefaultTracerProvider
	}
	pipelineName := pipeline.cfg.pipelineName
	// Create the tracer instance using the provider and adapter name
	pipeline.cfg.tracer = pipeline.cfg.tracerProvider.Tracer(fmt.Sprintf("fluxus/pipeline/%s", pipelineName))
	tracer := pipeline.cfg.tracer // Might be nil

	pipelineCtx := ctx // Start with the original context
	var pipelineSpan trace.Span

	// Start overall pipeline span if tracer exists
	pipelineCtx, pipelineSpan = tracer.Start(pipelineCtx, fmt.Sprintf("%s.Run", pipelineName),
		trace.WithAttributes(
			attribute.String("fluxus.pipeline.name", pipelineName),
			attribute.Int("fluxus.pipeline.stages", len(pipeline.stages)),
		),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)

	defer func() { // This defer will only run if pipelineSpan is not nil
		if runErr != nil {
			pipelineSpan.RecordError(runErr)
			pipelineSpan.SetStatus(codes.Error, runErr.Error())
		} else {
			pipelineSpan.SetStatus(codes.Ok, "")
		}
		pipelineSpan.End()
	}()

	// --- End Tracing Setup ---
	// --- Metrics Integration Start ---
	startTime := time.Now()
	collector := pipeline.cfg.metricsCollector // Might be NoopMetricsCollector
	// Check if the collector is non-nil and not the Noop one before calling
	// (Alternatively, rely on NoopCollector doing nothing, but check is clearer)
	isRealCollector := collector != nil && collector != DefaultMetricsCollector
	if isRealCollector {
		collector.PipelineStarted(pipelineCtx, pipelineName)
	}
	defer func() {
		duration := time.Since(startTime)
		if isRealCollector {
			collector.PipelineCompleted(pipelineCtx, pipelineName, duration, runErr)
		}
	}()
	// --- Metrics Integration End ---
	// Create errgroup context derived from pipelineCtx (which might contain span info)
	groupCtx, cancel := context.WithCancel(pipelineCtx)
	defer cancel()
	g, gctx := errgroup.WithContext(groupCtx)
	var currentInChan interface{} = source
	// Create and connect the stages
	for i, stage := range pipeline.stages {
		// ... (channel creation logic) ...
		var currentOutChan interface{}
		isLastStage := (i == len(pipeline.stages)-1)
		if isLastStage {
			currentOutChan = sink
		} else {
			chanType := reflect.ChanOf(reflect.BothDir, stage.outType)
			currentOutChan = reflect.MakeChan(chanType, pipeline.cfg.bufferSize).Interface()
		}
		// --- Explicitly capture loop variables ---
		inChan := currentInChan
		outChan := currentOutChan
		currentStage := stage
		stageIndex := i
		// --- End explicit capture ---
		g.Go(func() error {
			stageCtx := gctx // Start with the group context
			var stageSpan trace.Span
			if tracer != nil { // <<< Check if tracer is configured
				// --- Stage Span Start ---
				stageStartTime := time.Now()
				stageCtx, stageSpan = tracer.Start(gctx, // Use errgroup's context as parent
					fmt.Sprintf("Stage[%d]:%s", stageIndex, currentStage.name),
					trace.WithAttributes(
						attribute.String("fluxus.pipeline.stage.name", currentStage.name),
						attribute.Int("fluxus.pipeline.stage.index", stageIndex),
					),
					trace.WithSpanKind(trace.SpanKindInternal),
				)
				defer func() { // This defer only runs if stageSpan is not nil
					stageDuration := time.Since(stageStartTime)
					stageSpan.SetAttributes(
						attribute.Int64("fluxus.pipeline.stage.duration_ms", stageDuration.Milliseconds()),
					)
					stageSpan.End()
				}()
				// --- Stage Span End ---
			}
			// Execute the stage using stageCtx (which might contain span info)
			err := currentStage.run(stageCtx, inChan, outChan)
			if err != nil {
				if stageSpan != nil { // <<< Check if span exists before recording error
					stageSpan.RecordError(err)
					stageSpan.SetStatus(codes.Error, err.Error())
				}
				// Wrap error with stage context before returning to errgroup
				return fmt.Errorf("stage '%s' failed: %w", currentStage.name, err)
			}
			if stageSpan != nil { // <<< Check if span exists before setting status OK
				stageSpan.SetStatus(codes.Ok, "")
			}
			return nil
		})
		currentInChan = currentOutChan
	}

	// Wait for completion
	err := g.Wait()

	// Check parent context error *after* group wait
	if ctxErr := ctx.Err(); ctxErr != nil {
		runErr = ctxErr
		return runErr
	}

	runErr = err
	return runErr
}
